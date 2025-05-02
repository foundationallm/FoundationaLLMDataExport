using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;

// Define a record for the data structure matching the query.
public record MessageRecord(
    string id,
    string sessionId,
    string timeStamp, // ISO 8601 format string e.g., "2024-01-15T10:30:00Z"
    string sender,
    string senderDisplayName,
    int? tokens, // Nullable if the field might be missing.
    string upn,
    bool? deleted, // Nullable boolean.
    string status,
    string type // Added type for clarity, though filtered in query.
);

public record ExportState(DateTime LastExportDateUtc);

public class Program
{
    private static IConfiguration _configuration;
    private static BlobContainerClient _blobContainerClient;
    private static Container _cosmosContainer;
    private static string _stateBlobName;

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting Cosmos DB to Blob Storage Export...");

        // --- Configuration Setup ---
        _configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables() // Override with environment variables (highest precedence).
            .Build();

        var cosmosDbEndpoint = _configuration["CosmosDbEndpoint"];
        var cosmosDbDatabase = _configuration["CosmosDbDatabase"];
        var cosmosDbContainerName = _configuration["CosmosDbContainer"];
        var storageAccountName = _configuration["StorageAccountName"];
        var storageContainerName = _configuration["StorageContainerName"];
        _stateBlobName = _configuration["StateBlobName"];

        if (string.IsNullOrEmpty(cosmosDbEndpoint) || string.IsNullOrEmpty(cosmosDbDatabase) ||
            string.IsNullOrEmpty(cosmosDbContainerName) || string.IsNullOrEmpty(storageAccountName) ||
            string.IsNullOrEmpty(storageContainerName) || string.IsNullOrEmpty(_stateBlobName))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Error: Missing configuration values in appsettings.json or environment variables.");
            Console.WriteLine("Required: CosmosDbEndpoint, CosmosDbDatabase, CosmosDbContainer, StorageAccountName, StorageContainerName, StateBlobName");
            Console.ResetColor();
            Environment.ExitCode = 1;
            return;
        }

        // Ensure appsettings.json is copied to output directory.
        var appsettingsPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (!File.Exists(appsettingsPath))
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Warning: {appsettingsPath} not found. Ensure 'Copy to Output Directory' is set to 'Copy if newer' or 'Copy always' for appsettings.json in your project file.");
            Console.ResetColor();
            // Attempt to load from current directory anyway, but it might fail if not present.
            _configuration = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true)
               .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
               .AddEnvironmentVariables()
               .Build();
            // Re-check mandatory config after attempting reload
            cosmosDbEndpoint = _configuration["CosmosDbEndpoint"]; // Re-fetch potentially missing values
            cosmosDbDatabase = _configuration["CosmosDbDatabase"];
            cosmosDbContainerName = _configuration["CosmosDbContainer"];
            storageAccountName = _configuration["StorageAccountName"];
            storageContainerName = _configuration["StorageContainerName"];
            _stateBlobName = _configuration["StateBlobName"];

            if (string.IsNullOrEmpty(cosmosDbEndpoint) || string.IsNullOrEmpty(cosmosDbDatabase) ||
                string.IsNullOrEmpty(cosmosDbContainerName) || string.IsNullOrEmpty(storageAccountName) ||
                string.IsNullOrEmpty(storageContainerName) || string.IsNullOrEmpty(_stateBlobName))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: Still missing required configuration after checking current directory.");
                Console.ResetColor();
                Environment.ExitCode = 1;
                return;
            }
        }

        var storageAccountUri = new Uri($"https://{storageAccountName}.blob.core.windows.net");

        // --- Azure Clients Setup ---
        var credential = new DefaultAzureCredential();

        try
        {
            Console.WriteLine($"Authenticating and connecting to services...");
            // Blob Storage Client.
            var blobServiceClient = new BlobServiceClient(storageAccountUri, credential);
            _blobContainerClient = blobServiceClient.GetBlobContainerClient(storageContainerName);
            await _blobContainerClient.CreateIfNotExistsAsync(PublicAccessType.None); // Ensure container exists, private
            Console.WriteLine($"Ensured Blob Storage container exists: {storageContainerName}");

            // Cosmos DB Client.
            var cosmosClientOptions = new CosmosClientOptions
            {
                // Switch to Direct connection mode - recommended for large continuation tokens
                ConnectionMode = ConnectionMode.Direct, 
                RequestTimeout = TimeSpan.FromSeconds(90), // Keep increased timeout
                EnableTcpConnectionEndpointRediscovery = true 
            };
            var cosmosClient = new CosmosClient(cosmosDbEndpoint, credential, cosmosClientOptions);
            var database = cosmosClient.GetDatabase(cosmosDbDatabase);
            _cosmosContainer = database.GetContainer(cosmosDbContainerName);
            // Perform a cheap operation to verify connection and permissions (e.g., read container properties)
            await _cosmosContainer.ReadContainerAsync();
            Console.WriteLine($"Successfully connected to Cosmos DB container: {cosmosDbDatabase}/{cosmosDbContainerName}");

            // --- State Management ---
            var startDateToProcess = await GetStartDateToProcessAsync();
            // Process up to the beginning of *yesterday* UTC to avoid potential issues with ongoing writes for the current day.
            var endDateToProcess = DateTime.UtcNow.Date.AddDays(-1);

            if (startDateToProcess > endDateToProcess)
            {
                Console.WriteLine($"Data is already up-to-date (processed up to {startDateToProcess.AddDays(-1):yyyy-MM-dd}). No new days to process.");
            }
            else
            {
                Console.WriteLine($"Starting export process from {startDateToProcess:yyyy-MM-dd} up to {endDateToProcess:yyyy-MM-dd}");

                // --- Processing Loop ---
                var currentExportDate = startDateToProcess;
                while (currentExportDate <= endDateToProcess)
                {
                    await ProcessDayAsync(currentExportDate);
                    // Update state immediately after successfully processing a day.
                    await UpdateStateAsync(currentExportDate);
                    Console.WriteLine($"Successfully processed and updated state for: {currentExportDate:yyyy-MM-dd}");
                    currentExportDate = currentExportDate.AddDays(1);
                }
            }

            // Process today separately, knowing it might be incomplete and will be overwritten next run.
            var today = DateTime.UtcNow.Date;
            Console.WriteLine($"Processing today's data ({today:yyyy-MM-dd}) separately (will be overwritten on next run)...");
            await ProcessDayAsync(today);
            // DO NOT update the state blob after processing today, so the next run re-processes it.
            Console.WriteLine($"Finished processing today's data: {today:yyyy-MM-dd}. State remains at {endDateToProcess:yyyy-MM-dd}.");
            Console.WriteLine("Export process completed successfully.");
        }
        catch (CosmosException cosmosEx) // Catch specific Cosmos exceptions first
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"A Cosmos DB specific error occurred: {cosmosEx.StatusCode} (Substatus: {cosmosEx.SubStatusCode})");
            Console.WriteLine($"Message: {cosmosEx.Message}");
            // Log detailed diagnostics from the failed request
            Console.WriteLine("--- Cosmos DB Diagnostics ---");
            Console.WriteLine(cosmosEx.Diagnostics?.ToString() ?? "No diagnostics available.");
            Console.WriteLine("--- End Diagnostics ---");
            Console.ResetColor();
            Environment.ExitCode = 4; // Indicate Cosmos DB specific error
        }
        catch (Azure.RequestFailedException azureEx) when (azureEx.Status == 403)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Authorization Error: The application's identity does not have the required permissions.");
            Console.WriteLine($"Please check Azure RBAC roles for the identity running this application on both the Cosmos DB account/database and the Storage Account/container.");
            Console.WriteLine($"Details: {azureEx.Message}");
            Console.ResetColor();
            Environment.ExitCode = 2; // Indicate auth error.
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"An unexpected error occurred: {ex.Message}");
            Console.WriteLine(ex.ToString());
            Console.ResetColor();
            Environment.ExitCode = 3; // Indicate general error.
        }
    }

    private static async Task<DateTime> GetStartDateToProcessAsync()
    {
        var stateBlobPath = $"cosmosdb/{_stateBlobName}";
        var stateBlobClient = _blobContainerClient.GetBlobClient(stateBlobPath);
        // Initial default start date (used if state blob is invalid AND Cosmos query fails)
        var defaultStartDate = DateTime.UtcNow.Date.AddYears(-2);
        var lastExportDate = defaultStartDate;

        var stateFoundAndValid = false;

        try
        {
            if (await stateBlobClient.ExistsAsync())
            {
                Console.WriteLine($"Reading state from blob: {stateBlobPath}");
                var response = await stateBlobClient.DownloadContentAsync();
                var state = JsonSerializer.Deserialize<ExportState>(response.Value.Content.ToStream());
                if (state != null && state.LastExportDateUtc > DateTime.MinValue)
                {
                    lastExportDate = DateTime.SpecifyKind(state.LastExportDateUtc.Date, DateTimeKind.Utc);
                    Console.WriteLine($"Found previous state. Last successfully exported date (UTC): {lastExportDate:yyyy-MM-dd}");
                    stateFoundAndValid = true; // Mark state as successfully read.
                }
                else
                {
                    Console.WriteLine($"State blob '{stateBlobPath}' contained invalid data or MinValue date.");
                }
            }
            else
            {
                Console.WriteLine($"State blob '{stateBlobPath}' not found.");
            }
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Warning: Could not read or parse state blob '{stateBlobPath}'. Error: {ex.Message}");
            Console.ResetColor();
            // Proceed as if state was not found.
        }

        // If state wasn't found or was invalid, try querying Cosmos DB for the oldest record.
        if (!stateFoundAndValid)
        {
            Console.WriteLine("No valid state found. Querying Cosmos DB for the minimum timestamp...");
            try
            {
                var minTimestampQuery = new QueryDefinition(
                    "SELECT VALUE MIN(c.timeStamp) FROM c WHERE c.type = 'Message'");

                using FeedIterator<string> feedIterator = _cosmosContainer.GetItemQueryIterator<string>(minTimestampQuery);

                string minTimestampString = null;
                if (feedIterator.HasMoreResults)
                {
                    FeedResponse<string> response = await feedIterator.ReadNextAsync();
                    minTimestampString = response.FirstOrDefault(); // Should be only one result (or null)
                    Console.WriteLine($"Cosmos DB minimum timestamp query returned: '{minTimestampString ?? "null"}' (RU: {response.RequestCharge:F2})");
                }

                if (!string.IsNullOrEmpty(minTimestampString) &&
                    DateTime.TryParse(minTimestampString, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out DateTime minTimestamp))
                {
                    // Successfully found the earliest record date.
                    // We want to start processing *on* this day.
                    // The calling logic adds 1 day, so set lastExportDate to the day *before* the min timestamp.
                    lastExportDate = minTimestamp.Date.AddDays(-1);
                    Console.WriteLine($"Setting effective start date based on oldest record: process from {lastExportDate.AddDays(1):yyyy-MM-dd}");
                }
                else
                {
                    Console.WriteLine($"Could not determine minimum timestamp from Cosmos DB or container is empty. Using default start date: {defaultStartDate:yyyy-MM-dd}");
                    lastExportDate = defaultStartDate.AddDays(-1); // Set to day before default start.
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error querying Cosmos DB for minimum timestamp: {ex.Message}. Using default start date: {defaultStartDate:yyyy-MM-dd}");
                Console.ResetColor();
                lastExportDate = defaultStartDate.AddDays(-1); // Fallback to default on error.
            }
        }

        // Start processing from the day of the determined last successfully exported date to ensure all data is captured.
        return lastExportDate;
    }

    private static async Task UpdateStateAsync(DateTime lastProcessedDateUtc)
    {
        var stateBlobPath = $"cosmosdb/{_stateBlobName}";
        var stateBlobClient = _blobContainerClient.GetBlobClient(stateBlobPath);
        // Ensure we store the date part only and specify UTC kind.
        var state = new ExportState(DateTime.SpecifyKind(lastProcessedDateUtc.Date, DateTimeKind.Utc));
        var stateJson = JsonSerializer.Serialize(state);
        var content = Encoding.UTF8.GetBytes(stateJson);

        Console.WriteLine($"Updating state in '{stateBlobPath}' to: {state.LastExportDateUtc:yyyy-MM-dd}");
        using (var stream = new MemoryStream(content))
        {
            try
            {
                await stateBlobClient.UploadAsync(stream, new BlobUploadOptions { AccessTier = AccessTier.Cool }); // Example: Set tier
                Console.WriteLine($"State successfully updated.");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error updating state blob '{stateBlobPath}': {ex.Message}");
                Console.ResetColor();
                // Depending on requirements, you might want to retry or throw here
                // to prevent processing the next day if the state couldn't be saved.
                throw; // Re-throw to halt the process if state saving fails
            }
        }
    }

    private static async Task ProcessDayAsync(DateTime dateToProcessUtc)
    {
        // Ensure date is treated as UTC
        dateToProcessUtc = DateTime.SpecifyKind(dateToProcessUtc.Date, DateTimeKind.Utc);
        Console.WriteLine($"--------------------------------------------------");
        Console.WriteLine($"Processing date (UTC): {dateToProcessUtc:yyyy-MM-dd}");

        // Define the date range for the query using ISO 8601 format for Cosmos DB
        // Ensures comparison works correctly regardless of timestamp precision in Cosmos
        string startDateIso = dateToProcessUtc.ToString("o", CultureInfo.InvariantCulture); // e.g., 2023-10-27T00:00:00.0000000Z
        string endDateIso = dateToProcessUtc.AddDays(1).ToString("o", CultureInfo.InvariantCulture); // e.g., 2023-10-28T00:00:00.0000000Z

        // Construct the Cosmos DB Query for the specific day
        var queryDefinition = new QueryDefinition(
            @"SELECT c.id, c.sessionId, c.timeStamp, c.sender, c.senderDisplayName, c.tokens, c.upn, c.deleted, c.status, 'Message' as type
              FROM c
              WHERE c.type = 'Message'
              AND c.timeStamp >= @startDate AND c.timeStamp < @endDate
              ORDER BY c.timeStamp ASC")
            .WithParameter("@startDate", startDateIso)
            .WithParameter("@endDate", endDateIso);

        Console.WriteLine($"Executing Cosmos Query from {startDateIso} (inclusive) to {endDateIso} (exclusive)");
        // Use a FeedIterator to handle potential pagination and large result sets
        using FeedIterator<MessageRecord> feedIterator = _cosmosContainer.GetItemQueryIterator<MessageRecord>(
            queryDefinition,
            requestOptions: new QueryRequestOptions
            {
                // Optional: MaxItemCount can be tuned based on expected item size and network latency.
                // Lowering this (e.g., 100 or 500) might help if large pages cause client-side stress,
                // but it won't fix underlying connectivity issues. Default is often 1000 or based on response size.
                MaxItemCount = 500, 
                // ConsistencyLevel = ConsistencyLevel.Session // Optional: Specify consistency
            });

        var records = new List<MessageRecord>();
        double totalRequestCharge = 0;
        int pageCount = 0;
        TimeSpan queryTime = TimeSpan.Zero;
        var queryStopwatch = System.Diagnostics.Stopwatch.StartNew();


        // Read results page by page
        while (feedIterator.HasMoreResults)
        {
            pageCount++;
            FeedResponse<MessageRecord> response;
            try
            {
                response = await feedIterator.ReadNextAsync();
            }
            catch (CosmosException cosmosEx) when (cosmosEx.StatusCode == System.Net.HttpStatusCode.RequestEntityTooLarge)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"Warning: Query response too large for a single page (RequestEntityTooLarge). Consider refining the query or reducing MaxItemCount if set.");
                Console.ResetColor();
                // Potentially break or handle differently if this happens
                break; // Stop processing this day if a page is too large to retrieve
            }

            records.AddRange(response);
            totalRequestCharge += response.RequestCharge;
            queryTime += response.Diagnostics.GetClientElapsedTime(); // Capture client-side time

            // Log progress periodically for large datasets
            if (pageCount == 1 || pageCount % 10 == 0 || records.Count % 1000 == 0 && records.Count > 0)
            {
                Console.WriteLine($"  ... fetched page {pageCount} ({response.Count} records, RU: {response.RequestCharge:F2}). Total for day: {records.Count}.");
            }
        }
        queryStopwatch.Stop();

        Console.WriteLine($"Query finished for {dateToProcessUtc:yyyy-MM-dd}.");
        Console.WriteLine($"  Total records found: {records.Count}");
        Console.WriteLine($"  Total RUs consumed: {totalRequestCharge:F2}");
        Console.WriteLine($"  Client-side query elapsed time: {queryStopwatch.Elapsed.TotalSeconds:F2}s (SDK reported: {queryTime.TotalSeconds:F2}s)");


        // Define CSV blob name within the 'cosmosdb' virtual folder
        string blobName = $"cosmosdb/{dateToProcessUtc:yyyy-MM-dd}-Messages.csv";
        BlobClient blobClient = _blobContainerClient.GetBlobClient(blobName);

        if (records.Any())
        {
            Console.WriteLine($"Writing {records.Count} records to CSV format...");
            // Write records to CSV in memory
            using var memoryStream = new MemoryStream();
            // Ensure UTF-8 without BOM for better compatibility, especially with Power BI
            using (var writer = new StreamWriter(memoryStream, new UTF8Encoding(false), leaveOpen: true))
            using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = true }))
            {
                // Register the custom map
                csv.Context.RegisterClassMap<MessageRecordMap>();
                csv.WriteRecords(records);
            } // StreamWriter and CsvWriter are disposed here, flushing data to memoryStream

            memoryStream.Position = 0; // Reset stream position for reading by upload

            var blobUploadOptions = new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders { ContentType = "text/csv" },
                AccessTier = AccessTier.Cool // Use Cool tier for less frequently accessed archive data
            };

            Console.WriteLine($"Uploading CSV to Blob Storage: {blobName} ({memoryStream.Length / 1024.0:F2} KB)...");
            var uploadStopwatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await blobClient.UploadAsync(memoryStream, blobUploadOptions); // Overwrites by default if blob exists
                uploadStopwatch.Stop();
                Console.WriteLine($"Successfully uploaded {blobName} in {uploadStopwatch.Elapsed.TotalSeconds:F2}s.");
            }
            catch (Exception ex)
            {
                uploadStopwatch.Stop();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error uploading blob '{blobName}' after {uploadStopwatch.Elapsed.TotalSeconds:F2}s: {ex.Message}");
                Console.ResetColor();
                // Decide if failure to upload a day's file should halt the entire process
                throw; // Re-throw to halt the process if upload fails
            }
        }
        else
        {
            Console.WriteLine($"No records found for {dateToProcessUtc:yyyy-MM-dd}.");
            // Check if a blob exists for this day and delete it to ensure no stale data remains
            // Also check within the 'cosmosdb' folder
            string blobToDeleteName = $"cosmosdb/{dateToProcessUtc:yyyy-MM-dd}-Messages.csv";
            BlobClient blobToDeleteClient = _blobContainerClient.GetBlobClient(blobToDeleteName);
            bool blobExists = await blobToDeleteClient.ExistsAsync();
            if (blobExists)
            {
                Console.WriteLine($"Deleting existing blob (as no records were found for this date): {blobToDeleteName}");
                await blobToDeleteClient.DeleteIfExistsAsync();
            }
            else
            {
                Console.WriteLine($"No CSV file generated or uploaded as no records were found.");
            }
        }
        Console.WriteLine($"Finished processing for {dateToProcessUtc:yyyy-MM-dd}.");
        Console.WriteLine($"--------------------------------------------------");
    }
}

// Define a CsvClassMap for custom CSV generation rules
public sealed class MessageRecordMap : ClassMap<MessageRecord>
{
    public MessageRecordMap()
    {
        AutoMap(CultureInfo.InvariantCulture); // Map properties automatically first

        // Custom mapping for the 'status' field
        Map(m => m.status).Name("Status").Convert(args =>
        {
            // args.Value is the MessageRecord object
            // args.Value.status is the string value from Cosmos DB (assuming it's read as string)
            if (int.TryParse(args.Value.status, out int statusValue))
            {
                return statusValue switch
                {
                    0 => "Pending",
                    1 => "InProgress",
                    2 => "Completed",
                    3 => "Failed",
                    _ => $"Unknown ({statusValue})" // Handle unexpected integer values
                };
            }
            // Handle cases where status is not a valid integer or is null/empty
            return string.IsNullOrEmpty(args.Value.status) ? "Not Specified" : $"Invalid ({args.Value.status})";
        });

        // Optional: Rename other headers if desired
        // Map(m => m.id).Name("MessageId");
        // Map(m => m.sessionId).Name("SessionId");
        Map(m => m.timeStamp).Name("Timestamp"); // Example: Rename timeStamp header
        // Map(m => m.senderDisplayName).Name("SenderName");

        // Ensure nullable fields are handled (AutoMap usually does this, but explicit is fine)
        Map(m => m.tokens).Optional();
        Map(m => m.deleted).Optional();

        // Explicitly map 'type' if needed, or ignore it if not desired in CSV
        // By default AutoMap includes it. Use Ignore() to exclude it.
        // Map(m => m.type).Ignore();
    }
}
