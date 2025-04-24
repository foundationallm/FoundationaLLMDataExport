# Cosmos DB to Blob Storage Exporter

This .NET 8 console application exports message data from an Azure Cosmos DB container to CSV files in Azure Blob Storage. It uses Azure Identity for authentication, so ensure you are logged in via `az login` or that the application is running in an environment with appropriate managed identity configuration (like an Azure VM, App Service, etc.).

## Features

-   Connects to Azure Cosmos DB and Azure Blob Storage using `DefaultAzureCredential` (Managed Identity).
-   Queries messages of type 'Message' from the 'Sessions' container in Cosmos DB.
-   Exports data to daily CSV files within a `cosmosdb/` virtual folder in the specified Azure Blob Storage container.
-   Partitions CSV files by day based on the `timeStamp` field (e.g., `cosmosdb/YYYY-MM-DD-Messages.csv`).
-   Maps the numeric `status` field from Cosmos DB to human-readable strings ("Pending", "Completed", etc.) in the CSV output. Renames `timeStamp` header to `Timestamp`.
-   Tracks the last successfully exported date in a state file (`cosmosdb/export_state.json`).
-   On the first run (or if the state file is missing/invalid), it queries Cosmos DB for the earliest message timestamp to determine the starting date, avoiding unnecessary history queries.
-   Processes historical data incrementally up to yesterday, updating the state file after each day.
-   Processes today's data separately and overwrites the corresponding daily CSV on each run without updating the state file, ensuring the latest data for the current day is captured.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/foundationallm/FoundationaLLMDataExport
    cd FoundationaLLMDataExport
    ```
2.  **Configure Application Settings:**
    Create or modify the `appsettings.json` file in the project's output directory (e.g., `bin/Debug/net8.0`). Ensure the file's properties in your IDE are set to "Copy to Output Directory: Copy if newer" or "Copy always".

    **`appsettings.json` (Base Configuration):**
    ```json
    {
      "CosmosDbEndpoint": "https://your-cosmos-account.documents.azure.com/",
      "CosmosDbDatabase": "database",
      "CosmosDbContainer": "Sessions",
      "StorageAccountName": "yourstorageaccountname",
      "StorageContainerName": "exports", // Container for all exports
      "StateBlobName": "export_state.json" // Base name for the state file
    }
    ```

    *   `CosmosDbEndpoint`: The URI of your Azure Cosmos DB account.
    *   `CosmosDbDatabase`: The name of the database containing the 'Sessions' container (should be `database`).
    *   `CosmosDbContainer`: The name of the Cosmos DB container to query (should be `Sessions`).
    *   `StorageAccountName`: The name of your Azure Storage account. The Blob Storage endpoint URI will be constructed from this (e.g., `https://<yourstorageaccountname>.blob.core.windows.net/`).
    *   `StorageContainerName`: The name of the *root* container within the Blob Storage account. Daily CSVs and the state file will be placed inside a `cosmosdb/` folder within this container. This container must exist.
    *   `StateBlobName`: The base name for the state file. It will be stored as `cosmosdb/export_state.json`.

    **(Optional) `appsettings.Development.json` (Overrides):**
    Create this file in the same directory to override settings specifically for local development. For example:
    ```json
    {
      "CosmosDbEndpoint": "https://dev-cosmos-account.documents.azure.com/",
      "StorageAccountName": "devstorageaccount"
    }
    ```
    Remember to also set this file's properties to "Copy to Output Directory".

    **Environment Variables:**
    You can also override any setting using environment variables (e.g., `CosmosDbEndpoint=value dotnet run`). Environment variables take the highest precedence.

3.  **Azure Login (if running locally):**
    Ensure you have the Azure CLI installed and run:
    ```bash
    az login
    ```
    Log in with an account that has the required permissions:
    *   Cosmos DB: `Cosmos DB Built-in Data Reader` role (or custom role with read permissions) on the database/container.
    *   Storage Account: `Storage Blob Data Contributor` role on the target container (`StorageContainerName`).

4.  **Build the application:**
    ```bash
    dotnet build
    ```

## Running the Application

Execute the application from the project directory:

```bash
dotnet run
```

The application will:

1.  Load configuration from `appsettings.json`, `appsettings.Development.json` (if present), and environment variables.
2.  Connect to Azure services using `DefaultAzureCredential`.
3.  Attempt to read the last exported date from `cosmosdb/export_state.json` in the configured storage container.
4.  If no valid state is found, query Cosmos DB for the minimum `timeStamp` of type 'Message' to determine the start date.
5.  Iterate through each day from the start date up to yesterday (UTC), querying Cosmos DB for messages within that day's timestamp range.
6.  Write the results to a CSV file named `cosmosdb/YYYY-MM-DD-Messages.csv`, overwriting if it exists. Maps status codes to names.
7.  Update the `cosmosdb/export_state.json` blob with the date of the last successfully processed day (yesterday or earlier).
8.  Process today's (UTC) data separately and write/overwrite `cosmosdb/YYYY-MM-DD-Messages.csv` for today, *without* updating the state file.

## Power BI Integration using Template

This repository includes a Power BI Template file (`FoundationaLLM Message Stats.pbit`) designed to work with the CSV files generated by this exporter. Using the template allows you to quickly create a Power BI report connected to your specific data export location.

1.  **Ensure you have exported data:** Run the console application at least once to export some data to your Azure Blob Storage account in the `cosmosdb/` folder.
2.  **Open the template file:** Double-click the `FoundationaLLM Message Stats.pbit` file located in the root of this repository. This will open Power BI Desktop and start the process of creating a new report (`.pbix`) based on the template.
3.  **Enter Storage Account Name:** Power BI Desktop will prompt you to enter the **Azure Storage Account Name**. Enter the *exact* name of the Azure Storage account you configured in the `StorageAccountName` setting in your `appsettings.json` (or environment variable) for the data exporter (e.g., `yourstorageaccountname`). Do **not** enter the full URL here, just the account name.
4.  **Click Load:** After entering the storage account name, click the **Load** button.
5.  **Authenticate:** Power BI will likely need to authenticate to your Azure Storage account.
    *   Select **Organizational account** (or potentially **Microsoft account** depending on your setup) from the options on the left.
    *   Click **Sign in** and use the same Azure account you use for `az login` or an account that has at least `Storage Blob Data Reader` permissions on the storage container specified in `StorageContainerName`.
    *   Once signed in, click **Connect**.
6.  **Load Data:** Power BI will connect to your storage account, find the CSV files in the `cosmosdb/` folder, combine them, and load the data into the report model defined in the template. This might take a few moments depending on the amount of data.
7.  **Save the Report:** Once the data is loaded, Power BI Desktop will display the report based on the template. **Save** this new report as a `.pbix` file (e.g., `My Message Stats Report.pbix`) in a location of your choice. You now have a standard Power BI report file connected to your data.
8.  **Refresh Data:** To update the report with the latest exported data in the future, open your saved `.pbix` file and click the **Refresh** button on the **Home** tab in Power BI Desktop.
