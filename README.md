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
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```
2.  **Configure Application Settings:**
    Create or modify the `appsettings.json` file in the project's output directory (e.g., `bin/Debug/net8.0`). Ensure the file's properties in your IDE are set to "Copy to Output Directory: Copy if newer" or "Copy always".

    **`appsettings.json` (Base Configuration):**
    ```json
    {
      "CosmosDbEndpoint": "https://your-cosmos-account.documents.azure.com/",
      "CosmosDbDatabase": "your-database-name",
      "CosmosDbContainer": "Sessions",
      "StorageAccountName": "yourstorageaccountname",
      "StorageContainerName": "exported-data", // Container for all exports
      "StateBlobName": "export_state.json" // Base name for the state file
    }
    ```

    *   `CosmosDbEndpoint`: The URI of your Azure Cosmos DB account.
    *   `CosmosDbDatabase`: The name of the database containing the 'Sessions' container.
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

## Power BI Integration

You can now use the Azure Blob Storage container (`exported-data` in the example config) as a data source in Power BI.
1.  In Power BI Desktop, select "Get data" -> "Azure" -> "Azure Blob Storage".
2.  Enter the Storage Account name or URL.
3.  Authenticate as appropriate (e.g., using your organizational account if you granted yourself permissions).
4.  Navigate to the specified container (`exported-data`).
5.  You should see the `cosmosdb` folder. Power BI can treat this folder as a source and combine the CSV files within it. Select the `cosmosdb` folder (or the container and filter later).
6.  Click "Transform Data".
7.  In the Power Query Editor, filter the "Folder Path" column to include only your `cosmosdb/` folder if necessary.
8.  Click the "Combine Files" button (double downward arrows) on the "Content" column.
9.  Power BI will analyze the files and create helper queries to combine them into a single table containing all the message records across the daily CSV files.
10. You can now model and visualize this data. Remember to refresh the dataset in Power BI to pick up newly exported daily files.

## Using the Provided Power BI Report (.pbix)

If a `.pbix` file is included in this repository, it is pre-configured to connect to the structure created by this export tool. However, you will need to point it to *your* specific Azure Storage account.

1.  **Open the `.pbix` file** in Power BI Desktop.
2.  Go to the **Home** tab, click **Transform data**, and then select **Data source settings**.
3.  In the **Data source settings** dialog, you should see an existing **Azure Blob Storage** connection. Select it.
4.  Click the **Change Source...** button.
5.  In the **Azure Blob Storage** dialog that appears, update the **Account name or URL** field to match the `StorageAccountName` you configured in your `appsettings.json` or environment variables for the exporter application (e.g., `yourstorageaccountname` or `https://yourstorageaccountname.blob.core.windows.net`).
6.  Click **OK**.
7.  You might be prompted to **Edit Credentials** or sign in again. Use the same Azure account you use for `az login` or an account with at least `Storage Blob Data Reader` permissions on the storage container.
8.  Click **Close** on the **Data source settings** dialog.
9.  If the Power Query Editor opens, click **Close & Apply** on the **Home** tab.
10. Back in the main Power BI report view, click **Refresh** on the **Home** tab to load the data from your storage account into the report. 