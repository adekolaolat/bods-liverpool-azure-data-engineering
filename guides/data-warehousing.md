# Data warehousing - Azure Synapse Analytics

To connect to my Gold layer in Azure Data Lake Storage (ADLS), I'm using Azure Synapse Analytics with a serverless SQL pool. This allows me to query Delta tables stored in ADLS directly.

For this project, I’ve created a Synapse workspace to manage and query the data. The idea is to create SQL view that fetches data for the previous day’s bus activity.

I'm using a view instead of creating external tables for the following reasons:
- The daily dataset is relatively small— approximately 65MB—which makes on-demand querying efficient. 7 days data should just be shy of 500MB which should be enough to pull into Power BI Desktop.

- Views in serverless SQL pools enable querying data directly from ADLS without the need to move or duplicate it.

Creating the external tables would incur higher costs, especially during inital phase when exploring the data. For this use case ADLS Gen2 should be enough.


## Plan
- Set up Synapse Workspace
- Grant Synapse workspace access to ADLS
- Connect gold data to Synapse
- Create views that fetch bus activity data for the previous day and last 7 days.

## Concept Applied
- [Serverless SQL pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/on-demand-workspace-overview)
- [Querying Delta Lake files](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/query-delta-lake-format)
- External Tables and Views

## Setting up Azure Synapase

- Create an Azure Synapse Analytics workspace.

Configure :
- Workspace name - `livbodsbus-synapse`
- Subscription
- Resource group as 'livbusbods'
- Region to `UK West` as the other resources.
- Select the ADLS Gen 2 resource - (`livbusdatastore`)
- Select file-system or create a new one.
- Create the resource.


## Grant Synapse Managed Identity Access to Storage

In the Azure Portal:

Go to `livbusdatastore` storage account

Go to Access Control (IAM) > Add Role Assignment

- Assign Role: `Storage Blob Data Reader`

- Assign to: Managed Identity

- Select the Synapse workspace - `livbodsbus-synapse`


## Query bus_activity table from Synapse


After deploying, Launch Synapse studio.
In the Synapse studio, 
- Go to Data
- Under Linked, brouwse to `bus_activity` under `gold` directory.
- Click on New SQL script and 'Select TOP 100 Rows'
- Choose `Delta` as the format and then Apply.

A query to select TOP 100 rows from the bus_activity table would be auto-generated.

Query to create an External table can also be created using a similar method, but you'll need to provide data base name 


## Create View for the daily report

In Synapse studio:
  - Go to **Develop**.
  - Create a new resource and select **SQL script**.

Run the script below to create a view that gets previous day's data.

This script sets up a database `livbusdb`, creates the credential using Managed Identity and an external data source and finally creates the view `vw_bus_activity_yesterday`

```
-- Create the database if it doesn't exist
IF DB_ID('livbusdb') IS NULL
BEGIN
    CREATE DATABASE livbusdb;
END;
GO

-- Use the livbusdb database
USE livbusdb;
GO

-- Create a database scoped credential using Managed Identity
IF NOT EXISTS (
    SELECT * FROM sys.database_scoped_credentials
    WHERE name = 'livbus_cred'
)
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL livbus_cred
    WITH IDENTITY = 'Managed Identity';
END;
GO

-- Create an external data source pointing to the storage account
IF NOT EXISTS (
    SELECT * FROM sys.external_data_sources
    WHERE name = 'livbus_datasource'
)
BEGIN
    CREATE EXTERNAL DATA SOURCE livbus_datasource
    WITH (
        LOCATION = 'https://livbusdatastore.dfs.core.windows.net',
        CREDENTIAL = livbus_cred
    );
END;
GO

-- Create or alter the view for yesterday's bus activity
CREATE OR ALTER VIEW vw_bus_activity_yesterday AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/bus_activity/',
    DATA_SOURCE = 'livbus_datasource',
    FORMAT = 'DELTA'
) AS [data]
WHERE CAST(ingestion_date AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE);
GO


```




## Create a View for last 7 days of data.

This view gets data for the last 7 days.
```
CREATE OR ALTER VIEW vw_bus_activity_last7days AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/bus_activity/',
    DATA_SOURCE = 'livbus_datasource',
    FORMAT = 'DELTA'
) AS [data]
WHERE CAST(ingestion_date AS DATE) BETWEEN 
      DATEADD(day, -7, CAST(GETDATE() AS DATE)) AND 
      DATEADD(day, -1, CAST(GETDATE() AS DATE));
```



[ ⏮️ Transformation](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-transformation.md)

[ ⏭️Go to BI integration](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-viz.md)