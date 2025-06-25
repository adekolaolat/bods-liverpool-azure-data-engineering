# Transformation

In this phase, I used Databricks (built on Apache Spark) to pull in the raw files from storage and transform them into a more structured format for the silver and gold layers. It is all part of a lakehouse setup, which sits on top of a data lake (ADLS Gen2) using Delta tables. The approach combines the SQL-based capabilities you would expect from a data warehouse with the flexibility and scalability of a data lake.

## More digging into the raw data

Understanding the structure and schema of the raw data is important before transforming it. The raw JSON files use UTF-8 with BOM encoding, so I’ll make sure to handle that properly when reading them. After looking at the VehicleActivity element for both operators, I found that their schemas are similar but not exactly the same.

After flattening the raw data into a tabular manner I got this number of possible fields for each operator. 


```
['RecordedAtTime', 'ItemIdentifier', 'ValidUntilTime', 'LineRef',
       'DirectionRef', 'DataFrameRef', 'DatedVehicleJourneyRef',
       'PublishedLineName', 'OperatorRef', 'OriginRef', 'OriginName',
       'DestinationRef', 'DestinationName', 'OriginAimedDepartureTime',
       'DestinationAimedArrivalTime', 'Longitude', 'Latitude', 'BlockRef',
       'VehicleRef', 'TicketMachineServiceCode', 'JourneyCode',
       'VehicleUniqueId', 'Bearing', 'Monitored',
       'DriverRef']
```
Along with this dimension info

```
Total columns in AMSY: 24
Total columns in SCCU: 21
Common columns: 19

Columns only in AMSY:
{'VehicleUniqueId', 'JourneyCode', 'DestinationAimedArrivalTime', 'TicketMachineServiceCode', 'BlockRef'}

Columns only in SCCU:
{'DriverRef', 'Monitored'}
```

Running this [script](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/process_raw_bus.py) should give a similar results.


I’ve flattened the data into a table format and listed all the possible fields for each operator. I’m also adding a timestamp column based on the file name, since that reliably tells me when the data was collected. 

How I plan to process the Silver layer:

To start, I’ll load all the existing data into the Silver layer (batch load). After that, I’ll set things up to update the Silver layer daily as new data comes in.


## Plan

- Use Azure Databricks for transformation of raw data.
- Create Azure Databricks Workspace
- [Set up Azure Databricks to connect to ADLS gen2](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage)
- Set up Azure key vault to hold secrets.
- Implement silver notbook 
- Implement gold notebook
- Integrate silver and gold notebook into pipeline (ADF).


## Task requirements
- Implement code to extract timestamp from the filename add to the flattened table
- I'll build up a silver table to hold the data.
- Since I have collected some data already, I need to do a batch loading of the historical data from `bronze` container and then updating daily with new data, all flowing into the `silver` container.
- Use Auto Loader to read files from bonze container. [Why use Autoloader?](https://www.youtube.com/watch?v=8a38Fv9cpd8)
- Implement batch and incremental loading for gold notebook.
- Daily updates of data in the silver and gold layer.




## Concepts applied
- [Delta Lake](https://learn.microsoft.com/en-us/azure/databricks/introduction/delta-comparison)
- [DLT](https://learn.microsoft.com/en-us/azure/databricks/dlt/)
- [ETL pipeline with DLT](https://learn.microsoft.com/en-us/azure/databricks/getting-started/data-pipeline-get-started)
- [Autoloader](https://www.youtube.com/watch?v=8a38Fv9cpd8)
- Apache Spark
- Batch Loading
- Incemental Loading


## Set Up Azure Databricks Workspace

- Go to Azure Portal.

- Create a Databricks workspace.

- Choose your subscription, resource group, and location.
-  Set workspace  to `livbusbods-dbx`
- After deployment, launch the workspace and create a new cluster.

**Takes a while for my Databricks resource to be deployed.**

### Set up Cluster
In Databricks UI, go to Compute → Create Cluster

Fill out:

  - Cluster name

  - Cluster mode: Single node

  - Databricks Runtime Version: Use latest LTS (e.g., 13.x LTS with Spark 3.x)

  - Unchecked Photon Acceleration to save cost.

  - Worker type: Choose based on your needs (for dev: Standard_DS3_v2 is fine)

- Click Create Cluster

**Cluster will take few minutes to spin up**


## Connecting ADLS Gen2 in Databricks

- Register an Application to create the client ID  and secret. In this context, Databrick is the client.

- Create the client Secret

- The app registration has created a service principal.

- Assign role of `Storage Blob Data Contributor` to the Service principal created.


- Next is to save the Secrets that has been created in a Key vault (Create Key Vault resource).

- Import the Service principal Secrets ( Copy value from the service principal) into the Key vault .
Click Generate/Import in the Key vault resource

In Databricks : 
- Create scope secrets within Databricks - This can be created with Databrick CLI or append `/#secrets/createScope` to the end of Base URL in your browser to create in the UI.

- Create secrete name.

- Appication ID as the DNS name

- Key vault URL - Resource ID

## Silver Notebook Implementation
The [silver notebook](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/Silver%20Notebook.ipynb) uses PySpark to processes raw JSON data  stored in Azure Data Lake (Bronze layer), flattens it, and writes it to the Silver layer as a Delta table.

The raw data is heavily nested.

Implementation of the [silver notebook](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/Silver%20Notebook.ipynb) include : 

### Authentication and Configuration

- Retrieves Azure AD service principal credentials (client ID, secret, tenant ID) securely using Databricks secrets from the Azure Key Vault that was set up.

- Configures Spark to authenticate to Azure Data Lake Storage Gen2 (ADLS Gen2) via service principal.


### Reading Raw Data from `bronze`

 - Uses Auto Loader (cloudFiles) to continuously or batch-read new JSON files from the bronze path.

- Infers schema and stores metadata at a defined schema location.

- Limits batch size to 600 files.

### Metadata Extraction

- Adds some metadata column that's used to extract timestamps:

  - file_path — the source file’s full path.

  - file_timestamp — extracted from the filename for tracking and partitioning.

 - Extracts the bus `operator_ref` from the file path for routing and partitioning.

### Data Flattening

- Parses the nested Siri JSON structure and explodes VehicleActivity records.

- Flattens fields related to:

  - Journey (e.g., line_ref, direction_ref, block_ref, vehicle_ref)

  - Timing (e.g., recorded_at_time, origin_aimed_departure_time)

  - Location (e.g., longitude, latitude)

  - Optional attributes (e.g., ticket_machine_service_code, vehicle_unique_id)
  - Handles missing fields.

### Timestamp Conversion
- Converts file_timestamp into datetime timestamp and extracting year, month and day.

### Writing to Silver Layer
- Writes the cleaned, structured data to the Silver layer as a Delta Lake table.

- Output is partitioned by `operator_ref`, `year`, `month`, and `day`.

- Uses trigger once mode to simulate batch execution in a streaming context.

- Defines a checkpoint location to track progress and ensure fault tolerance.


### Registering Delta Table for SQL Access
- Registers the Delta table at for querying via Spark SQL or Databricks SQL.



[Silver Transformation Notebook](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/Silver%20Notebook.ipynb)

## Getting Gold layer from Silver.

The gold container would hold Delta Live Tables for :

**bus_activity** - Contains enriched data (columns) which can be used for further logic like detecting only moving buses, filtering buses within boundary box of liverpool, etc.

**bus trips** -  Gives the the daily count of trips and line information that will be used to 

The transformation to gold layer include two  scripts. 

One for  Batch Loading - Load existing data from delta lake table.

and Incremental loading for the data - implement notebook to load previous day's data  to delta lake table.

## Gold Notebook Implementation

The [gold notebook](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/Gold%20Notebook.ipynb) implementation include:

### Date Setup and data loading from silver
- Defines `yesterday` and `day before yesterday` for partition filtering.
- Load yesterday's records from Silver.
- Load latest vehicle records from day before yesterday for continuity.

### Combine & Geo-Tagging
- Union both datasets.
- Add `in_liverpool` flag based on bounding box coordinates.

### Add temporal features
- Use window functions to compute:
  - Previous location/time
  - Time difference (`dur_min_since_last_recorded`)
  - Idle status (`possibly_idle`)

### Movement Classification
- Tag movements of the buses wrt the liverpool area:
  
### Route filtering
- Keep only bus routes that pass through Liverpool.
- Apply route and location filters from JSON config.

### Cleanup & Write
- Remove existing Gold records for the same date.
- Write enriched data to Gold Delta table (partitioned by date).


## Orchestrate Silver and Gold Notebook using ADF


- Create new pipeline in ADF
- Set up Databricks Activity in ADF
- Add Linked service to Azure Databricks Notebook in the Activity
- Give permission to ADF to have access to Databricks(Or Use Access Key)
- Create Access Key in Databricks workspace.
Configure the Cluster to use. You can use the same compute.
- Give permison to ADF to have access to keyVault via IAM (Access control)
- Add the the silver and gold notebook to pipeline pane.
- Add a daily schedule trigger to the pipeline.


[Gold Notebook](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/Gold%20Notebook.ipynb)

[Gold Notebook - Batch Load](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/Gold%20Notebook%20-%20Batch%20Load.ipynb)
## Helpful Readings
- [Auto Loader Readings](https://aeshantechhub.co.uk/databricks-autoloader-advanced-techniques-and-best-practices/)

## Cost Considerations

- Using Autoloader in silver transformation and incremental loading in gold notebook helps reduce costs by processing and transferring only new or changed data, rather than reloading the entire dataset each time. This minimizes compute time, storage usage, and data movement, which can significantly lower overall cloud credits.

```
cloudFiles.maxBytesPerTrigger
Type: Byte String
The maximum number of new bytes to be processed in every trigger. You can specify a byte string such as 10g to limit each microbatch to 10 GB of data. This is a soft maximum. If you have files that are 3 GB each, Azure Databricks processes 12 GB in a microbatch. When used together with cloudFiles.maxFilesPerTrigger, Azure Databricks consumes up to the lower limit of cloudFiles.maxFilesPerTrigger or cloudFiles.maxBytesPerTrigger, whichever is reached first. This option has no effect when used with Trigger.Once() (Trigger.Once() is deprecated).
Default value: None


For this project - I set my maxFiles per Trigger to 600
```

```
cloudFiles.maxFilesPerTrigger

Type: Integer
The maximum number of new files to be processed in every trigger. When used together with cloudFiles.maxBytesPerTrigger, Databricks consumes up to the lower limit of cloudFiles.maxFilesPerTrigger or cloudFiles.maxBytesPerTrigger, whichever is reached first. This option has no effect when used with Trigger.Once() (deprecated).

Default value: 1000
```

[ ⏮️Go to Data Ingestion](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-ingestion.md)

[ ⏭️ Data Warehousing](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-warehousing.md)
