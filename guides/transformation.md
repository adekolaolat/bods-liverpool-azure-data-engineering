# Transformation

## More digging into the raw data
Undersatnding the structure and schema of the raw data is important for the transformation of the raw data .


The raw json files has a UTF-8 with BOM encoding which I'll put in mind when reading the files.

Extracting all possible fields from the raw data (Vehicle Activity element) for each of the operators, I realized the raw data for the two operators have overlapping, but not identical schemas.

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

I also want to include a timestamp which will be extracted from the file name. This timestamp indicates when the live data was collected. This, I am sure of, since I'm yet to understand what the timestamps field means in the raw file.

How do I want to process into the silver?, How often?

At first I might want to load all the data and then subsequently, I may want to update this weekly.

## What's the plan?

- Use Azure Databricks for transformation of raw data.
- Create Azure Databricks Workspace
- How do I set up Azure Databricks to connect to ADLS gen2?
- What do I need to set up?
- 




## Task requirements
- 
- I need the code to extract timestamp from the filename add to the flattened table
- I'll be using a single silver entity table to hold the data.
- Since I have collected some data already, I need to do a batch loading of the historical data from `bronze` container and then updating weekly with new data, all flowing into the `silver` container.

- Consider using Auto Loader as an option to leverage data updates. [Why use Autoloader?](https://www.youtube.com/watch?v=8a38Fv9cpd8)





## Concepts applied
- [Delta Lake](https://learn.microsoft.com/en-us/azure/databricks/introduction/delta-comparison)
- [DLT](https://learn.microsoft.com/en-us/azure/databricks/dlt/)
- [ETL pipeline with DLT](https://learn.microsoft.com/en-us/azure/databricks/getting-started/data-pipeline-get-started)
- [Autoloader][https://www.youtube.com/watch?v=8a38Fv9cpd8]



## Set Up Azure Databricks Workspace

- Go to Azure Portal.

- Create a Databricks workspace.

- Choose your subscription, resource group, and location.
-  Set workspace  to `livbusbods-dbx`
- After deployment, launch the workspace and create a new cluster.

**Took a while for my Databricks resource to be deployed.**

In Databricks UI, go to Compute → Create Cluster

Fill out:

Cluster name:

Cluster mode: Single node

Databricks Runtime Version: Use latest LTS (e.g., 13.x LTS with Spark 3.x)

Unchecked Photon Acceleration to save cost.

Worker type: Choose based on your needs (for dev: Standard_DS3_v2 is fine)

Click Create Cluster
**Cluster will take a few minutes to spin up**

Accessing DataLake in Gen2

## [How to Connect ADLS Gen2 in Databricks](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage)

- Register an Application to create the client ID  and secret. In this context, Databrick is the client.

- Create the client Secret

- The app registration has created a service principal.

- Assign role of `Storage Blob Data Contributor` to the Service principal created.


- Next is to save the Secrets that has been created in a Key vault.

-Import the Service principal Secrets ( Copy value from the sercice principal) into the Key vault .
Click Generate/Import in the Key vault resource

In Databricks : 
- Create scope secrets within Databricks - This can be created with Databrick CLI or append `/#secrets/createScope` to the end of Base URL in your browser to create in the UI.

- Create secrete name.

- Appication Id as the DNS name

- Key vault URL - Resource ID

## Now I can connect to ADLS Gen2 from Databrick

## Next is to Write code to read the file, transform in to a structured (tabular) format.

**I need the code to extract timestamp and add to the flattened table**

## And Save the structured format into the silver container.

I've wriiten code to save to the delta to the container.

Read more on this.


## After runnning verifying the Data

Set up Databricks Activity in ADF
Give permission to ADF to have access to Databricks
Give permison to ADF to have access to keyVault via IAM(Access control)

## Readings

Read up on Delta Table 

Read up on AutoLoader - Auto Loader remembers what's already processed via the checkpoint.

## Notebook - Transform data to Silver container

- Set up Configuration to connect to ADLS
- Read data from partioned paths using the Autoloader
- Tranform data into a flat table
- Write data into silver contaners as delta live table
- Register table for SQL Acesss

## Integrating Notbook into ADF

Create new pipeline to add a new
Set up Databricks Activity in ADF
Add Linked service to Azure Databricks Notebook in the Activity
Give permission to ADF to have access to Databricks(Or Use Access Key)
Create Access Key in Databricks workspace.
Configure Cluster to use. You can use the same compute.
Give permison to ADF to have access to keyVault via IAM(Access control)

Cofigure 


[Silver Transformation Notebook](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/add-tranformation/notebooks/Silver%20Notebook.ipynb)

## Used personal

## Readings
- [Auto Loader Readings](https://aeshantechhub.co.uk/databricks-autoloader-advanced-techniques-and-best-practices/)





## Cost Considerations

```
cloudFiles.maxBytesPerTrigger
Type: Byte String
The maximum number of new bytes to be processed in every trigger. You can specify a byte string such as 10g to limit each microbatch to 10 GB of data. This is a soft maximum. If you have files that are 3 GB each, Azure Databricks processes 12 GB in a microbatch. When used together with cloudFiles.maxFilesPerTrigger, Azure Databricks consumes up to the lower limit of cloudFiles.maxFilesPerTrigger or cloudFiles.maxBytesPerTrigger, whichever is reached first. This option has no effect when used with Trigger.Once() (Trigger.Once() is deprecated).
Default value: None


For this project - I set my maxFiles per Trigger to 
```

```
cloudFiles.maxFilesPerTrigger

Type: Integer
The maximum number of new files to be processed in every trigger. When used together with cloudFiles.maxBytesPerTrigger, Databricks consumes up to the lower limit of cloudFiles.maxFilesPerTrigger or cloudFiles.maxBytesPerTrigger, whichever is reached first. This option has no effect when used with Trigger.Once() (deprecated).

Default value: 1000
```



