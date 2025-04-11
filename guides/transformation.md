# Transformation

## More digging into the raw data
Undersatnding the structure of the raw data is important for the transformation of the raw data .


The raw json files has a UTF-8 with BOM encoding which I'll considered when reading the files.

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




## Task requirements
-




## Concepts applied
- [Delta Lake](https://learn.microsoft.com/en-us/azure/databricks/introduction/delta-comparison)
- [DLT](https://learn.microsoft.com/en-us/azure/databricks/dlt/)
- [ETL pipeline with DLT](https://learn.microsoft.com/en-us/azure/databricks/getting-started/data-pipeline-get-started)
-



## Set Up Azure Databricks Workspace

- Go to Azure Portal.

- Create a Databricks workspace.

- Choose your subscription, resource group, and location.

- After deployment, launch the workspace and create a new cluster.