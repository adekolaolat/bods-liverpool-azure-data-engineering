
# Azure pipeline to collect public bus location data. 

## Overview

This project focuses on building a scalable Azure data pipeline solution to collect public bus location in Liverpool via the **Bus Open Data Service (BODS)** API.


## About BODS

BODS has open data published by the operators of local bus and coach services across England which are timetable, location, fares and disruption data and this has been possible through The Bus Services Act 2017 that requires local bus operators in England to provide specific data between 2020 and 2021. 

The Bus Open Data Service offers free, license-free access to this information and allows users to give feedback to data publishers.


## Why store this data?

Currently, the Bus Open Data Service (BODS) only provides live location data, meaning there is no historical record to analyze trends, identify recurring issues, or build predictive models. By collecting and storing historical bus tracking data, we can analyze operational trends, predict potential delays, and improve service planning.


### Business Use Cases

- Transport Authorities & City Planning need historical data used to make decisons to improve route planning, predict delays, and enhance public transport efficiency.

- Bus Operators can leverage insights from analysis of the historical data to benchmark performance and make strategic decisions to gain a competitive edge or break monopoly in certain regions.

- Help public transport users save money on tickets for routes with multiple operators, reduce travel time, and improve their commuting experience.


## Requirements

- [ BODS gov.uk ](https://data.bus-data.dft.gov.uk/) account to get access to API documentations and API key.

- Azure account or create a free account.

- Fundamentals of Azure Data Engineering, Azure Data Factory, Azure Data Lake Gen 2.

- Knowledge of [ Medalllion lakehouse architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)


## Pipeline Overview

This [ guide](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guide.md) is a walkthrough for implementing a scalable Azure solution that automates the collection of raw location data from the two major bus operators (Arriva and Stagecoach) in Liverpool and store these in a query-able file format system using Azure Data Factory and Azure Data Lake Gen 2 (ADLSG2). The stored data would be used for later transformation and processing into usable data for analysis or prediction use cases.


![alt text](<images/BODS_to_ADLSG2_bronze.png>)

## Implementation Steps
- [ Understand the data and how to interact with the BODS API endpoint that is needed](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/bods-data-guide.md). 
- Data ingestion - Use an Azure data pipline to collect and store data to a data lake.

## Data Ingestion

For the data ingestion, I'll be using two resources Azure Data Factory (ADF) and Azure Data Lake Gen 2.

For data ingestion, I’ll be using two Azure resources: Azure Data Factory (ADF) and Azure Data Lake Storage Gen2 (ADLS Gen2).

I’ll be collecting live bus location data every 5 minutes and storing the raw files saved in ADLS Gen2. Azure Data Factory will handle the orchestration, automating the process of calling the BODS API and saving the responses into the correct location in the data lake.

In ADLS Gen2, I’ll store both a configuration file for the operators I need and the raw data, organized in a way that makes it easy to query later. Each raw data file will follow a consistent naming structure that includes the bus operator code and the timestamp of when the data was collected, making it simple to track and manage.

### Task requirements
- Schedule and automate API calls every 5 minutes
- Store responses into Azure Data Lake Gen2 container.
- File should be organised in a queryable stucture, using a file naming convention that includes the operator code and timestamp.
- Keep a config file in ADLSG2 to map API call for each bus operator.

