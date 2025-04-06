
# Azure pipeline to collect public bus location data. 

## Overview

This project focuses on building a scalable Azure data pipeline solution to collect public bus location in Liverpool via the **Bus Open Data Service (BODS)** API.


## About BODS

BODS has open data published by the operators of local bus and coach services across England which are timetable, location, fares and disruption data and this has been possible through The Bus Services Act 2017 that requires local bus operators in England to provide specific data between 2020 and 2021. 

BODS offers free, license-free access to this information and allows users to give feedback to data publishers.


## Why store this data?

Currently, the Bus Open Data Service (BODS) only provides live location data, meaning there is no historical record to analyze trends, identify recurring issues, or build predictive models. By collecting and storing historical bus tracking data, we can analyze operational trends, predict potential delays, and improve service planning.


### Business Use Cases

- Transport Authorities & City Planning need historical data used to make decisons to improve route planning, predict delays, and enhance public transport efficiency.

- Bus Operators can leverage insights from analysis of the historical data to benchmark performance and make strategic decisions to gain a competitive edge or break monopoly in certain regions.

- Help public transport users save money on tickets for routes with multiple operators, reduce travel time, and improve their commuting experience.


## Requirements

- [ BODS gov.uk ](https://data.bus-data.dft.gov.uk/) account to get access to API documentations and API key.

- Azure account or create a free account.

- Fundamentals of Azure Data Engineering, **Azure Data Factory**, **Azure Data Lake Gen 2**.

- Knowledge of [ Medalllion lakehouse architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)


## Pipeline Overview

This [ guide](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/README.md#implementation-steps) is a walkthrough for implementing a scalable Azure solution that automates the collection of raw location data from the two major bus operators (Arriva and Stagecoach) in Liverpool and store these in a query-able file format system using Azure Data Factory and Azure Data Lake Gen 2 (ADLSG2). The stored data would be used for later transformation and processing into usable data for analysis or prediction use cases.


![alt text](<images/BODS_to_ADLSG2_bronze.png>)

## Implementation Steps
- [ Understand the data and how to interact with the BODS API endpoint that is needed](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/bods-data-guide.md). 
- [ Data ingestion - Use an Azure data pipline to collect and store data to a data lake.](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-ingestion.md)

