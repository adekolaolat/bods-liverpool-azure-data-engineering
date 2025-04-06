# Data Ingestion

For data ingestion, I’ll be using two resources - Azure Data Factory (ADF) and Azure Data Lake Storage Gen2 (ADLS Gen2).

I’ll like to collect the live bus location data every 5 minutes and storing the raw files saved in ADLS Gen2. Azure Data Factory will handle the orchestration, automating the process of calling the BODS API and saving the responses into the correct location in the data lake.

In ADLS Gen2, I’ll store a configuration file for the operators I need to make my API calls dynamic and also store the raw data, organized in a way that makes it easy to query later. Each raw data file will follow a consistent naming structure that includes the bus operator code and the timestamp of when the data was collected, making it simple to track and manage.

## Task requirements
- Schedule and automate API calls every 5 minutes
- Store responses into Azure Data Lake Gen2 container.
- File should be organised in a queryable stucture, using a file naming convention that includes the operator code and timestamp.
- Keep a config file in ADLSG2 to map API call for each bus operator.
