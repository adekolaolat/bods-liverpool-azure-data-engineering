# Data Ingestion

For data ingestion, I’ll be using two resources - Azure Data Factory (ADF) and Azure Data Lake Storage Gen2 (ADLS Gen2).

I’ll like to collect the live bus location data every 5 minutes and storing the raw files saved in ADLS Gen2. Azure Data Factory will handle the orchestration, automating the process of calling the BODS API and saving the responses into the correct location in the data lake.

In ADLS Gen2, I’ll store a configuration file for the operators I need to make my API calls dynamic and also store the raw data, organized in a way that makes it easy to query later. Each raw data file will follow a consistent naming structure that includes the bus operator code and the timestamp of when the data was collected, making it simple to track and manage.

## Task requirements
- Schedule and automate API calls every 5 minutes
- Store responses into Azure Data Lake Gen2 container.
- File should be organised in a queryable stucture, using a file naming convention that includes the operator code and timestamp.
- Keep a config file in ADLSG2 to map API call for each bus operator.

## Setting up Azure Resources

Firstly, set up the storage - Azure Data Lake Storage.

### 1. Set up Azure Data Lake Gen 2
Open Azure portal and :

**Create Storage**

- Create a new resource and choose "**Storage account**"
- Configure :
  - Azure subscription ( A free accouunt would most likely have the name **"Azure Subscription 1"**)
  - Resource group as `livbusbods` (this is a new resource group which will hold all our resources including ADF resource and future resources we would like to use in the project)
  - Storage name  as `livbusdatastore` 
  - Region as UK West  (Use your prefered region)
  - Performance: Standard
  - Redundancy: LRS

- Under "**Advanced**", enable **Hierarchical namespace**. This is required for ADLSG2 storage.
- Click "Review + create" to complete the storage set up

 **Create Containers**

  - Open the created storage (`livbusdatastore`)
  - Navigate to "Data Storage" and select "Containers" 
  - Create two containers:
    -  `bronze` (access level set to Private) - this will hold our raw data.
    - `config` - this will hold the config file used in making API call for each bus operators 


### 2. Set up Azure Data Factory

- Go to **"Create resource"** → **"Integration"** → **"Data Factory"** or search Data Factory
- Configure:
  - Resource group : choose `livbusbods`, same group as ADLG2
  - Name: `livbusbods-adf`
  - Version: `V2`
  - Region: `UK West`

Click "Review + create" → "Create" to complete the set up for the Azure Data Factory.

### **One more thing !** - Create the config file

I've set up a simple config file to keep things organized. This file stores the bus operator codes and their corresponding data feed IDs, which are essential for making the API calls and saving the data in the correct path for each operator.

I used a JSON format so the setup can be  clean and easy to manage. It will also make the pipeline dynamic and scalable. If I want to add more bus operators in the future, all I need to do is update this one file.

Create an [ operators.json](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/notebooks/operators.json) file and add this code to it:

```
[
    { "operator": "AMSY", "apiEndpoint": "datafeed/708" },
    { "operator": "SCCU", "apiEndpoint": "datafeed/1695" }
]

```
*where AMSY is for Arriva Merseyside and SCCU is for Stagecoach NW*

Add the json file by uploading to the `config` container.

- Navigate to  `livbusbods` → `livbusdatastore`→  Containers →  `config` and upload the file.

Now, we have ADLSG2 all set up for our pipeline.

## Building the pipeline in ADF

I want my pipeline to first **look up** my `operator.json` file in my `config` container in ADLSG2 and **for each** operator it finds in this file, it should use this information to make an HTTP GET Request to the BODS enpoint to retrieve the raw data. This raw data would be moved (**copied**) to the `bronze` container in the ADLSG2.

How do we build this process/pipeline?

We can split up this process into several activities:
 
    First Activity is to look up the the config container for the operator.json file.
 
    Second Activity goes through each content over the config file

        and move data copied from the BODS data source via HTTPS to ADLSG2 container per operator. - Third Activity

So, for the pipeline I'll need 3 Activities:

1. **Look up Activity**
2. **ForEach Activity**
    
    3. **Copy Activity** 


### Implementing pipeline in ADF

Launch ADF studio by navigating to `livbusbods-adf` resource

Go to **Author** → Factory Resources 

and Create a new pipeline and name it `pullbodstobronze`

#### 1. Add Lookup activity

Under Activities search for Lookup. 

Drag and Drop it into the pipeline pane.

Set name of Lookup activity to `LookupBusOperators`

Go to Settings tab
Add a source dataset, which will reference the config file 
 - Create new Source dataset

- **Create Linked Services Dataset for the Bus Operators using the operators.json file**

  - Search for "Azure Data Lake" and select ADLSG2
  - Select JSON as format
  - Set Name to `BusOperators`
  - Under Linked Service, Create a new Linked service
  - Set the name to `BusOperators`- not necessarily the same as Dataset name, but I like it. Add description
  - Select AutoResolveIntegrationRuntime - I want Azure to mange this.
  - Select Account key as Authentication 
  - Choose your Azure Subcription (`Azure Subscription 1`)
  - Select Storage account - `livbusdatastore` - where our operator.json file lies 
  - Click on Create.
  - Under **Connection** tab, select Linked service as `BusOperators`, Choose the file path to the `operators.json` file

    ![alt text](/images/bus-operator-dataset.png)

  - Test the connection - the output should be the content of the  file.

  Go back to `LookupBusOperators` Activity in the pipeline
  
  - Under **Settings**, select `BusOperator` as **Source dataset**
  - Uncheck **First Row only**

This Activity is now set up to point to the config file     `operator.json` file

#### **2. Add ForEach activity**

- Drag and drop the For Each activity

- Under Activities search ForEach. 

- Drag and Drop it into the pipeline pane.

- Set name of activity to `ForEachBusOperator`

- Connect `LookupBusOperators` Activity to `ForEachBusOperator` using `on success` arrow.

- Go to Settings tab

  - Add this code to Item field  as dynamic content  `@activity('LookupBusOperators').output.value`

  - This is a pipeline expression and which gets the value from the `LookupBusOperator` i.e. the data in the config file.

ForEach activity is now almost set up. 

Next step is to add the Copy Activity to the `ForEachBusOperator` Activity.
 

#### **3. Add Copy activity**

The copy Activity reads data from from a **source** data and writes to a **sink/destination** data store.

In this case, my source would be the BODS API endpoint and my sink is ADLSG2.

I still have a requirement to write my data to the bronze container in a particular format following a timestamp format.

But first, let's configure the source dataset in the Copy activity, what do I need to consider?

I'll be leveraging pipeline/dataset parameterization to make my API calls dynamic, first let create  dataset for my source.

#### Create source dataset for Copy activity

My source dataset would reference the BODS API endpoint, so I need to to grab the data feed ID for each bus operator. I'll pass the data feed information, stored as `apiEndpoint` in the config file as a parameter to this dataset.

Got to Factory Resources

- Select the Copy Activity in `ForEachBusOperator`
-  Set Name to `CopyBODSRawData`
- Go to **Source** tab, and Create new Source dataset
 - Search for "**HTTP**" and select **HTTP**
  - Select "**XML**" as format
  - Set Name to `httplinkedservicebodsnorthwest`
  - Under Linked Service, Create a new Linked service
  - Set the name to `httplinkedservicebodsnorthwest`- not necessarily the same as Dataset name, but I like it. Add a description
  - Select AutoResolveIntegrationRuntime - I want Azure to manage this.
  - Select "**Base URL**" as `https://data.bus-data.dft.gov.uk/api/v1/`
  - Set "**Authentication type**" to "**Anonymous**"
  - Enable "**Server certificates validation**"
  - Test connection
  - Click on Create.
  
  Go to `httplinkedservicebodsnorthwest` dataset
  - Click on Create.
  - Select Storage account - `livbusdatastore` - where our operator.json file lies 
  - Click on Create.
  - Under **Connection** tab, select Linked service as `BusOperators`, Choose the file path to the `operators.json` file

- 

Click on the `ForEachBusOperator` activity and add a new Copy Activity to it.

Take to the da


Add the data to the Operator in the  data 







  

[ Go to Implementation Steps](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/README.md#implementation-steps)