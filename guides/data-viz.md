# Integrating into BI 

I used Power BI Desktop to connect to my data and build a dashboard that looks at how buses are running in Liverpool on a daily basis. Here's a quick rundown of how I connected everything and what the dashboard shows.

Before connecting to Power BI, grab the Workspace endpoint from Synapse Studio.

To get it:
- Go to Manage
- Click on the **Built-in (Serverless) SQL pool**
- Copy the **Workspace SQL endpoint**


## Connect Synapse data source to Power BI Desktop.

- Launch Power BI Desktop.
- Click on Get Data. 
- Search and Select Azure Synapse Analystics SQL.
- Paste the Workspace SQL endpoint int the `Server` field
- Enter `livbusdb` or your database name  as the database
- Sign in using you Azure credentials- Azure Active Directory (AAD) 

- Choose `Import` as the **Data connectivity mode**

## Dashboard

I've built a dashboard that shows a daily report of how operators run the public buses for a particular day. The dashboard answers some questions on how operators run their buses and gives insight into traffic for the day.


The dashboard can be be published to the Power BI Service, where it can be set refresh the data every day to stay up to date.


![alt text](/images/Bus_Operation_Liverpool.jpg)


[ ⏮️ Data Warehousing](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-warehousing.md)

[ Go to Implementation Steps](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/README.md#implementation-steps)