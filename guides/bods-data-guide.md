# Getting Started with BODS

One of the first things to do when working with data is to get familiar with the data source and understand how to interact with it. So, before jumping into building the pipeline, it’s important to explore how the BODS API works and what kind of data it returns.

For this project, I’ll be focusing on buses in Liverpool and surrounding areas within the Merseyside region.

The two main bus operators in Liverpool are **Arriva** and **Stagecoach**. So, how do we get data for their buses?

We can do that by making an API call! 

## Talking to BODS

To make an API call to BODS, you’ll need two things:

- An API key

- Feed IDs for the bus operators in your region of interest

BODS provides a unique data feed for each bus operator, depending on where they operate. To find the data feed ID for a specific operator, you can use the filter tool on the BODS website [ here](https://data.bus-data.dft.gov.uk/avl/?status=live).

Getting an API key is easy. [ Sign up](https://data.bus-data.dft.gov.uk/) for an account on BODS, and you’ll be able to generate one and have access to the API docs.

For Liverpool buses, this would be the data feed  information :

| Operator/Publisher    | Data feed ID| Region|
| -------- | ------- |------- |
| Arriva UK Bus | 708    |Arriva Merseyside   |
| Stagecoach | 1695    | Stagecoach NW   |

Once you're set up, the API gives you access to real-time bus location data, **updated every 10 seconds**.

### Make API call

To see what the API response looks like, you can try making a HTTP GET request right from your terminal. Just use the`curl`command along with the URL below to make the call and view the response.

Use your API key!

For Arriva

```
curl https://data.bus-data.dft.gov.uk/api/v1/datafeed/708/?api_key=your_api_key
```

For Stagecoach

```
curl https://data.bus-data.dft.gov.uk/api/v1/datafeed/1695/?api_key=your_api_key
```
### Response

The response from the BODS API comes in XML format, following the SIRI-VM (Vehicle Monitoring) standard. You can check out this [ sample responses (xml files) ](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/tree/main/sample-raw-data)  for both Arriva and Stagecoach.

Understanding how to make the API call, the structure of the URL and what the response looks like is a key part of building the pipeline and transformations that would be done.



[ Go to Implementation Steps](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/README.md#implementation-steps)

[ ⏭️ Data Ingestion](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-ingestion.md)

