# Approach

Automate Data Collection

First step in this data ingestion or collection would be to have an idea of how to talk to BODS and specify which operator data that is of interest.

## How to talk to BODS?

Arriva and Stagecoach are major bus operators in Liverpool, so we would like to get thier bus locations. So how do we get data for the buses?

We make an API call - GET request To make this API call, we need two things from BODS.

1. API key
2. Data feed IDs for bus operators in the region of interest.

BODS datasource has unique datafeed for each bus operators in the region they operate. ID for an operator can be checked using the filter tool for a region of interest [ here](https://data.bus-data.dft.gov.uk/avl/?status=live).

For Liverpool buses, this would be the datafeed  info

| Operator/Publisher    | Data feed ID| Region|
| -------- | ------- |------- |
| Arriva UK Bus | 708    |Arriva Merseyside   |
| Stagecoach | 1695    | Stagecoach NW   |


### Make API call

To view how the response would look like, open your terminal and make a API call using this `curl` command.

For Arriva

```
curl https://data.bus-data.dft.gov.uk/api/v1/datafeed/708/?api_key=your_api_key
```

For Stagecoach

```
curl https://data.bus-data.dft.gov.uk/api/v1/datafeed/1695/?api_key=your_api_key
```
### Response

Response is in an XML format, which is in SIRI-VM. Find sample responses for both Arriva and Stagecoach here.

