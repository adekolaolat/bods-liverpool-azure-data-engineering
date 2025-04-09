# JSON for Data Ingestion

[⏮️ Set up Azure  Resources](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-ingestion.md#setting-up-azure-resources)

[⏮️ Outcome for Data Ingestion](https://github.com/adekolaolat/bods-liverpool-azure-data-engineering/blob/main/guides/data-ingestion.md#outcome)


<details><summary><b> Pipeline - pullbodstobronze </b></summary>

```
{
    "name": "pullbodstobronze",
    "properties": {
        "description": "Pulls data from BODS endpoint to bronze container",
        "activities": [
            {
                "name": "ForEachBusOperator",
                "description": "Iterates over bus operators and datafeed values to pull data. ",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "LookupBusOperators",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "items": {
                        "value": "@activity('LookupBusOperators').output.value",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "CopyBODSRawData",
                            "description": "Copy data and store in bronze for bus operator",
                            "type": "Copy",
                            "dependsOn": [],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "source": {
                                    "type": "XmlSource",
                                    "storeSettings": {
                                        "type": "HttpReadSettings",
                                        "requestMethod": "GET"
                                    },
                                    "formatSettings": {
                                        "type": "XmlReadSettings",
                                        "validationMode": "none",
                                        "namespaces": true
                                    }
                                },
                                "sink": {
                                    "type": "JsonSink",
                                    "storeSettings": {
                                        "type": "AzureBlobFSWriteSettings"
                                    },
                                    "formatSettings": {
                                        "type": "JsonWriteSettings"
                                    }
                                },
                                "enableStaging": false
                            },
                            "inputs": [
                                {
                                    "referenceName": "httplinkedservicebodsnorthwest",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "apiEndpoint": {
                                            "value": "@item().apiEndpoint",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": "ADLS2LinkedServiceBODS_bronze",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "operator": {
                                            "value": "@item().operator",
                                            "type": "Expression"
                                        },
                                        "triggerTime": {
                                            "value": "@convertFromUtc(utcNow(),'GMT Standard Time')",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": "LookupBusOperators",
                "type": "Lookup",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "JsonSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true,
                            "enablePartitionDiscovery": false
                        },
                        "formatSettings": {
                            "type": "JsonReadSettings"
                        }
                    },
                    "dataset": {
                        "referenceName": "BusOperators",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": false
                }
            }
        ],
        "annotations": [],
        "lastPublishTime": "2025-04-03T10:11:19Z"
    },
    "type": "Microsoft.DataFactory/factories/pipelines"
}
```

</details>


<details><summary><b> Dataset - BusOperators</b></summary>

```
{
    "name": "BusOperators",
    "properties": {
        "linkedServiceName": {
            "referenceName": "BusOperators",
            "type": "LinkedServiceReference"
        },
        "annotations": [],
        "type": "Json",
        "typeProperties": {
            "location": {
                "type": "AzureBlobFSLocation",
                "fileName": "operators.json",
                "fileSystem": "config"
            }
        },
        "schema": {
            "type": "object",
            "properties": {
                "operator": {
                    "type": "string"
                },
                "apiEndpoint": {
                    "type": "string"
                }
            }
        }
    },
    "type": "Microsoft.DataFactory/factories/datasets"
}
```

</details>


<details><summary><b> Dataset - ADLS2LinkedServiceBODS_bronze</b></summary>

```
{
    "name": "ADLS2LinkedServiceBODS_bronze",
    "properties": {
        "linkedServiceName": {
            "referenceName": "busdatatobronze_adls2",
            "type": "LinkedServiceReference"
        },
        "parameters": {
            "operator": {
                "type": "string"
            },
            "triggerTime": {
                "type": "string"
            }
        },
        "annotations": [],
        "type": "Json",
        "typeProperties": {
            "location": {
                "type": "AzureBlobFSLocation",
                "fileName": {
                    "value": "@concat(\n  dataset().operator, '_',\n  formatDateTime(dataset().triggerTime, 'yyyyMMdd_HHmmss'),\n  '.json'\n)\n",
                    "type": "Expression"
                },
                "folderPath": {
                    "value": "@concat(\n  'operator=', dataset().operator, \n  '/year=', formatDateTime(dataset().triggerTime, 'yyyy'),\n  '/month=', formatDateTime(dataset().triggerTime, 'MM'),\n  '/day=', formatDateTime(dataset().triggerTime, 'dd'), \n  '/'\n)\n",
                    "type": "Expression"
                },
                "fileSystem": "bronze"
            }
        },
        "schema": {}
    },
    "type": "Microsoft.DataFactory/factories/datasets"
}

```

</details>




<details><summary><b> Dataset - httplinkedservicebodsnorthwest </b></summary>

```
{
    "name": "httplinkedservicebodsnorthwest",
    "properties": {
        "description": "Pull raw data from BODS for AMSY and SCCU",
        "linkedServiceName": {
            "referenceName": "httplinkedservicebodsnorthwest",
            "type": "LinkedServiceReference"
        },
        "parameters": {
            "apiEndpoint": {
                "type": "string"
            }
        },
        "annotations": [],
        "type": "Xml",
        "typeProperties": {
            "location": {
                "type": "HttpServerLocation",
                "relativeUrl": {
                    "value": "@concat(dataset().apiEndpoint, '?api_key=d952afc16dbf43c0726102754408092217b11864')",
                    "type": "Expression"
                }
            }
        }
    },
    "type": "Microsoft.DataFactory/factories/datasets"
}

```

</details>








