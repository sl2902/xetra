# Xetra trading exchange - Stock price analysis
This project was created as part of finale for the DataTalksClub - Data Engineering Zoomcamp 4 week course. [Xetra](https://www.xetra.com/xetra-en/) is a tradding platform operated by the Frankfurt tock Exchange based in Frankfurt, Germany. It allows trading in all financial instruments including shares, ETFs and ETPs. It has a significant market share throught Europe

## Problem Description
The goal of the project is to build and end to end data pipeline that will store data, aggregated to one minute interval, containing stocks prices from 2022-01-03 to 2022-04-25; this will be stored on Google Cloud Storage. From here, it will be tranformed using dbt and the cleaned table will be stored in BigQuery; the final dataset is available to the end user to create reports and dashboards. The dataset, in its current form, is static in nature as it is no longer available [here](https://github.com/awslabs/open-data-registry/blob/main/datasets/deutsche-boerse-pds.yaml). So I have made it available on [Kaggle](https://www.kaggle.com/datasets/laxmsun/xetra-stocks)

## Technology Stack 
The following technologies have been used
- Google Cloud Storage (GCS) <br>
- Google Compute Engine <br>
- Google Cloud Run <br>
- Google BigQuery<br>
- Terraform<br>
- Prefect<br>
- dbt<br>
- Google Looker studio <br>
- GitHub Actions

## Data Pipeline Architecture
![](assets/Xetra_ETL_architecture.png)

## Data Dictionary - xetra_stocks
|Field name    |Type     |Description                               | 
|--------------|---------|------------------------------------------|
|ISIN          | STRING  |Unique security identifier                |
|Mnemonic      | STRING  |Abbreviated security ID                   |
|SecurityDesc  | STRING  |Security Description                      |
|SecurityType  | STRING  |Type of security                          |
|Currency      | STRING  |Currency                                  |
|SecurityID    | INTEGER |Security ID                               |
|Date          | DATETIME|Transaction date                          |
|Time          | TIME    |Transaction time                          |
|StartPrice    | FLOAT   |Opening price                             |
|MaxPrice      | FLOAT   |Maximum price                             |
|MinPrice      | FLOAT   |Minimum price                             |
|EndPrice      | FLOAT   |Closing price                             |
|TradedVolume  | INTEGER |Total volume of shares traded at that time|
|NumberOfTrades| INTEGER |Number of trades placed at that time      |
|key           | STRING   |Unique record identifer                  |

## Dashboard
![](assets/Xetra_shares_analysis_March_2022.png)

The analysis is for the month of March 2022
1) The top left card shows the total number of unique shares traded in that month
2) The chart at the top middle shows the correlation between average traded volume and average closing price
3) The chart at the top right corner shows the correlation between the average minimum price and average maximum price
4) The chart at the bottom displays the trend for the percent change in closing price

## Steps to reproduce the project
1. Clone the repo

```
git clone https://github.com/sl2902/xetra.git
```

2. Setup your Google Cloud environment
- Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)
- Configure Identity and Access Management (IAM) for the service account, giving it the following privileges: 
    - Viewer
    - Storage Admin 
    - Storage Object Admin 
    - BigQuery Admin
- Download the JSON credentials and save it, e.g. to `~/.gc/<credentials>`
- Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
- Let the [environment variable point to your GCP key](https://cloud.google.com/docs/authentication/application-default-credentials#GAC), authenticate it and refresh the session token
```bash
export GOOGLE_APPLICATION_CREDENTIALS=<path_to_your_credentials>.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login
```
