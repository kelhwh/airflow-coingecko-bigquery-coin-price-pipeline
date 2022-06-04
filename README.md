# airflow-coingecko-bigquery-coin-price-pipeline
## Introduction
This DAG defines tasks to ingest price data for btc, usdc, usdt and busd all at once from the Coingecko public API every 10 minutes. Then, prices of each coin are splitted and loaded into google bigquery tables respectively. It's set to Sequetial executor by default, make sure to change it to other executors for production.

## Initialization
### Install
```
pip install apache-airflow[google]==2.3.1
```

### Set up Airflow
- Move to project root directory
- Set environmental variable AIRFLOW_HOME by
```
export AIRFLOW_HOME=$(pwd)
```
- Initialize airflow db
```
airflow db init
```

### Customize your deployment
- Set variables to your GCP project name and Bigquery dataset in `variables/prod.json`.
- Initialize webserver
```
airflow webserver
```
- Go to `localhost:8080` in your browser and load your variables at `Admin > Variables` (screehshot below)
![image](https://user-images.githubusercontent.com/43116359/171997507-0fbfd75d-4605-465d-ac28-f7415b76b0cd.png)
- Get your bigquery service account key for the target GCP project.
- Paste your json file content in `Admin > Connections > google_cloud_default > Keyfile JSON` (screehshot below)
![image2](https://user-images.githubusercontent.com/43116359/171997871-67803e48-ddef-4d22-83d9-d3d0e6160086.png)
