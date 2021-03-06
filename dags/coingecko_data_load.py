"""
# Coingecko cryptocurrencies price pipeline
This DAG defines tasks to ingest price data all at once from the Coingecko API.
Then, prices of each coin are splitted and loaded into google bigquery tables respectively.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

from airflow.utils.task_group import TaskGroup

from datetime import datetime
import requests


PROJECT = Variable.get('project')
DATASET = Variable.get('dataset')
COIN_LIST = ["bitcoin", "tether", "usd-coin", "binance-usd", "dai"]

with DAG(
    'coingecko_data_load',
    schedule_interval = '*/10 * * * *',
    catchup = False,
    start_date=datetime(2022, 6, 1)
) as dag:
    dag.doc_md = __doc__

    @task(task_id='get_data', doc="Send the request to Coingecko API and get the price data of all required cryptocurrencies at once.")
    def get_data():
        coins = '%2C'.join(COIN_LIST)
        URL = f'https://api.coingecko.com/api/v3/simple/price?ids={coins}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true'
        r = requests.get(URL)
        return [r.json(), datetime.utcnow().isoformat()]



    @task(provide_context=True, doc="Transform the response json of each coin to the target schema.")
    def transform(coin, **kwargs):
        xcom = kwargs['ti'].xcom_pull(task_ids='get_data')
        data = xcom[0][coin]
        return {"price": data['usd'], "market_cap": data['usd_market_cap'], "volume_24h": data['usd_24h_vol'], "change_24h":data['usd_24h_change'], "ingested_timestamp": xcom[1]}

    @task(doc="Insert the coin price into the target table in Bigquery.")
    def load_data(coin, data):
        table_id = f'{PROJECT}.{DATASET}.{coin}'
        client = BigQueryHook().get_client()
        errors = client.insert_rows_json(table_id, [data])
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    with TaskGroup(group_id='group1') as tg1:
        for coin in COIN_LIST:
            with TaskGroup(group_id=coin):
                create_table = BigQueryCreateEmptyTableOperator(
                    task_id = f"create_{coin}_table",
                    project_id = PROJECT,
                    dataset_id = DATASET,
                    table_id = coin,
                    schema_fields = [
                        {"name": "price", "type": "FLOAT", "mode": "REQUIRED"},
                        {"name": "market_cap", "type": "FLOAT", "mode": "REQUIRED"},
                        {"name": "volume_24h", "type": "FLOAT", "mode": "REQUIRED"},
                        {"name": "change_24h", "type": "FLOAT", "mode": "REQUIRED"},
                        {"name": "ingested_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
                    ]
                )
                create_table.doc = "Create table for each coin, if not exist."
                create_table >> load_data(coin, transform(coin))


get_data() >> tg1
