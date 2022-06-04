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


    @task(task_id='get_data')
    def get_data():
        coins = '%2C'.join(COIN_LIST)
        URL = f'https://api.coingecko.com/api/v3/simple/price?ids={coins}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true'
        r = requests.get(URL)
        return [r.json(), datetime.utcnow().isoformat()]

    @task(provide_context=True)
    def transform(coin, **kwargs):
        xcom = kwargs['ti'].xcom_pull(task_ids='get_data')
        data = xcom[0][coin]
        return {"price": data['usd'], "market_cap": data['usd_market_cap'], "volume_24h": data['usd_24h_vol'], "change_24h":data['usd_24h_change'], "ingested_timestamp": xcom[1]}
