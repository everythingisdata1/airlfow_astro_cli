from datetime import datetime

import requests
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task, PokeReturnValue

from include.stock_market.tasks import _get_stock_prices, _process_stock_data

SYMBOL = "NVDA"


@dag(schedule="@daily",
     start_date=datetime(2024, 1, 1),
     catchup=False,
     tags=["finance_stocks"])
def stock_market():
    @task.sensor(
        poke_interval=30,
        timeout=300,
        mode="poke"
    )
    def is_api_available() -> PokeReturnValue:
        print("Checking if stock market API is available...")
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"  # Important to avoid 403
        }

        url = "https://query1.finance.yahoo.com/v8/finance/chart/"
        resp = requests.get(url=url, headers=headers)
        print("Body:", resp.json())
        condition = resp.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_price = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={"symbol": SYMBOL, 'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}'}
    )

    store_prices = PythonOperator(
        task_id="store_stock_prices",
        python_callable=_process_stock_data,
        op_kwargs={"stock_prices": '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    spark_formate_prices = DockerOperator(
        task_id="spark_format_stock_prices",
        image="airflow/spark-app",
        container_name="spark_format_stock_prices",
        api_version="auto",
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": '{{ti.xcom_pull(task_ids="store_stock_prices")}}'
        },
        auto_remove="success",
    )

    is_api_available() >> get_stock_price >> store_prices >> spark_formate_prices


stock_market()
