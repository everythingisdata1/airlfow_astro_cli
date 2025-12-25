from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO


def _get_stock_prices(url, symbol):
    import json

    import requests
    print("Getting stock price data...")
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"  # Important to avoid 403
    }
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    resp = requests.get(url, headers=headers)
    return json.dumps(resp.json()['chart']['result'][0])


def _process_stock_data(data):
    print("Processing stock price data...")
    miniio_conn = Minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin", )
    stock_data = json.loads(data)
    # Example processing: Extract closing prices
    timestamps = stock_data['timestamp']
    indicators = stock_data['indicators']['quote'][0]
    closing_prices = indicators['close']
    processed_data = list(zip(timestamps, closing_prices))
    return processed_data
