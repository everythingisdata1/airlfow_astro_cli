import json

import requests
from minio import Minio


def _get_stock_prices(url, symbol):
    print("Getting stock price data...")
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"  # Important to avoid 403
    }
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    resp = requests.get(url, headers=headers)
    return json.dumps(resp.json()['chart']['result'][0])


def _process_stock_data(stock_prices):
    print("Processing stock price data...")
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    bucket_name = "stock-data"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    print(f"Storing stock price data in bucket '{bucket_name}'...")
    print(f"Data: {stock_prices}")
