import json
from io import BytesIO

import requests
from minio import Minio

bucket_name = "stock-data"


def get_minio_client():
    """Return a MinIO client instance."""
    return Minio(
        endpoint="minio:9000",
        secure=False,
        access_key="minioadmin",
        secret_key="minioadmin",
    )


def _get_stock_prices(url, symbol):
    """Fetch stock data and return the first chart result as a JSON string."""
    print("Getting stock price data...")
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    resp = requests.get(url, headers=headers)
    return json.dumps(resp.json()["chart"]["result"][0])


def _process_stock_data(stock_prices: str):
    """Store stock_prices JSON in MinIO and return object path."""
    print("Processing stock price data...")
    client = get_minio_client()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Parse ONCE
    stock_dict = json.loads(stock_prices)
    symbol = stock_dict["meta"]["symbol"]
    print(f"Stock symbol: {symbol}")

    object_name = f"{symbol}/_stock_data.json"

    # Write raw JSON bytes (NO double dumps)
    data = stock_prices.encode("utf-8")

    print(f"Uploading to MinIO as object '{object_name}'...")
    obj = client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json"
    )

    print(f"{obj.bucket_name}/{obj.object_name} stock data stored successfully.")
    return f"{obj.bucket_name}/{symbol}"
