import json
import os
from pathlib import Path
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen

import pandas as pd
from minio import Minio


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "orders")
ORDERS_API_URL = os.getenv("ORDERS_API_URL", "http://orders-api:8000")


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )



def extract(csv_path: Path | str) -> pd.DataFrame:
    return pd.read_csv(Path(csv_path))


def extract_orders_from_minio() -> pd.DataFrame:
    now = datetime.now()
    date = now.date()

    folder = f"etl-raw/orders/load_date={date}"
    file_name = f"orders_{date}.csv"
    object_name = f"{folder}/{file_name}"

    client = get_minio_client()
    response = client.get_object(MINIO_BUCKET_NAME, object_name)

    try:
        return pd.read_csv(BytesIO(response.read()))
    finally:
        response.close()
        response.release_conn()

def extract_users_from_api() -> pd.DataFrame:
    api_base_url = ORDERS_API_URL
    users_url = f"{api_base_url.rstrip('/')}/api/v1/users/"

    with urlopen(users_url, timeout=30) as response:
        users = json.loads(response.read().decode("utf-8"))

    return pd.json_normalize(users)
