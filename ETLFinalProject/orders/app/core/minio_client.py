from functools import lru_cache
import urllib3
from minio import Minio
from minio.error import S3Error
from urllib3.util.retry import Retry

from app.core import config


@lru_cache(maxsize=1)
def get_minio_client() -> Minio:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=None,
    )

    http_client = urllib3.PoolManager(
        retries=retry,
        timeout=urllib3.Timeout(connect=5.0, read=30.0),
    )

    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
        http_client=http_client,
    )


def ensure_minio_ready() -> None:
    client = get_minio_client()

    try:
        if not client.bucket_exists(config.MINIO_BUCKET_NAME):
            client.make_bucket(config.MINIO_BUCKET_NAME)
    except S3Error as exc:
        raise RuntimeError(f"MinIO is not ready: {exc}") from exc