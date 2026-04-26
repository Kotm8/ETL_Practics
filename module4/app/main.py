from app.core.minio_client import get_minio_client
from app.services.extract import extract_from_csv
from app.services.marts import generate_mart_daily, get_report
from app.services.quality import run_quality_checks
from app.services.quarantine import quarantine_invalid_rows
from app.services.staging import (
    staging_orders_clean,
    staging_orders_dedup,
    staging_orders_raw,
)


LOAD_DATE = "2026-04-13"
RAW_BUCKET = "etl-training-raw"


def run_pipeline(load_date: str = LOAD_DATE):
    client = get_minio_client()

    response = client.get_object(
        RAW_BUCKET,
        f"orders/load_date={load_date}/orders_{load_date}.csv",
    )

    try:
        df = extract_from_csv(response)
    finally:
        response.close()
        response.release_conn()

    staging_orders_raw(df)
    quarantine_invalid_rows(load_date)

    staging_orders_clean()
#full
    staging_orders_dedup()
    generate_mart_daily()
#increm
    #merge_orders_dedup()
    #rebuild_mart_daily_last_7_days()
    
    run_quality_checks()
    print(get_report())


if __name__ == "__main__":
    run_pipeline()
