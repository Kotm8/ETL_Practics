from app.core.minio_client import get_minio_client
from app.services.extract_service import extractService
from app.services.marts_service import martsService
from app.services.quality_service import qualityService
from app.services.quarantine_service import quarantineService
from app.services.staging_service import stagingService


LOAD_DATE = "2026-04-13"
RAW_BUCKET = "etl-training-raw"


def run_pipeline(load_date: str = LOAD_DATE):
    client = get_minio_client()

    response = client.get_object(
        RAW_BUCKET,
        f"orders/load_date={load_date}/orders_{load_date}.csv",
    )

    try:
        df = extractService.extract_from_csv(response)
    finally:
        response.close()
        response.release_conn()

    stagingService.staging_orders_raw(df)
    quarantineService.quarantine_invalid_rows(load_date)

    stagingService.staging_orders_clean()

    # full
    stagingService.staging_orders_dedup()
    martsService.generate_mart_daily()

    # incremental
    # stagingService.merge_orders_dedup()
    # martsService.rebuild_mart_daily_last_7_days()

    qualityService.run_quality_checks()
    print(martsService.get_report())


if __name__ == "__main__":
    run_pipeline()
