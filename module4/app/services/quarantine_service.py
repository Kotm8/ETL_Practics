import os

import pandas as pd

from app.core.minio_client import get_minio_client
from app.db.db import get_conn


class quarantineService:
    @staticmethod
    def quarantine_invalid_rows(load_date: str):
        query = """
            SELECT *
            FROM staging_orders_raw
            WHERE order_id IS NULL
               OR updated_at IS NULL
               OR amount <= 0
               OR amount IS NULL;
        """

        with get_conn() as conn:
            bad_rows = pd.read_sql(query, conn)

        if bad_rows.empty:
            print("No rows to quarantine")
            return

        local_path = "rejected.csv"
        bad_rows.to_csv(local_path, index=False)

        client = get_minio_client()

        client.fput_object(
            "etl-training-quarantine",
            f"orders/load_date={load_date}/rejected.csv",
            local_path,
        )

        os.remove(local_path)

        print(f"Quarantined {len(bad_rows)} bad rows")
