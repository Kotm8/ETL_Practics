from datetime import datetime

import pandas as pd

from app.core import config
from app.db.db import get_conn


class stagingService:
    @staticmethod
    def staging_orders_raw(data: pd.DataFrame):
        DDL = """
            CREATE TABLE IF NOT EXISTS staging_orders_raw (
                order_id INTEGER,
                customer_id INTEGER,
                order_ts TEXT,
                status TEXT,
                amount REAL,
                region TEXT,
                updated_at TEXT,
                load_ts TEXT,
                batch_id TEXT
            );
            """

        with get_conn() as conn:
            conn.executescript(DDL)

            data = data.copy()
            data["load_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data["batch_id"] = str(config.BATCH_ID)

            data = data[
                [
                    "order_id",
                    "customer_id",
                    "order_ts",
                    "status",
                    "amount",
                    "region",
                    "updated_at",
                    "load_ts",
                    "batch_id",
                ]
            ]

            data.to_sql("staging_orders_raw", conn, if_exists="append", index=False)

    @staticmethod
    def staging_orders_clean():
        DDL = """
            DROP TABLE IF EXISTS staging_orders_clean;

            CREATE TABLE staging_orders_clean AS
            SELECT
                order_id,
                customer_id,
                order_ts,
                status,
                CAST(amount AS REAL) AS amount,
                LOWER(region) AS region,
                updated_at,
                load_ts,
                batch_id
            FROM staging_orders_raw
            WHERE order_id IS NOT NULL
              AND updated_at IS NOT NULL
              AND amount > 0;
            """

        with get_conn() as conn:
            conn.executescript(DDL)

    @staticmethod
    def staging_orders_dedup():
        DDL = """
            DROP TABLE IF EXISTS staging_orders_dedup;

            CREATE TABLE staging_orders_dedup (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_ts TEXT,
                status TEXT,
                amount REAL,
                region TEXT,
                updated_at TEXT,
                load_ts TEXT,
                batch_id TEXT
            );

            INSERT INTO staging_orders_dedup (
                order_id,
                customer_id,
                order_ts,
                status,
                amount,
                region,
                updated_at,
                load_ts,
                batch_id
            )
            SELECT
                order_id,
                customer_id,
                order_ts,
                status,
                amount,
                region,
                updated_at,
                load_ts,
                batch_id
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY updated_at DESC, load_ts DESC
                    ) AS rn
                FROM staging_orders_clean
            )
            WHERE rn = 1;
            """

        with get_conn() as conn:
            conn.executescript(DDL)

    @staticmethod
    def merge_orders_dedup():
        sql = """
        INSERT INTO staging_orders_dedup (
            order_id,
            customer_id,
            order_ts,
            status,
            amount,
            region,
            updated_at,
            load_ts,
            batch_id
        )
        SELECT
            order_id,
            customer_id,
            order_ts,
            status,
            amount,
            region,
            updated_at,
            load_ts,
            batch_id
        FROM staging_orders_clean
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id = excluded.customer_id,
            order_ts = excluded.order_ts,
            status = excluded.status,
            amount = excluded.amount,
            region = excluded.region,
            updated_at = excluded.updated_at,
            load_ts = excluded.load_ts,
            batch_id = excluded.batch_id
        WHERE excluded.updated_at > staging_orders_dedup.updated_at;
        """

        with get_conn() as conn:
            conn.executescript(sql)
