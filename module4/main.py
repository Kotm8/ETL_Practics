from minio import Minio
from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "etl.db"

BATCH_ID = 1
load_dotenv()

def get_conn():
    return sqlite3.connect(DB_PATH)

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
        data["batch_id"] = str(BATCH_ID)
        
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

def generate_mart_daily():
    DDL = """
        DROP TABLE IF EXISTS mart_orders_daily;

        CREATE TABLE mart_orders_daily AS
        SELECT
            DATE(order_ts) AS order_day,
            region,
            COUNT(DISTINCT order_id) AS orders_cnt,
            SUM(CASE WHEN status IN ('paid', 'shipped') THEN amount ELSE 0 END) AS revenue,
            AVG(CASE WHEN status IN ('paid', 'shipped') THEN amount END) AS avg_check
        FROM staging_orders_dedup
        GROUP BY DATE(order_ts), region;
        """
    
    with get_conn() as conn:
        conn.executescript(DDL)

def get_report():
    DDL = """
        SELECT
            order_day,
            SUM(orders_cnt) AS total_orders,
            ROUND(SUM(revenue), 2) AS total_revenue,
            ROUND(AVG(avg_check), 2) AS avg_check_by_region
        FROM mart_orders_daily
        GROUP BY order_day
        ORDER BY order_day;
        """
    
    with get_conn() as conn:
        return pd.read_sql(DDL, conn)
    
def run_quality_checks():
    checks = {
        "duplicate_order_id": """
            SELECT order_id, COUNT(*) AS cnt
            FROM staging_orders_dedup
            GROUP BY order_id
            HAVING COUNT(*) > 1;
        """,

        "invalid_amount": """
            SELECT *
            FROM staging_orders_clean
            WHERE amount <= 0 OR amount IS NULL;
        """,

        "empty_order_day": """
            SELECT *
            FROM mart_orders_daily
            WHERE order_day IS NULL;
        """
    }

    failed = False

    with get_conn() as conn:
        for check_name, query in checks.items():
            result = pd.read_sql(query, conn)

            if not result.empty:
                failed = True
                print(f"\nQUALITY CHECK FAILED: {check_name}")
                print(result)
            else:
                print(f"QUALITY CHECK PASSED: {check_name}")

    if failed:
        raise Exception("Pipeline failed quality checks")

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

    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    client.fput_object(
        "etl-training-quarantine",
        f"orders/load_date={load_date}/rejected.csv",
        local_path
    )

    os.remove(local_path)

    print(f"Quarantined {len(bad_rows)} bad rows")

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
        
def rebuild_mart_daily_last_7_days():
    sql = """
    CREATE TABLE IF NOT EXISTS mart_orders_daily (
        order_day TEXT,
        region TEXT,
        orders_cnt INTEGER,
        revenue REAL,
        avg_check REAL
    );

    DELETE FROM mart_orders_daily
    WHERE order_day >= DATE('now', '-7 days');

    INSERT INTO mart_orders_daily (
        order_day,
        region,
        orders_cnt,
        revenue,
        avg_check
    )
    SELECT
        DATE(order_ts) AS order_day,
        region,
        COUNT(DISTINCT order_id) AS orders_cnt,
        SUM(CASE WHEN status IN ('paid', 'shipped') THEN amount ELSE 0 END) AS revenue,
        AVG(CASE WHEN status IN ('paid', 'shipped') THEN amount END) AS avg_check
    FROM staging_orders_dedup
    WHERE DATE(order_ts) >= DATE('now', '-7 days')
    GROUP BY DATE(order_ts), region;
    """

    with get_conn() as conn:
        conn.executescript(sql)

def extract_from_csv(data_url) -> pd.DataFrame:
    data = pd.read_csv(data_url)

    data["amount"] = pd.to_numeric(data["amount"], errors="coerce")
    data["order_ts"] = pd.to_datetime(data["order_ts"], errors="coerce")
    data["updated_at"] = pd.to_datetime(data["updated_at"], errors="coerce")

    return data

if __name__ == "__main__":
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    response = client.get_object(
    "etl-training-raw",
    "orders/load_date=2026-04-13/orders_2026-04-13.csv"
    )

    try:
        df = extract_from_csv(response)
    finally:
        response.close()
        response.release_conn()

    staging_orders_raw(df)


    quarantine_invalid_rows("2026-04-13")

    staging_orders_clean()
#full
    staging_orders_dedup()
    generate_mart_daily()
#increm
    #merge_orders_dedup()
    #rebuild_mart_daily_last_7_days()

    
    run_quality_checks()

    print(get_report())