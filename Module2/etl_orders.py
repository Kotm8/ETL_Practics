import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "etl.db"
DATA_PATH = BASE_DIR / "data" / "orders_source.csv"
PIPELINE_NAME = "orders_pipeline"

def get_conn():
    return sqlite3.connect(DB_PATH)

def create_tables():
    DDL = """
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_ts TEXT,
            status TEXT,
            amount REAL,
            updated_at TEXT,
            load_dttm TEXT
        );
        CREATE TABLE IF NOT EXISTS dwh_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_ts TEXT NOT NULL,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            updated_at TEXT NOT NULL,
            load_dttm TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS etl_state (
            pipeline_name TEXT PRIMARY KEY,
            last_success_ts TEXT NOT NULL
        );
        """
    
    with get_conn() as conn:
        conn.executescript(DDL)

def check_table():
    with get_conn() as conn:
        null_critical = conn.execute("""
                SELECT COUNT(*) AS null_critical
                FROM dwh_orders
                WHERE order_id IS NULL OR updated_at IS NULL OR amount IS NULL;
            """).fetchone()[0]

        duplicate_orders = conn.execute("""
                SELECT order_id, COUNT(*) AS cnt
                FROM dwh_orders
                GROUP BY order_id
                HAVING COUNT(*) > 1;
            """).fetchall()

        non_positive_amount = conn.execute("""
                SELECT COUNT(*) AS non_positive_amount
                FROM dwh_orders
                WHERE amount <= 0;
            """).fetchone()[0]

        print("QUALITY CHECKS")
        print(f"null_critical: {null_critical}")
        print(f"duplicate_orders: {duplicate_orders}")
        print(f"non_positive_amount: {non_positive_amount}")

        if null_critical == 0 and len(duplicate_orders) == 0 and non_positive_amount == 0:
            print("All checks passed")
        else:
            print("Some checks failed")


def extract_from_csv(data_url) -> pd.DataFrame:
    data = pd.read_csv(data_url)

    data["amount"] = pd.to_numeric(data["amount"], errors="coerce")
    data["order_ts"] = pd.to_datetime(data["order_ts"], errors="coerce")
    data["updated_at"] = pd.to_datetime(data["updated_at"], errors="coerce")

    data = data.dropna(subset=["order_id", "updated_at", "amount"])

    data["load_dttm"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return data

def make_view():
    with get_conn() as conn:
        conn.execute("""
                    CREATE VIEW IF NOT EXISTS mart_orders_daily AS
                    SELECT date(order_ts) AS order_day,
                    COUNT(*) AS orders_cnt,
                    ROUND(SUM(amount), 2) AS total_revenue
                    FROM dwh_orders
                    WHERE status IN ('paid', 'new', 'shipped')
                    GROUP BY date(order_ts)
                    ORDER BY order_day;
                    """)
    
def show_view():
    with get_conn() as conn:
        df = pd.read_sql("SELECT * FROM mart_orders_daily;", conn)
        print("\nMART ORDERS DAILY")
        print(df)

def run_full_load(data: pd.DataFrame):
    with get_conn() as conn:

        conn.execute("DELETE FROM stg_orders;")

        data.to_sql("stg_orders", conn, if_exists="append", index=False)

        conn.execute("DELETE FROM dwh_orders;")

        conn.execute("""
            INSERT INTO dwh_orders (
                order_id, customer_id, order_ts, status, amount, updated_at, load_dttm
            )
            SELECT
                order_id, customer_id, order_ts, status, amount, updated_at, load_dttm
            FROM stg_orders
                    """)

        max_updated_at = data["updated_at"].max()

        conn.execute("""
            INSERT INTO etl_state(pipeline_name, last_success_ts)
            VALUES (?, ?)
            ON CONFLICT(pipeline_name) DO UPDATE
            SET last_success_ts = excluded.last_success_ts
            """, (PIPELINE_NAME, str(max_updated_at)))
        conn.commit()


def run_incremental_load(data: pd.DataFrame):
    with get_conn() as conn:

        row = conn.execute("""
            SELECT last_success_ts
            FROM etl_state
            WHERE pipeline_name = ?
            """, (PIPELINE_NAME,)).fetchone()

        last_success_ts = pd.to_datetime(row[0]) if row else None

        if last_success_ts is not None:
            filtered_df = data[data["updated_at"] > last_success_ts].copy()
        else:
            filtered_df = data.copy()

        if filtered_df.empty:
            print("No new rows for incremental load")
            return

        conn.execute("DELETE FROM stg_orders;")

        filtered_df.to_sql("stg_orders", conn, if_exists="append", index=False)

        conn.execute("""
                    INSERT INTO dwh_orders (
                        order_id, customer_id, order_ts, status, amount, updated_at, load_dttm
                    )
                    SELECT
                        order_id, customer_id, order_ts, status, amount, updated_at, load_dttm
                    FROM stg_orders
                    WHERE true
                    ON CONFLICT(order_id) DO UPDATE SET
                        customer_id = excluded.customer_id,
                        order_ts = excluded.order_ts,
                        status = excluded.status,
                        amount = excluded.amount,
                        updated_at = excluded.updated_at,
                        load_dttm = excluded.load_dttm;
                    """)

        max_updated_at = filtered_df["updated_at"].max()

        conn.execute("""
            INSERT INTO etl_state(pipeline_name, last_success_ts)
            VALUES (?, ?)
            ON CONFLICT(pipeline_name) DO UPDATE
            SET last_success_ts = excluded.last_success_ts
            """, (PIPELINE_NAME, str(max_updated_at)))
        conn.commit()
    
if __name__ == "__main__":
    create_tables()
    data = extract_from_csv(DATA_PATH)
    #run_full_load(data)
    run_incremental_load(data)

    check_table()
    make_view()
    show_view()
