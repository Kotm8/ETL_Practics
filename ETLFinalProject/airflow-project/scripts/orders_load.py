import sqlite3
from pathlib import Path

import pandas as pd

from config.clickhouse_client import get_clickhouse_client


ORDER_COLUMNS = {
    "order_id": "INTEGER",
    "customer_id": "TEXT",
    "order_ts": "TEXT",
    "status": "TEXT",
    "amount": "REAL",
    "delivery_quality": "INTEGER",
    "product_category": "INTEGER",
    "customer_satisfaction": "INTEGER",
    "customer_email": "TEXT",
    "customer_name": "TEXT",
    "customer_age": "INTEGER",
    "customer_created_at": "TEXT",
}


CLICKHOUSE_COLUMNS = list(ORDER_COLUMNS)


def _create_dwh_orders_table(cur: sqlite3.Cursor) -> None:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dwh_orders (
        order_id INTEGER PRIMARY KEY,
        customer_id TEXT NOT NULL,
        order_ts TEXT NOT NULL,
        status TEXT NOT NULL,
        amount REAL NOT NULL,
        delivery_quality INTEGER,
        product_category INTEGER,
        customer_satisfaction INTEGER,
        customer_email TEXT,
        customer_name TEXT,
        customer_age INTEGER,
        customer_created_at TEXT
    )
    """)


def _create_dwh_orders(cur: sqlite3.Cursor) -> None:
    _create_dwh_orders_table(cur)
    _add_missing_columns(cur)
    _migrate_customer_id_to_text(cur)


def _add_missing_columns(cur: sqlite3.Cursor) -> None:
    cur.execute("PRAGMA table_info(dwh_orders)")
    existing_columns = {row[1] for row in cur.fetchall()}

    for column_name, column_type in ORDER_COLUMNS.items():
        if column_name not in existing_columns:
            cur.execute(f"ALTER TABLE dwh_orders ADD COLUMN {column_name} {column_type}")


def _migrate_customer_id_to_text(cur: sqlite3.Cursor) -> None:
    cur.execute("PRAGMA table_info(dwh_orders)")
    table_info = cur.fetchall()
    column_types = {row[1]: row[2].upper() for row in table_info}

    if column_types.get("customer_id") == "TEXT":
        return

    existing_columns = [row[1] for row in table_info]
    copy_columns = [column for column in ORDER_COLUMNS if column in existing_columns]
    copy_column_sql = ", ".join(copy_columns)

    cur.execute("ALTER TABLE dwh_orders RENAME TO dwh_orders_old")
    _create_dwh_orders_table(cur)
    cur.execute(
        f"""
        INSERT INTO dwh_orders ({copy_column_sql})
        SELECT {copy_column_sql}
        FROM dwh_orders_old
        """
    )
    cur.execute("DROP TABLE dwh_orders_old")


def _prepare_orders(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared = prepared.rename(
        columns={
            "email": "customer_email",
            "name": "customer_name",
            "age": "customer_age",
            "created_at": "customer_created_at",
        }
    )

    for column in ORDER_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = None

    prepared["customer_id"] = prepared["customer_id"].astype(str)

    return prepared[list(ORDER_COLUMNS)]


def _prepare_clickhouse_rows(df: pd.DataFrame) -> list[tuple]:
    prepared = _prepare_orders(df)
    rows = []

    for row in prepared.itertuples(index=False, name=None):
        rows.append(tuple(None if pd.isna(value) else value for value in row))

    return rows


def _clickhouse_order_ids(df: pd.DataFrame) -> list[int]:
    prepared = _prepare_orders(df)
    return prepared["order_id"].dropna().astype(int).drop_duplicates().tolist()


def _upsert_orders(
    cur: sqlite3.Cursor,
    df: pd.DataFrame,
    fail_after: int | None = None,
) -> None:
    prepared = _prepare_orders(df)

    for i, row in enumerate(prepared.itertuples(index=False), start=1):
        cur.execute(
            """
            INSERT INTO dwh_orders(
                order_id,
                customer_id,
                order_ts,
                status,
                amount,
                delivery_quality,
                product_category,
                customer_satisfaction,
                customer_email,
                customer_name,
                customer_age,
                customer_created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id=excluded.customer_id,
                order_ts=excluded.order_ts,
                status=excluded.status,
                amount=excluded.amount,
                delivery_quality=excluded.delivery_quality,
                product_category=excluded.product_category,
                customer_satisfaction=excluded.customer_satisfaction,
                customer_email=excluded.customer_email,
                customer_name=excluded.customer_name,
                customer_age=excluded.customer_age,
                customer_created_at=excluded.customer_created_at
            """,
            (
                row.order_id,
                row.customer_id,
                row.order_ts,
                row.status,
                row.amount,
                row.delivery_quality,
                row.product_category,
                row.customer_satisfaction,
                row.customer_email,
                row.customer_name,
                row.customer_age,
                row.customer_created_at,
            ),
        )
        if fail_after is not None and i == fail_after:
            raise RuntimeError(f"Simulated failure after {fail_after} rows")


def load(df: pd.DataFrame, db_path: Path | str, fail_after: int | None = None) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        conn.execute("BEGIN")
        _create_dwh_orders(cur)
        _upsert_orders(cur, df, fail_after=fail_after)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def full_reload(
    df: pd.DataFrame,
    db_path: Path | str,
    fail_after: int | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        conn.execute("BEGIN")
        _create_dwh_orders(cur)
        cur.execute("DELETE FROM dwh_orders")
        _upsert_orders(cur, df, fail_after=fail_after)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _create_clickhouse_orders(client) -> None:
    client.command("""
    CREATE TABLE IF NOT EXISTS dwh_orders (
        order_id UInt64,
        customer_id String,
        order_ts String,
        status String,
        amount Float64,
        delivery_quality Nullable(UInt8),
        product_category Nullable(UInt8),
        customer_satisfaction Nullable(UInt8),
        customer_email Nullable(String),
        customer_name Nullable(String),
        customer_age Nullable(UInt8),
        customer_created_at Nullable(String)
    )
    ENGINE = ReplacingMergeTree
    ORDER BY order_id
    """)


def load_clickhouse(df: pd.DataFrame) -> None:
    client = get_clickhouse_client()
    _create_clickhouse_orders(client)

    rows = _prepare_clickhouse_rows(df)
    if rows:
        order_ids = _clickhouse_order_ids(df)
        if order_ids:
            ids_sql = ", ".join(str(order_id) for order_id in order_ids)
            client.command(f"DELETE FROM dwh_orders WHERE order_id IN ({ids_sql})")
        client.insert("dwh_orders", rows, column_names=CLICKHOUSE_COLUMNS)


def full_reload_clickhouse(df: pd.DataFrame) -> None:
    client = get_clickhouse_client()
    _create_clickhouse_orders(client)
    client.command("TRUNCATE TABLE dwh_orders")

    rows = _prepare_clickhouse_rows(df)
    if rows:
        client.insert("dwh_orders", rows, column_names=CLICKHOUSE_COLUMNS)


def quality_checks(db_path: Path | str) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM dwh_orders WHERE amount <= 0")
        if cur.fetchone()[0] > 0:
            raise ValueError("Проверка качества не пройдена: amount <= 0")

        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT order_id, COUNT(*) AS cnt
                FROM dwh_orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            ) t
        """)
        if cur.fetchone()[0] > 0:
            raise ValueError("Проверка качества не пройдена: дубли order_id")
    finally:
        conn.close()
