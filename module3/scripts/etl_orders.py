import re
import sqlite3
from pathlib import Path

import pandas as pd


def choose_load_mode(logical_date=None, **kwargs):
    run_date = logical_date or kwargs["logical_date"]
    if run_date.day == 1:
        return "full_reload_task"
    return "incremental_task"


def extract(csv_path: Path | str) -> pd.DataFrame:
    return pd.read_csv(Path(csv_path))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()
    transformed["amount"] = pd.to_numeric(transformed["amount"], errors="coerce")
    transformed = transformed.dropna(
        subset=["order_id", "customer_id", "order_ts", "status", "amount"]
    )
    transformed = transformed[transformed["amount"] > 0]
    return transformed


def _create_dwh_orders(cur: sqlite3.Cursor) -> None:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dwh_orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_ts TEXT NOT NULL,
        status TEXT NOT NULL,
        amount REAL NOT NULL
    )
    """)


def _upsert_orders(
    cur: sqlite3.Cursor,
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    fail_after: int | None = None,
) -> None:
    for i, row in enumerate(df.itertuples(index=False), start=1):
        cur.execute(
            """
            INSERT INTO dwh_orders(order_id, customer_id, order_ts, status, amount)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id=excluded.customer_id,
                order_ts=excluded.order_ts,
                status=excluded.status,
                amount=excluded.amount
            """,
            (row.order_id, row.customer_id, row.order_ts, row.status, row.amount),
        )
        if fail_after is not None and i == fail_after:
            conn.commit()
            raise RuntimeError(f"Simulated failure after {fail_after} rows")


def load(df: pd.DataFrame, db_path: Path | str, fail_after: int | None = None) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        _create_dwh_orders(cur)
        _upsert_orders(cur, conn, df, fail_after=fail_after)
        conn.commit()
    finally:
        conn.close()


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _stage_path(stage_dir: Path | str, run_id: str, step: str) -> Path:
    return Path(stage_dir) / f"{_safe_run_id(run_id)}_{step}.csv"


def _write_dataframe(df: pd.DataFrame, output_path: Path) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return str(output_path)


def extract_to_stage(csv_path: str, stage_dir: str, run_id: str) -> str:
    df = extract(csv_path)
    return _write_dataframe(df, _stage_path(stage_dir, run_id, "extract"))


def transform_stage(stage_dir: str, run_id: str) -> str:
    extract_path = _stage_path(stage_dir, run_id, "extract")
    transformed = transform(extract(extract_path))
    return _write_dataframe(transformed, _stage_path(stage_dir, run_id, "transform"))


def load_stage(
    db_path: str,
    stage_dir: str,
    run_id: str,
    fail_after: int | None = None,
) -> None:
    transform_path = _stage_path(stage_dir, run_id, "transform")
    load(extract(transform_path), db_path, fail_after=fail_after)


def incremental_callable(
    db_path: str,
    stage_dir: str,
    run_id: str,
    fail_after: int | None = None,
) -> None:
    load_stage(db_path=db_path, stage_dir=stage_dir, run_id=run_id, fail_after=fail_after)


def full_reload_callable(
    db_path: str,
    stage_dir: str,
    run_id: str,
    fail_after: int | None = None,
) -> None:
    transform_path = _stage_path(stage_dir, run_id, "transform")
    df = extract(transform_path)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        _create_dwh_orders(cur)
        cur.execute("DELETE FROM dwh_orders")
        _upsert_orders(cur, conn, df, fail_after=fail_after)
        conn.commit()
    finally:
        conn.close()


def quality_checks(db_path: Path | str) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

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

    conn.close()
