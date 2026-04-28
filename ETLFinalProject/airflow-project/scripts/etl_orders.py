import re
from pathlib import Path

import pandas as pd

from scripts.orders_extract import extract, extract_orders_from_minio, extract_users_from_api
from scripts.orders_load import (
    full_reload,
    full_reload_clickhouse,
    load,
    load_clickhouse,
    quality_checks,
)
from scripts.orders_transform import transform_orders_with_users


def choose_load_mode(logical_date=None, **kwargs):
    run_date = logical_date or kwargs["logical_date"]
    if run_date.day == 1:
        return "full_reload_task"
    return "incremental_task"


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _stage_path(stage_dir: Path | str, run_id: str, step: str) -> Path:
    return Path(stage_dir) / f"{_safe_run_id(run_id)}_{step}.csv"


def _write_dataframe(df: pd.DataFrame, output_path: Path) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return str(output_path)


def extract_orders_to_stage(stage_dir: str, run_id: str) -> str:
    df = extract_orders_from_minio()
    return _write_dataframe(df, _stage_path(stage_dir, run_id, "extract_orders"))

def extract_users_to_stage(stage_dir: str, run_id: str) -> str:
    df = extract_users_from_api()
    return _write_dataframe(df, _stage_path(stage_dir, run_id, "extract_users"))


def transform_stage(stage_dir: str, run_id: str) -> str:
    orders_path = _stage_path(stage_dir, run_id, "extract_orders")
    users_path = _stage_path(stage_dir, run_id, "extract_users")
    transformed = transform_orders_with_users(extract(orders_path), extract(users_path))
    return _write_dataframe(transformed, _stage_path(stage_dir, run_id, "transform"))


def load_stage(
    db_path: str,
    stage_dir: str,
    run_id: str,
    fail_after: int | None = None,
) -> None:
    transform_path = _stage_path(stage_dir, run_id, "transform")
    df = extract(transform_path)
    load(df, db_path, fail_after=fail_after)
    load_clickhouse(df)


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
    full_reload(df, db_path, fail_after=fail_after)
    full_reload_clickhouse(df)
