from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.task.trigger_rule import TriggerRule

from plugins.deadline_callbacks import deadline_missed_callback
from scripts.etl_orders import (
    choose_load_mode,
    extract_orders_to_stage,
    extract_users_to_stage,
    full_reload_callable,
    incremental_callable,
    quality_checks,
    transform_stage,
)


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / ".." / "warehouse" / "etl_orders.db"
STAGE_DIR = BASE_DIR / ".." / "warehouse" / "stage"


default_args = {
    "owner": "etl",
    "start_date": datetime(2026, 4, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_orders_pipeline",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=1),
        callback=SyncCallback(deadline_missed_callback),
    ),
) as dag:
    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders_to_stage,
        op_kwargs={
            "stage_dir": str(STAGE_DIR),
            "run_id": "{{ run_id }}",
        },
    )

    extract_users = PythonOperator(
        task_id="extract_users",
        python_callable=extract_users_to_stage,
        op_kwargs={
            "stage_dir": str(STAGE_DIR),
            "run_id": "{{ run_id }}",
        },
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_stage,
        op_kwargs={
            "stage_dir": str(STAGE_DIR),
            "run_id": "{{ run_id }}",
        },
    )

    branch_task = BranchPythonOperator(
        task_id="choose_load_mode",
        python_callable=choose_load_mode,
    )

    full_reload_task = PythonOperator(
        task_id="full_reload_task",
        python_callable=full_reload_callable,
        op_kwargs={
            "db_path": str(DB_PATH),
            "stage_dir": str(STAGE_DIR),
            "run_id": "{{ run_id }}",
            "fail_after": None,
        },
    )

    incremental_task = PythonOperator(
        task_id="incremental_task",
        python_callable=incremental_callable,
        op_kwargs={
            "db_path": str(DB_PATH),
            "stage_dir": str(STAGE_DIR),
            "run_id": "{{ run_id }}",
            "fail_after": None,
        },
    )

    join_task = EmptyOperator(
        task_id="join_after_load_mode",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    qc_task = PythonOperator(
        task_id="run_quality_checks",
        python_callable=quality_checks,
        op_kwargs={"db_path": str(DB_PATH)},
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "ETL completed successfully"',
    )

    extract_orders >> extract_users >> transform_task >> branch_task # type: ignore
    branch_task >> full_reload_task >> join_task # type: ignore
    branch_task >> incremental_task >> join_task # type: ignore
    join_task >> qc_task >> notify_success # type: ignore
