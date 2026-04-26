import pandas as pd
from app.db.db import get_conn

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