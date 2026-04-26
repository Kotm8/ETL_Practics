import pandas as pd
from app.db.db import get_conn

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