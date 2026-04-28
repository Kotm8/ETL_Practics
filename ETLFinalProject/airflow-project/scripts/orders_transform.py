import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()

    if "amount" not in transformed.columns and "revenue" in transformed.columns:
        transformed["amount"] = transformed["revenue"]
    if "status" not in transformed.columns:
        transformed["status"] = "created"

    transformed["amount"] = pd.to_numeric(transformed["amount"], errors="coerce")
    transformed = transformed.dropna(
        subset=["order_id", "customer_id", "order_ts", "status", "amount"]
    )
    transformed = transformed[transformed["amount"] > 0]
    return transformed

def transform_orders_with_users(
    orders_df: pd.DataFrame,
    users_df: pd.DataFrame,
) -> pd.DataFrame:
    orders = orders_df.copy()
    users = users_df.copy()

    orders = transform(orders)

    orders["customer_id"] = orders["customer_id"].astype(str)
    users["id"] = users["id"].astype(str)

    joined = orders.merge(
        users,
        left_on="customer_id",
        right_on="id",
        how="left",
    )

    return joined
