import pandas as pd

def extract_from_csv(data_url) -> pd.DataFrame:
    data = pd.read_csv(data_url)

    data["amount"] = pd.to_numeric(data["amount"], errors="coerce")
    data["order_ts"] = pd.to_datetime(data["order_ts"], errors="coerce")
    data["updated_at"] = pd.to_datetime(data["updated_at"], errors="coerce")

    return data