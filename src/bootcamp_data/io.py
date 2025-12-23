import pandas as pd
from pathlib import Path
NA = ["", "NA", "N/A", "null", "None", "not_a_number"]

def read_order_csv(path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype={"order_id": "string", "user_id": "string"},
        na_values=NA,
        keep_default_na=True,
    )

def read_users_csv(path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype={"user_id": "string"},
        na_values=NA,
        keep_default_na=True,
    )
def write_parquet(df, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def read_parquet(path) -> pd.DataFrame:
    return pd.read_parquet(path)

# if __name__ == "__main__":
#     df =read_order_csv(ROOT/ "data/order.csv")
#     print(df)
#     write_parquet(df, ROOT/ "data/processed/orders_parquet.parquet")