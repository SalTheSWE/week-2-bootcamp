from pathlib import Path
import pandas as pd
def read_orders_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path,
                        sep=",",
                        decimal=".",
                        dtype={"order_id": "string",
                                "user_id": "string",
                                "created_at": "string",
                                "amount": "float64",
                                "quantity":"int64",
                                "status": "boolean"},
                                na_values=["","NA","null"])
def read_users_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path,
                        sep=",",
                        decimal=".",
                        dtype={ "user_id": "string",
                                "country": "string",
                                "signup_date": "float64",},
                        na_values=["","NA","null"])
def write_parquet(df, path)