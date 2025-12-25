from pathlib import Path

from dataclasses import dataclass
import pandas as pd

from bootcamp_data.io import write_parquet, read_orders_csv, read_users_csv
from bootcamp_data.transforms import winsorize, add_time_parts, parse_datetime, enforce_schema, missingness_report, normalize_text, add_missing_flags
from bootcamp_data.config import make_paths
from bootcamp_data.quality import assert_unique_key, require_columns, assert_non_empty, assert_in_range
from bootcamp_data.joins import safe_left_join
import json 
from dataclasses import asdict 




@dataclass(frozen=True) 
class ETLConfig: 
    root: Path 
    raw_orders: Path 
    raw_users: Path 
    out_orders_clean: Path 
    out_users: Path 
    out_analytics: Path 
    run_meta: Path


def run_etl(cfg: ETLConfig) -> None: 
    orders_raw, users = load_inputs(cfg)
    analytics = transform(orders_raw, users)
    load_outputs(analytics, users, cfg)
    write_run_meta(cfg, analytics=analytics)

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

def transform(orders: pd.DataFrame, users: pd.DataFrame):
    #check columns and uniqueness
    orders_cols = ["order_id","user_id","amount","quantity","created_at","status"]
    users_cols = ["user_id","country","signup_date"]
    require_columns(users, users_cols)
    require_columns(orders, orders_cols)
    assert_unique_key(users, "user_id")
    orders = enforce_schema(orders)
    #parsing time
    orders = parse_datetime(orders,"created_at")
    #add time parts(columns)
    orders = add_time_parts(orders,"created_at_timeparsed")
    #join orders with users
    joined_orders = safe_left_join(orders, users, "user_id", validate="many_to_one")
    assert len(orders) == len(joined_orders), "rows dont match after join"
    #amount winsor
    joined_orders["amount_winsor"]= winsorize(joined_orders["amount"], 0.01, 0.99)
    return joined_orders

def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    write_parquet(users, cfg.out_users) 
    write_parquet(analytics, cfg.out_analytics)




def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None: 
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())
    meta = {
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")

