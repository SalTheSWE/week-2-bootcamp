import pandas as pd
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from bootcamp_data.io import write_parquet, read_orders_csv
from bootcamp_data.transforms import winsorize, add_time_parts, parse_datetime, enforce_schema, missingness_report, normalize_text, add_missing_flags
from bootcamp_data.config import make_paths
from bootcamp_data.quality import assert_unique_key, require_columns, assert_non_empty, assert_in_range
from bootcamp_data.joins import safe_left_join

paths = make_paths(ROOT)
#load parquets
orders = pd.read_parquet(paths.processed/"orders_clean.parquet")
users = pd.read_parquet(paths.processed/"users.parquet")
#check columns and uniqueness
orders_cols = ["order_id","user_id","amount","quantity","created_at","status"]
users_cols = ["user_id","country","signup_date"]
require_columns(users, users_cols)
require_columns(orders, orders_cols)
assert_unique_key(users, "user_id")
#parsing time
orders = parse_datetime(orders,"created_at")
#add time parts(columns)
orders = add_time_parts(orders,["created_at_timeparsed"])
#join orders with users
joined_orders = safe_left_join(orders, users, "user_id", validate="many_to_one")
assert len(orders) == len(joined_orders), "rows dont match after join"
#amount winsor
amount_winsor = winsorize(joined_orders["amount"], 0.01, 0.99)


write_parquet(joined_orders,paths.processed/"analytics_table.parquet")
n_rows = len(joined_orders)
print(f"Saved {n_rows} rows to processed/analytics_table.parquet")

print(f"with dtypes:\n{joined_orders.dtypes}")
