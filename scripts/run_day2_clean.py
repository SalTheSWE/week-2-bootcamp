
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from bootcamp_data.io import write_parquet, read_orders_csv
from bootcamp_data.transforms import enforce_schema, missingness_report, normalize_text, add_missing_flags
from bootcamp_data.config import make_paths
from bootcamp_data.quality import require_columns, assert_non_empty, assert_in_range

paths = make_paths(ROOT)


df = read_orders_csv(paths.raw/"orders.csv")# up to this point is just to load the csv and setup


#verify columns + non-empty + enforce schema
cols = ["order_id","user_id","amount","quantity","created_at","status"]
require_columns(df, cols)
assert_non_empty(df, "orders")
df = enforce_schema(df)

#generate missingness report and save as parquet to reports
report = missingness_report(df)
write_parquet(report,paths.reports/"missing_report.parquet")


#normalize status into status_clean(without changing the original dataframe)
df = df.copy()
df["status_clean"] = normalize_text(df["status"])

#add flags columns
orders_clean = add_missing_flags(df,["amount","quantity"])

assert_in_range(orders_clean["amount"], lo=0, name="amount")
assert_in_range(orders_clean["quantity"], lo=0, name="quantity")
n_rows = len(df)
write_parquet(orders_clean,paths.processed/"orders_clean.parquet")

print(f"Saved {n_rows} rows to processed/orders_clean.parquet")
print(f"with dtypes:\n{df.dtypes}")
