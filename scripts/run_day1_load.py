
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from bootcamp_data.io import write_parquet, read_orders_csv, read_users_csv
from bootcamp_data.transforms import enforce_schema
from bootcamp_data.config import make_paths

paths = make_paths(ROOT)




df = read_orders_csv(paths.raw/"orders.csv")

users = read_users_csv(paths.raw/"users.csv")
n_rows = len(df)

write_parquet(enforce_schema(df),paths.processed/"processed_orders.parquet")
write_parquet(users,paths.processed/"users.parquet")
print(f"Saved {n_rows} rows to processed/processed_orders.parquet")
print(f"with dtypes:\n{df.dtypes}")
