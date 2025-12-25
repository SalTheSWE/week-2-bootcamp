import pandas as pd
from pathlib import Path
import sys



ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from bootcamp_data.etl import ETLConfig, run_etl

cfg = ETLConfig( root=ROOT, 
                raw_orders=ROOT / "data" / "raw" / "orders.csv", 
                raw_users=ROOT / "data" / "raw" / "users.csv",
                out_orders_clean=ROOT / "data" / "processed" / "orders_clean.parquet", 
                out_users=ROOT / "data" / "processed" / "users.parquet", 
                out_analytics=ROOT / "data" / "processed" / "analytics_table.parquet", 
                run_meta=ROOT / "data" / "processed" / "_run_meta.json", )

run_etl(cfg)