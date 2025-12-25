## Setup
uv venv -p 3.11

# activate venv
uv pip install -r requirements.txt

## Run ETL
uv run .\scripts\run_etl.py    

## Outputs
- data/processed/analytics_table.parquet
- data/processed/_run_meta.json
- reports/figures/*.png

## EDA
Open notebooks/eda.ipynb and run all cells, make sure u choose the venv kernel that u created above