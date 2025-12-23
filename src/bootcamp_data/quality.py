import pandas as pd


def require_columns(df,cols):
    missing = []
    for c in cols: 
        if c not in df.columns:
            missing.append(c)
    assert not missing, f"{missing} is missing(column)"

def assert_non_empty(df, name="df"):
    assert len(df)>0 , f"empty dataframe: {name}"

def assert_unique_key(df, key, allow_na=False):
    assert df[key].notna().all(), f"missing values in {key} column" or allow_na
    assert not df[key].duplicated().any(), f"duplicate values in {key} column"

def assert_in_range(series, lo=None, hi=None, name="series"):
    if lo:
        assert (series>=lo).all(), f"series{name} is below range"
    if hi:
        assert (series<=hi).all(), f"series{name} is above range"
