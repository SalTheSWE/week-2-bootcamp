import pandas as pd
def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    n_missing = df.isna().sum()
    p_missing = n_missing/len(df)
    dict = {"n_missing":n_missing, "p_missing":p_missing}
    return pd.DataFrame(dict)

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    copy = df.copy()
    for i in cols:
        copy[f"{i}(missing?)"] = copy[i].isna()
        return copy
    
def normalize_text(s: pd.Series) -> pd.Series:
    s = s.astype("string")
    return s.str.casefold().str.replace(" ","")

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.replace(mapping)

def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    return df.sort_values(ts_col).drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    df = df.copy()
    parsed_column = pd.to_datetime(df[col],errors="coerce", utc=utc)
    df[f"{col}_timeparsed"] = parsed_column
    return df

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    time_series = pd.Series(df[ts_col])
    df = df.copy()
    df["date"] = time_series.dt.date
    df["year"] = time_series.dt.year
    df["month"] = time_series.dt.to_period("M")
    df["day"] = time_series.dt.day_name()
    df["hour"] = time_series.dt.hour
    return df
def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k*iqr), float(q3 + k*iqr)
def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    a, b = s.quantile(lo), s.quantile(hi)
    return s.clip(lower=a, upper=b)
