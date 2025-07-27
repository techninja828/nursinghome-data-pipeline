import pandas as pd
from pathlib import Path
import re

DATA_DIR = Path("Nursing_Homes_data")
OUTPUT_CSV = "metrics_summary.csv"

# Columns used for metrics
REQUIRED_COLS = [
    "MDScensus",
    "STATE",
    "CY_Qtr",
    "PROVNUM",
    "Hrs_RN",
    "Hrs_LPN",
    "Hrs_CNA",
    "Hrs_RN_ctr",
    "Hrs_LPN_ctr",
    "Hrs_CNA_ctr",
    "Hrs_RN_emp",
    "Hrs_LPN_emp",
    "Hrs_CNA_emp",
]


def normalize_quarter(val: str) -> str:
    """Return a standardized CY_Qtr string like '2024-Q1'."""
    if pd.isna(val):
        return None
    m = re.search(r"(20\d{2}).*?(\d)", str(val))
    if m:
        year, q = m.groups()
        return f"{year}-Q{q}"
    return None


def load_data(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    frames = []
    for csv_file in data_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file, low_memory=False)
        except Exception:
            continue
        if not set(["MDScensus", "PROVNUM"]).issubset(df.columns):
            continue
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No valid CSV files found with required columns")
    return pd.concat(frames, ignore_index=True)


def clean_and_prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # normalize CY_Qtr
    df["CY_Qtr"] = df["CY_Qtr"].apply(normalize_quarter)

    numeric_cols = [
        "MDScensus",
        "Hrs_RN",
        "Hrs_LPN",
        "Hrs_CNA",
        "Hrs_RN_ctr",
        "Hrs_LPN_ctr",
        "Hrs_CNA_ctr",
        "Hrs_RN_emp",
        "Hrs_LPN_emp",
        "Hrs_CNA_emp",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=[
        "MDScensus",
        "STATE",
        "CY_Qtr",
        "Hrs_RN",
        "Hrs_LPN",
        "Hrs_CNA",
    ])
    return df


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["total_hours"] = df[["Hrs_RN", "Hrs_LPN", "Hrs_CNA"]].sum(axis=1)
    df["contract_hours"] = df[["Hrs_RN_ctr", "Hrs_LPN_ctr", "Hrs_CNA_ctr"]].sum(axis=1)
    df["employed_hours"] = df[["Hrs_RN_emp", "Hrs_LPN_emp", "Hrs_CNA_emp"]].sum(axis=1)

    grouped = (
        df.groupby(["STATE", "PROVNUM", "CY_Qtr"], as_index=False)
        .agg(
            total_nurse_hours=("total_hours", "sum"),
            total_census=("MDScensus", "sum"),
            total_contract=("contract_hours", "sum"),
            total_employed=("employed_hours", "sum"),
        )
    )

    grouped["nurse_to_patient_ratio"] = grouped["total_nurse_hours"] / grouped["total_census"]
    grouped["contract_vs_employed_ratio"] = grouped["total_contract"] / grouped["total_employed"]

    return grouped[[
        "PROVNUM",
        "STATE",
        "CY_Qtr",
        "nurse_to_patient_ratio",
        "contract_vs_employed_ratio",
        "total_nurse_hours",
    ]]


def main() -> None:
    df = load_data(DATA_DIR)
    df = clean_and_prepare(df)
    metrics = calculate_metrics(df)
    metrics.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved metrics -> {OUTPUT_CSV} rows={len(metrics)}")


if __name__ == "__main__":
    main()
