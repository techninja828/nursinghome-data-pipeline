import logging
import pandas as pd
from pathlib import Path
import re
from typing import List, Tuple

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


def load_data(data_dir: Path = DATA_DIR) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """Scan CSVs and merge them on PROVNUM/CY_Qtr.

    Returns a tuple of the merged dataframe and a list of merge pairs that
    produced empty results.
    """
    frames: List[Tuple[str, pd.DataFrame]] = []
    for csv_file in data_dir.glob("*.csv"):
        df = pd.read_csv(csv_file, low_memory=False)
        if {"PROVNUM", "CY_Qtr"}.issubset(df.columns):
            frames.append((csv_file.name, df))

    if not frames:
        raise FileNotFoundError(
            f"No CSV files with PROVNUM and CY_Qtr found in {data_dir}"
        )

    merged_name, merged = frames[0]
    empty_merges: List[Tuple[str, str]] = []
    for name, df in frames[1:]:
        merged = pd.merge(merged, df, on=["PROVNUM", "CY_Qtr"], how="inner")
        if merged.empty:
            empty_merges.append((merged_name, name))
        merged_name = f"{merged_name}+{name}"

    missing = [col for col in REQUIRED_COLS if col not in merged.columns]
    if missing:
        cols = ", ".join(missing)
        raise ValueError(f"Missing required columns after merge: {cols}")
    return merged, empty_merges


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

    critical_cols = ["MDScensus", "Hrs_RN", "Hrs_LPN", "Hrs_CNA"]
    zero_mask = (df[critical_cols] == 0).any(axis=1)
    if zero_mask.any():
        logging.warning(
            "Replacing zero values in critical columns with NA for %d rows", zero_mask.sum()
        )
        df.loc[zero_mask, critical_cols] = pd.NA

    before_drop = len(df)
    df = df.dropna(subset=[
        "MDScensus",
        "STATE",
        "CY_Qtr",
        "Hrs_RN",
        "Hrs_LPN",
        "Hrs_CNA",
    ])
    dropped = before_drop - len(df)
    if dropped:
        logging.warning(
            "Dropped %d rows due to zero or missing critical values", dropped
        )
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
    zero_census = grouped["total_census"] == 0
    zero_employed = grouped["total_employed"] == 0
    zero_rows = (zero_census | zero_employed).sum()
    if zero_rows:
        logging.warning(
            "Replacing zero denominators with NA for %d rows", zero_rows
        )
        grouped.loc[zero_census, "total_census"] = pd.NA
        grouped.loc[zero_employed, "total_employed"] = pd.NA

    grouped["nurse_to_patient_ratio"] = grouped["total_nurse_hours"] / grouped["total_census"]
    grouped["contract_vs_employed_ratio"] = grouped["total_contract"] / grouped["total_employed"]

    before_drop = len(grouped)
    grouped = grouped.dropna(
        subset=["nurse_to_patient_ratio", "contract_vs_employed_ratio"]
    )
    dropped = before_drop - len(grouped)
    if dropped:
        logging.warning(
            "Dropped %d rows due to zero or missing denominators", dropped
        )

    return grouped[[
        "PROVNUM",
        "STATE",
        "CY_Qtr",
        "nurse_to_patient_ratio",
        "contract_vs_employed_ratio",
        "total_nurse_hours",
    ]]


def main() -> None:
    df, empty_merges = load_data(DATA_DIR)
    if empty_merges:
        for left, right in empty_merges:
            print(f"WARNING: merge between {left} and {right} produced no rows")
    df = clean_and_prepare(df)
    metrics = calculate_metrics(df)
    metrics.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved metrics -> {OUTPUT_CSV} rows={len(metrics)}")


if __name__ == "__main__":
    main()
