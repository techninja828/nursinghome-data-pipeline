import pandas as pd
import sqlite3
from pathlib import Path

DATA_DIR = Path("Nursing_Homes_data")
DB_PATH = "nh_local.db"


def norm(name: str) -> str:
    """Normalize CSV filename to a sqlite table name."""
    return (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("__", "_")
        .rstrip(".csv")
    )


def check_completeness(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Return completeness metrics for a dataframe."""
    metrics = []
    total = len(df)
    for col in df.columns:
        non_null = df[col].notna().sum()
        pct = round(non_null / total * 100, 2) if total else 0
        metrics.append(
            {
                "table_name": table,
                "column_name": col,
                "row_count": total,
                "non_null_count": non_null,
                "pct_not_null": pct,
            }
        )
    return pd.DataFrame(metrics)


def load_csvs_to_db(db_path: str = DB_PATH, data_dir: Path = DATA_DIR) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS dq_completeness(
            table_name TEXT,
            column_name TEXT,
            row_count INTEGER,
            non_null_count INTEGER,
            pct_not_null REAL
        )"""
    )
    for csv_file in data_dir.glob("*.csv"):
        df = pd.read_csv(csv_file, low_memory=False)
        table = norm(csv_file.stem)
        df.to_sql(table, conn, if_exists="replace", index=False)
        metrics = check_completeness(df, table)
        metrics.to_sql("dq_completeness", conn, if_exists="append", index=False)
        print(f"Loaded {csv_file.name} -> {table} rows={len(df)}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    load_csvs_to_db()
