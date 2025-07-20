import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "nh_local.db"


def get_tables(conn):
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [r[0] for r in cursor.fetchall()]


def load_table(table: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df


def main():
    st.title("Nursing Home Data Overview")
    conn = sqlite3.connect(DB_PATH)
    tables = get_tables(conn)
    conn.close()
    if not tables:
        st.error("No tables found. Run load_nursing_home_data.py first.")
        return
    table = st.selectbox("Choose a table", tables)
    df = load_table(table)
    st.write(df.head())

    numeric_cols = df.select_dtypes(include=["number"]).columns
    if numeric_cols.any():
        st.subheader("Numeric Column Averages")
        st.bar_chart(df[numeric_cols].mean())

    conn = sqlite3.connect(DB_PATH)
    dq = pd.read_sql(
        "SELECT column_name, pct_not_null FROM dq_completeness WHERE table_name = ?",
        conn,
        params=(table,),
    )
    conn.close()
    if not dq.empty:
        st.subheader("Completeness % by Column")
        st.bar_chart(dq.set_index("column_name") ["pct_not_null"])


if __name__ == "__main__":
    main()
