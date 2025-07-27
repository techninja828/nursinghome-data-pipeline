import pandas as pd
import streamlit as st

DATA_PATH = "metrics_summary.csv"


@st.cache_data
def load_metrics(path: str = DATA_PATH) -> pd.DataFrame:
    return pd.read_csv(path)


def main() -> None:
    st.title("Nursing Home Staffing Metrics")
    try:
        df = load_metrics(DATA_PATH)
    except FileNotFoundError:
        st.error("metrics_summary.csv not found. Run calculate_metrics.py first.")
        return

    states = sorted(df["STATE"].dropna().unique())
    selected_state = st.sidebar.selectbox("State", states)
    provs = sorted(df.loc[df["STATE"] == selected_state, "PROVNUM"].unique())
    selected_provs = st.sidebar.multiselect("Facility (PROVNUM)", provs, default=provs)

    filtered = df[(df["STATE"] == selected_state) & (df["PROVNUM"].isin(selected_provs))]

    st.subheader("Nurse-to-Patient Ratio by Facility")
    ratio_chart = (
        filtered.groupby("PROVNUM")["nurse_to_patient_ratio"].mean().sort_index()
    )
    st.bar_chart(ratio_chart)

    st.subheader("Total Nurse Hours Over Time")
    pivot_hours = filtered.pivot_table(
        index="CY_Qtr",
        columns="PROVNUM",
        values="total_nurse_hours",
        aggfunc="sum",
    ).sort_index()
    st.line_chart(pivot_hours)

    st.subheader("Contract vs. Employed Ratio")
    contract_chart = (
        filtered.groupby("PROVNUM")["contract_vs_employed_ratio"].mean().sort_index()
    )
    st.bar_chart(contract_chart)


if __name__ == "__main__":
    main()
