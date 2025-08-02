# Nursing Home Data Pipeline (Local Prototype)

Purpose: Practice data engineering pipeline concepts (ingest → stage → curate → metrics) using public CMS nursing home CSVs with **SQLite only** first, then promote logic to AWS (S3 + Lambda + Aurora).

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdir -p csvs
# drop your CSVs into csvs/
python build.py
sqlite3 nh_local.db "SELECT * FROM v_penalties_by_state LIMIT 5;"
# load the additional nursing home CSVs into sqlite
python load_nursing_home_data.py
# calculate staffing metrics from the sqlite database
python calculate_metrics.py
# launch a Streamlit app to explore the data
streamlit run streamlit_app.py
