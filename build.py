#!/usr/bin/env python
import sqlite3, pandas as pd, yaml, glob, os, re, datetime as dt, sys
DB_PATH = "nh_local.db"
CONFIG_PATH = "config/datasets.yml"
CSV_DIR = "csvs"

def norm(name):
    return re.sub(r'_+','_', re.sub(r'[^0-9a-zA-Z]+','_', name.strip().lower())).strip('_')

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

def ensure_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE dq_audit(
        id INTEGER PRIMARY KEY,
        table_name TEXT, check_name TEXT, status TEXT,
        metric_value REAL, threshold REAL, sample_rows INTEGER,
        notes TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    return conn

def cast_series(s, meta):
    t = meta['type']
    if t == 'date':
        s = pd.to_datetime(s, errors='coerce').dt.date
    elif t in ('numeric','int'):
        s = pd.to_numeric(s, errors='coerce')
        if t == 'int':
            s = s.astype('Int64')
    else:
        s = s.astype(str).str.strip()
    return s

def dq_duplicate_check(conn, table, keys):
    if not keys: return
    q = f"""
      SELECT {', '.join(keys)}, COUNT(*) c
      FROM {table}
      GROUP BY {', '.join(keys)}
      HAVING COUNT(*) > 1
    """
    df = pd.read_sql(q, conn)
    status = 'ok' if df.empty else 'warn'
    conn.execute("""
      INSERT INTO dq_audit(table_name, check_name, status, metric_value, threshold, sample_rows, notes)
      VALUES (?,?,?,?,?,?,?)
    """, (table, f"dup_{'_'.join(keys)}", status, len(df), 0, min(len(df),5),
          df.head().to_json() if not df.empty else None))

def main():
    cfg = load_config()
    conn = ensure_db()
    datasets = cfg['datasets']
    for name, spec in datasets.items():
        pattern = os.path.join(CSV_DIR, spec['filename_pattern'])
        files = glob.glob(pattern)
        if not files:
            print(f"[WARN] No files for {name}")
            continue
        frames = []
        for fp in files:
            df = pd.read_csv(fp, low_memory=False)
            df.columns = [norm(c) for c in df.columns]
            # rename / align expected columns if necessary outside spec
            for col, meta in spec['columns'].items():
                if col in df.columns:
                    df[col] = cast_series(df[col], meta)
                else:
                    df[col] = None
            df['source_file'] = os.path.basename(fp)
            df['ingestion_date'] = dt.date.today()
            frames.append(df)
        full = pd.concat(frames, ignore_index=True)
        table = spec['staging_table']
        full.to_sql(table, conn, if_exists='replace', index=False)
        dq_duplicate_check(conn, table, spec.get('natural_key', []))
        print(f"[OK] Loaded {name} → {table} rows={len(full)}")
    # Curated example (penalties)
    if 'penalties' in datasets:
        conn.execute("DROP TABLE IF EXISTS fact_penalty;")
        conn.execute("""
          CREATE TABLE fact_penalty AS
          SELECT
            cms_certification_number_ccn AS ccn,
            date(penalty_date) AS penalty_date,
            penalty_type,
            COALESCE(fine_amount,0) AS fine_amount,
            payment_denial_length_in_days AS denial_days,
            state,
            provider_name
          FROM staging_penalties;
        """)
        conn.execute("""
          CREATE VIEW v_penalties_by_state AS
          SELECT state,
                 COUNT(*) penalty_events,
                 SUM(fine_amount) total_fines,
                 SUM(CASE WHEN fine_amount>0 THEN 1 END) fine_count,
                 ROUND(AVG(fine_amount),2) avg_fine
          FROM fact_penalty
          GROUP BY state;
        """)
        print("[OK] Curated fact_penalty + view v_penalties_by_state")
    conn.commit()
    conn.close()
    print("Done. DB:", DB_PATH)

if __name__ == "__main__":
    sys.exit(main())
