import requests
import snowflake.connector
import pandas as pd

def ingest_sessions():
    print("📅 Starting sessions data ingestion (incremental)...")

    # --- Fetch from API
    try:
        payload = requests.get("https://api.openf1.org/v1/sessions", timeout=30).json()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch sessions: {e}")

    if not payload:
        print("⚠️ No session data found")
        return
    if isinstance(payload, dict):
        # e.g., {"detail": "..."} or single object
        if "detail" in payload:
            print(f"⚠️ API error: {payload.get('detail')}")
            return
        payload = [payload]
    if not isinstance(payload, list):
        print(f"⚠️ Unexpected payload type: {type(payload)}")
        return

    df = pd.DataFrame(payload)

    required_cols = [
        "session_key", "meeting_key", "session_name",
        "session_type", "date_start", "date_end", "location"
    ]
    # keep only available columns
    keep_cols = [c for c in required_cols if c in df.columns]
    if "session_key" not in keep_cols:
        print("❌ sessions payload missing session_key; aborting.")
        return

    df = df[keep_cols]
    before = len(df)
    df = df.drop_duplicates(subset=["session_key"])
    if len(df) < before:
        print(f"ℹ️ Removed {before - len(df)} duplicate sessions in payload")

    # --- Snowflake connect
    conn = snowflake.connector.connect(
        user='MARKPONI',
        password='ZXCVB12345zxcvb',
        account='zczvngs-ti82223',
        warehouse='F1_WH',
        database='F1',
        schema='poni',
        role='ACCOUNTADMIN'
    )
    cur = conn.cursor()
    print("🔐 Connected to Snowflake")

    # --- Ensure table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_key INT PRIMARY KEY,
            meeting_key INT,
            session_name STRING,
            session_type STRING,
            date_start TIMESTAMP_TZ,
            date_end TIMESTAMP_TZ,
            location STRING
        );
    """)

    # --- Find existing session_keys to skip
    cur.execute("SELECT session_key FROM sessions")
    existing = {r[0] for r in cur.fetchall() if r[0] is not None}
    df_new = df[~df["session_key"].isin(existing)]

    if df_new.empty:
        print("⏭️ No new sessions to insert.")
        cur.close(); conn.close()
        return

    # Order columns for insert
    cols = [c for c in required_cols if c in df_new.columns]
    records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df_new[cols].iterrows()]

    # --- Insert only new rows
    try:
        cur.executemany(
            f"INSERT INTO sessions ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
            records
        )
        conn.commit()
        print(f"✅ Inserted {len(df_new)} new sessions")
    except Exception as e:
        print(f"❌ Insert failed: {e}")

    cur.close()
    conn.close()
