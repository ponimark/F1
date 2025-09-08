import requests
import snowflake.connector
import pandas as pd
import json


def ingest_intervals():
    print("⏱ Starting incremental intervals ingestion...")

    # 1) Fetch all sessions from API
    try:
        sessions = requests.get("https://api.openf1.org/v1/sessions", timeout=30).json()
    except json.JSONDecodeError:
        print("❌ Could not parse /sessions JSON")
        return
    if not isinstance(sessions, list):
        print(f"❌ /sessions returned {type(sessions)}; expected list")
        return

    NAME_ALLOW = ("Qualifying", "Race", "Sprint", "Sprint Shootout")
    target_sessions = [
        s for s in sessions
        if s.get("session_key") and any(tok in s.get("session_name", "") for tok in NAME_ALLOW)
    ]
    print(f"🧩 Found {len(target_sessions)} target sessions")

    # 2) Connect to Snowflake
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

    # 3) Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intervals (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            gap_to_leader string,
            interval string
        );
    """)

    # 4) Get already processed session_keys
    cur.execute("SELECT DISTINCT session_key FROM intervals")
    existing_sessions = {row[0] for row in cur.fetchall() if row[0] is not None}
    print(f"📂 {len(existing_sessions)} sessions already in Snowflake")

    insert_sql = """
        INSERT INTO intervals (
            driver_number, session_key, date, gap_to_leader, interval
        ) VALUES (%s, %s, %s, %s, %s)
    """

    total_inserted = 0

    # 5) Process only missing sessions
    for s in target_sessions:
        session_key = s["session_key"]

        if session_key in existing_sessions:
            print(f"⏭️ Skipping session {session_key} (already loaded)")
            continue

        print(f"📥 Fetching intervals for new session {session_key}")
        try:
            data = requests.get(f"https://api.openf1.org/v1/intervals?session_key={session_key}", timeout=30).json()
        except Exception as e:
            print(f"⚠️ Error fetching session {session_key}: {e}")
            continue

        if not data:
            print(f"⚠️ No intervals for session {session_key}")
            continue
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            print(f"⚠️ Unexpected type {type(data)} for session {session_key}")
            continue

        df = pd.DataFrame(data)
        if df.empty:
            continue

        expected_cols = ["driver_number", "session_key", "date", "gap_to_leader", "interval"]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]

        for col in ["gap_to_leader", "interval"]:
            df[col] = df[col].where(pd.notnull(df[col]), None)
            df[col] = df[col].apply(lambda v: None if v is None else str(v))

        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]
        if not records:
            continue

        try:
            cur.executemany(insert_sql, records)
            conn.commit()
            total_inserted += len(records)
            print(f"✅ Inserted {len(records)} rows for session {session_key}")
        except Exception as e:
            print(f"❌ Insert failed for session {session_key}: {e}")

    print(f"🎯 Done. Total new rows inserted: {total_inserted}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    ingest_intervals()