import requests
import snowflake.connector
import pandas as pd

def ingest_race_control():
    print("🚩 Starting incremental race control ingestion...")

    # 1) Get sessions (Race + Qualifying)
    try:
        sessions = requests.get("https://api.openf1.org/v1/sessions", timeout=30).json()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch sessions: {e}")

    target_sessions = [
        s for s in sessions
        if s.get("session_type") in ["Race", "Qualifying"] and s.get("session_key")
    ]
    print(f"🔍 Found {len(target_sessions)} target sessions")

    # 2) Snowflake connection
    conn = snowflake.connector.connect(
        user='MARKPONI',
        password='ZXCVB12345zxcvb',
        account='zczvngs-ti82223',
        warehouse='F1_WH',
        database='F1',
        schema='poni',
        role='ACCOUNTADMIN'
    )
    cursor = conn.cursor()
    print("🔐 Connected to Snowflake")

    # 3) Ensure table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS race_control (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            category STRING,
            flag STRING,
            message STRING,
            scope STRING
        );
    """)

    # 4) Skip sessions already loaded
    cursor.execute("SELECT DISTINCT session_key FROM race_control")
    existing_sessions = {r[0] for r in cursor.fetchall() if r[0] is not None}
    print(f"📂 {len(existing_sessions)} sessions already present")

    required_cols = ["driver_number", "session_key", "date", "category", "flag", "message", "scope"]
    total = 0

    # 5) Loop only NEW sessions
    for s in target_sessions:
        session_key = s["session_key"]
        if session_key in existing_sessions:
            print(f"⏭️ Skipping session {session_key} (already loaded)")
            continue

        print(f"📥 Fetching race_control for NEW session {session_key}")
        try:
            payload = requests.get(
                f"https://api.openf1.org/v1/race_control?session_key={session_key}",
                timeout=30
            ).json()
        except Exception as e:
            print(f"❌ Failed to fetch race control for session {session_key}: {e}")
            continue

        # Handle API error dicts like {"detail": "..."}
        if isinstance(payload, dict):
            if "detail" in payload:
                print(f"⚠️ API error for session {session_key}: {payload.get('detail')}")
                continue
            payload = [payload]

        if not payload or not isinstance(payload, list):
            print(f"⚠️ No/invalid race control payload for session {session_key}")
            continue

        df = pd.DataFrame(payload)
        if df.empty:
            print(f"⚠️ Empty race control payload for session {session_key}")
            continue

        # Ensure columns exist & order them
        for c in required_cols:
            if c not in df.columns:
                df[c] = None
        df = df[required_cols]

        # 🧹 Dedup ignoring `date`
        before = len(df)
        df = df.drop_duplicates(subset=["driver_number", "session_key", "category", "flag", "message", "scope"])
        removed = before - len(df)
        if removed:
            print(f"ℹ️ Removed {removed} duplicate rows (ignoring date) for session {session_key}")

        if df.empty:
            print(f"⚠️ Nothing to insert for session {session_key}")
            continue

        # NaN → None for Snowflake
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]

        try:
            cursor.executemany("""
                INSERT INTO race_control (
                    driver_number, session_key, date, category, flag, message, scope
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, records)
            conn.commit()
            total += len(records)
            print(f"✅ Inserted {len(records)} rows for session {session_key} | Total: {total}")
        except Exception as e:
            print(f"❌ Insert failed for session {session_key}: {e}")

    print(f"🎉 Total new race control records inserted: {total}")
    cursor.close()
    conn.close()
