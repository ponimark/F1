import requests
import snowflake.connector
import pandas as pd

def ingest_pit():
    print("🛠️ Starting incremental pit stop ingestion...")

    # 1) Sessions (Race + Quali)
    try:
        all_sessions = requests.get("https://api.openf1.org/v1/sessions", timeout=30).json()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch sessions: {e}")
    target_sessions = [s for s in all_sessions if s.get("session_type") in ["Race", "Qualifying"] and s.get("session_key")]
    print(f"🔍 Found {len(target_sessions)} relevant sessions")

    # 2) Snowflake
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

    # 3) Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pit (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            lap_number INT,
            pit_duration FLOAT
        );
    """)

    # 4) Existing sessions → skip already loaded
    cursor.execute("SELECT DISTINCT session_key FROM pit")
    existing_sessions = {row[0] for row in cursor.fetchall() if row[0] is not None}
    print(f"📂 {len(existing_sessions)} sessions already loaded in pit")

    required_cols = ["driver_number", "session_key", "date", "lap_number", "pit_duration"]
    total_inserted = 0

    # 5) Loop sessions
    for s in target_sessions:
        session_key = s["session_key"]
        if session_key in existing_sessions:
            print(f"⏭️ Skipping session {session_key} (already loaded)")
            continue

        print(f"📥 Fetching pit data for NEW session {session_key}")
        try:
            payload = requests.get(f"https://api.openf1.org/v1/pit?session_key={session_key}", timeout=30).json()
        except Exception as e:
            print(f"❌ Failed to fetch pit data for session {session_key}: {e}")
            continue

        # Handle API error dicts like {"detail": "..."}
        if isinstance(payload, dict):
            if "detail" in payload:
                print(f"⚠️ API error for session {session_key}: {payload.get('detail')}")
                continue
            payload = [payload]

        if not payload or not isinstance(payload, list):
            print(f"⚠️ No pit data or unexpected payload for session {session_key}")
            continue

        df = pd.DataFrame(payload)
        if df.empty:
            print(f"⚠️ Empty pit payload for session {session_key}")
            continue

        # Ensure columns exist
        for c in required_cols:
            if c not in df.columns:
                df[c] = None
        df = df[required_cols]

        # 🧹 Dedup (ignore date)
        before = len(df)
        df = df.drop_duplicates(subset=["driver_number", "session_key", "lap_number", "pit_duration"])
        removed = before - len(df)
        if removed:
            print(f"ℹ️ Removed {removed} dup rows (ignoring date) for session {session_key}")

        if df.empty:
            print(f"⚠️ Nothing to insert for session {session_key}")
            continue

        # NaN → None
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]

        try:
            cursor.executemany("""
                INSERT INTO pit (
                    driver_number, session_key, date, lap_number, pit_duration
                ) VALUES (%s, %s, %s, %s, %s)
            """, records)
            conn.commit()
            total_inserted += len(records)
            print(f"✅ Inserted {len(records)} rows for session {session_key} | Total: {total_inserted}")
        except Exception as e:
            print(f"❌ Insert failed for session {session_key}: {e}")

    print(f"🎉 Total new pit records inserted: {total_inserted}")
    cursor.close()
    conn.close()
