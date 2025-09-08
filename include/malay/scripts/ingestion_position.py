import requests
import snowflake.connector
import pandas as pd

def ingest_position():
    print("📊 Starting incremental driver position ingestion...")

    # 1) Get sessions (Race + Qualifying)
    try:
        all_sessions = requests.get("https://api.openf1.org/v1/sessions", timeout=30).json()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch sessions: {e}")

    target_sessions = [
        s for s in all_sessions
        if s.get("session_type") in ["Race", "Qualifying"] and s.get("session_key")
    ]
    print(f"🔍 Found {len(target_sessions)} relevant sessions")

    # 2) Snowflake connect
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
        CREATE TABLE IF NOT EXISTS position (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            position INT
        );
    """)

    # 4) Skip sessions already loaded
    cursor.execute("SELECT DISTINCT session_key FROM position")
    existing_sessions = {r[0] for r in cursor.fetchall() if r[0] is not None}
    print(f"📂 {len(existing_sessions)} sessions already present")

    required_cols = ["driver_number", "session_key", "date", "position"]
    total_inserted = 0

    # 5) Loop new sessions only
    for s in target_sessions:
        session_key = s["session_key"]
        if session_key in existing_sessions:
            print(f"⏭️ Skipping session {session_key} (already loaded)")
            continue

        print(f"📥 Fetching position data for NEW session {session_key}")
        try:
            payload = requests.get(
                f"https://api.openf1.org/v1/position?session_key={session_key}",
                timeout=30
            ).json()
        except Exception as e:
            print(f"❌ Failed to fetch position data for session {session_key}: {e}")
            continue

        # Handle API error dicts like {"detail": "..."}
        if isinstance(payload, dict):
            if "detail" in payload:
                print(f"⚠️ API error for session {session_key}: {payload.get('detail')}")
                continue
            payload = [payload]

        if not payload or not isinstance(payload, list):
            print(f"⚠️ No/invalid position payload for session {session_key}")
            continue

        df = pd.DataFrame(payload)
        if df.empty:
            print(f"⚠️ Empty position payload for session {session_key}")
            continue

        # Ensure columns exist
        for c in required_cols:
            if c not in df.columns:
                df[c] = None
        df = df[required_cols]


        # NaN → None
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]

        try:
            cursor.executemany("""
                INSERT INTO position (driver_number, session_key, date, position)
                VALUES (%s, %s, %s, %s)
            """, records)
            conn.commit()
            total_inserted += len(records)
            print(f"✅ Inserted {len(records)} rows for session {session_key} | Total: {total_inserted}")
        except Exception as e:
            print(f"❌ Insert failed for session {session_key}: {e}")

    print(f"🎉 Total new position rows inserted: {total_inserted}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    ingest_position()