import requests
import snowflake.connector
import pandas as pd
import json

# Optional: limit sessions during testing
SESSION_LIMIT = None  # e.g., 20

def ingest_weather():
    print("🌦️ Starting incremental weather ingestion...")

    # --- 1) Fetch sessions ---
    try:
        sessions = requests.get("https://api.openf1.org/v1/sessions", timeout=30).json()
    except Exception as e:
        print(f"❌ Failed to fetch /sessions: {e}")
        return

    if isinstance(sessions, dict):
        if "detail" in sessions:
            print(f"⚠️ API error from /sessions: {sessions.get('detail')}")
            return
        sessions = [sessions]
    if not isinstance(sessions, list):
        print(f"❌ /sessions returned {type(sessions)}; expected list")
        return

    # Filter by session_name keywords
    NAME_ALLOW = ("Qualifying", "Race", "Sprint", "Sprint Shootout")
    sessions = [
        s for s in sessions
        if s.get("session_key") and any(tok in s.get("session_name", "") for tok in NAME_ALLOW)
    ]
    if SESSION_LIMIT:
        sessions = sessions[:SESSION_LIMIT]

    print(f"🧩 Sessions to consider: {len(sessions)}")

    # --- 2) Connect to Snowflake ---
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

    # --- 3) Ensure table exists ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weathers (
            session_key INT,
            date TIMESTAMP_TZ,
            air_temperature FLOAT,
            track_temperature FLOAT,
            humidity INT,
            wind_speed FLOAT,
            rainfall FLOAT
        );
    """)

    # --- 4) Get existing sessions ---
    cur.execute("SELECT DISTINCT session_key FROM weathers")
    existing_sessions = {r[0] for r in cur.fetchall() if r[0] is not None}
    print(f"📂 {len(existing_sessions)} sessions already in weather")

    expected_cols = [
        "session_key", "date", "air_temperature",
        "track_temperature", "humidity", "wind_speed", "rainfall"
    ]

    insert_sql = """
        INSERT INTO weathers (
            session_key, date, air_temperature,
            track_temperature, humidity, wind_speed, rainfall
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    total_inserted = 0

    # --- 5) Loop sessions and ingest only new ones ---
    for s in sessions:
        session_key = s["session_key"]
        if session_key in existing_sessions:
            print(f"⏭️ Skipping session {session_key} (already loaded)")
            continue

        print(f"📥 Fetching weather for NEW session {session_key}")
        try:
            resp = requests.get(
                f"https://api.openf1.org/v1/weather?session_key={session_key}",
                timeout=30
            )
        except Exception as e:
            print(f"⚠️ Request error for session {session_key}: {e}")
            continue

        try:
            data = resp.json()
        except json.JSONDecodeError:
            print(f"⚠️ Non-JSON response for session {session_key}; skipping")
            continue

        if isinstance(data, dict):
            if "detail" in data:
                print(f"⚠️ API error for session {session_key}: {data.get('detail')}")
                continue
            data = [data]
        if not data or not isinstance(data, list):
            print(f"⚠️ No/invalid weather payload for session {session_key}")
            continue

        df = pd.DataFrame(data)
        if df.empty:
            print(f"⚠️ Empty weather payload for session {session_key}")
            continue

        # Ensure required columns exist
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]

        # De-duplicate rows by (session_key, date)
        df = df.drop_duplicates(subset=["session_key", "date"])
        if df.empty:
            print(f"⚠️ Nothing to insert for session {session_key}")
            continue

        # NaN → None
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]
        if not records:
            print(f"⚠️ No records to insert for session {session_key}")
            continue

        try:
            cur.executemany(insert_sql, records)
            conn.commit()
            total_inserted += len(records)
            print(f"✅ Inserted {len(records)} rows for session {session_key} | Total: {total_inserted}")
        except Exception as e:
            print(f"❌ Insert failed for session {session_key}: {e}")

    print(f"🎉 Weather ingestion complete. Total NEW rows inserted: {total_inserted}")

    try:
        cur.close()
        conn.close()
    except Exception:
        pass

if __name__ == "__main__":
    ingest_weather()
