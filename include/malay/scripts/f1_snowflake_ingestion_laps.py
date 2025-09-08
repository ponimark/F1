import requests
import snowflake.connector
import pandas as pd

def ingest_laps():
    print("🏁 Starting incremental lap data ingestion...")

    # Step 1: Get all Race + Qualifying sessions
    session_url = "https://api.openf1.org/v1/sessions"
    sessions_response = requests.get(session_url)
    all_sessions = sessions_response.json()
    target_sessions = [s for s in all_sessions if s.get("session_type") in ["Race", "Qualifying"]]
    print(f"🔍 Found {len(target_sessions)} Race/Qualifying sessions")

    # Step 2: Connect to Snowflake
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

    # Step 3: Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS laps (
            meeting_key INT,
            session_key INT,
            driver_number INT,
            lap_number INT,
            date_start TIMESTAMP_TZ,
            duration_sector_1 FLOAT,
            duration_sector_2 FLOAT,
            duration_sector_3 FLOAT,
            lap_duration FLOAT,
            is_pit_out_lap BOOLEAN,
            i1_speed INT,
            i2_speed INT,
            st_speed INT
        );
    """)

    # Step 4: Get already ingested session_keys
    cursor.execute("SELECT DISTINCT session_key FROM laps")
    existing_sessions = {row[0] for row in cursor.fetchall() if row[0] is not None}
    print(f"📂 Found {len(existing_sessions)} sessions already in Snowflake")

    # Step 5: Process only missing sessions
    total_records = 0
    for session in target_sessions:
        session_key = session["session_key"]

        if session_key in existing_sessions:
            print(f"⏭️ Skipping session {session_key} (already loaded)")
            continue

        print(f"📥 Fetching laps for new session_key: {session_key} ({session['session_type']})")

        lap_url = f"https://api.openf1.org/v1/laps?session_key={session_key}"
        try:
            data = requests.get(lap_url, timeout=30).json()
        except Exception as e:
            print(f"⚠️ Failed to fetch laps for session {session_key}: {e}")
            continue

        if not data:
            print(f"⚠️ No laps found for session_key {session_key}")
            continue

        df = pd.DataFrame(data)

        selected_columns = [
            "meeting_key", "session_key", "driver_number", "lap_number",
            "date_start", "duration_sector_1", "duration_sector_2",
            "duration_sector_3", "lap_duration", "is_pit_out_lap",
            "i1_speed", "i2_speed", "st_speed"
        ]
        for col in selected_columns:
            if col not in df.columns:
                df[col] = None

        df = df[selected_columns]

        # Replace NaNs with None
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]

        if not records:
            print(f"⚠️ No valid records for session {session_key}")
            continue

        cursor.executemany(f"""
            INSERT INTO laps (
                meeting_key, session_key, driver_number, lap_number,
                date_start, duration_sector_1, duration_sector_2,
                duration_sector_3, lap_duration, is_pit_out_lap,
                i1_speed, i2_speed, st_speed
            ) VALUES ({','.join(['%s']*13)})
        """, records)

        conn.commit()
        total_records += len(records)
        print(f"✅ Inserted {len(records)} records for session {session_key}")

    print(f"🎉 Total new laps inserted: {total_records}")
    cursor.close()
    conn.close()
