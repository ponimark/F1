import requests
import snowflake.connector
import pandas as pd

def ingest_location():
    print("🌍 Starting incremental location data ingestion...")

    # Step 1: Fetch relevant sessions
    sessions = requests.get("https://api.openf1.org/v1/sessions").json()
    target_sessions = [s for s in sessions if s.get("session_type") in ["Race", "Qualifying"]]
    print(f"🔍 Found {len(target_sessions)} sessions")

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
        CREATE TABLE IF NOT EXISTS location (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            x FLOAT,
            y FLOAT,
            z FLOAT
        );
    """)

    # Step 4: Get existing session+driver combos
    cursor.execute("SELECT DISTINCT session_key, driver_number FROM location")
    existing_pairs = {(row[0], row[1]) for row in cursor.fetchall()}
    print(f"📂 Found {len(existing_pairs)} session/driver pairs already loaded")

    total_inserted = 0

    # Step 5: Process each session
    for session in target_sessions:
        session_key = session["session_key"]

        # Fetch drivers for this session
        drivers_url = f"https://api.openf1.org/v1/drivers?session_key={session_key}"
        try:
            drivers = requests.get(drivers_url, timeout=30).json()
        except Exception as e:
            print(f"❌ Failed to fetch drivers for session {session_key}: {e}")
            continue

        if not drivers or not isinstance(drivers, list):
            print(f"⚠️ No valid driver list for session {session_key}")
            continue

        # Step 6: Fetch location data for each driver not yet loaded
        for driver in drivers:
            driver_number = driver.get("driver_number")
            if driver_number is None:
                continue

            if (session_key, driver_number) in existing_pairs:
                print(f"⏭️ Skipping session {session_key}, driver {driver_number} (already loaded)")
                continue

            url = f"https://api.openf1.org/v1/location?session_key={session_key}&driver_number={driver_number}"
            try:
                data = requests.get(url, timeout=30).json()
            except Exception as e:
                print(f"❌ Failed to fetch location for driver {driver_number} in session {session_key}: {e}")
                continue

            if not data:
                print(f"⚠️ No location data for driver {driver_number} in session {session_key}")
                continue

            if isinstance(data, dict):
                data = [data]

            df = pd.DataFrame(data)
            required_cols = ["driver_number", "session_key", "date", "x", "y", "z"]

            if not all(col in df.columns for col in required_cols):
                print(f"⚠️ Missing required fields in driver {driver_number} for session {session_key}, skipping.")
                continue

            df = df[required_cols]

            # Remove duplicates ignoring `date`
            before_count = len(df)
            df = df.drop_duplicates(subset=["driver_number", "session_key", "x", "y", "z"])
            after_count = len(df)

            if after_count < before_count:
                print(f"ℹ️ Removed {before_count - after_count} duplicate rows for driver {driver_number}")

            records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]
            if not records:
                continue

            cursor.executemany("""
                INSERT INTO location (
                    driver_number, session_key, date, x, y, z
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, records)

            total_inserted += len(records)
            print(f"✅ Inserted {len(df)} rows for driver {driver_number} in session {session_key}")

    print(f"🎉 Total new location records inserted: {total_inserted}")
    cursor.close()
    conn.close()
