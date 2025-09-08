import requests
import snowflake.connector
import pandas as pd
import os

PROGRESS_FILE = "processed_location_pairs.csv"

# List of sessions to skip completely (all drivers already done)
SKIP_SESSIONS = {
    9204, 9531, 9594, 9668, 9074, 9570, 9502, 9145, 9549, 9215, 9554,
    9220, 9189, 9193, 9545, 9106, 9314, 9129, 9498, 7768, 9586, 9590,
    9468, 9541, 9304, 9177, 9527, 7775, 7783, 9519, 9286, 9117, 9562,
    9069, 9153, 9308, 9492, 9672, 9664, 9578, 9511, 9294, 9064, 9278,
    9161, 9476, 9169, 9298, 9602, 9140, 9535, 9282, 9122, 9506, 9207,
    9484, 9574, 9090, 9212, 9098, 9112, 9135
}

def load_processed_pairs():
    if not os.path.exists(PROGRESS_FILE) or os.stat(PROGRESS_FILE).st_size == 0:
        return set()
    df = pd.read_csv(PROGRESS_FILE)
    return set(tuple(x) for x in df.values)

def save_processed_pair(session_key, driver_number):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"{session_key},{driver_number}\n")

def ingest_location():
    print("🌍 Starting location data ingestion with resume support...")

    # Step 1: Fetch sessions
    sessions = requests.get("https://api.openf1.org/v1/sessions").json()
    target_sessions = [
        s for s in sessions
        if s.get("session_type") in ["Race", "Qualifying"]
        and s.get("session_key") not in SKIP_SESSIONS
    ]
    print(f"🔍 Found {len(target_sessions)} sessions to process (skipping {len(SKIP_SESSIONS)} already done)")

    # Step 2: Load processed pairs
    processed_pairs = load_processed_pairs()
    print(f"📂 Loaded {len(processed_pairs)} processed session/driver pairs from progress file")

    # Step 3: Connect to Snowflake
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

    # Step 4: Ensure table exists
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

    # Counters
    inserted_rows = 0
    skipped_rows = 0
    failed_pairs = 0
    attempted_pairs = 0

    # Step 5: Process each session
    for session in target_sessions:
        session_key = session["session_key"]

        # Fetch drivers for this session
        try:
            drivers = requests.get(f"https://api.openf1.org/v1/drivers?session_key={session_key}").json()
        except Exception as e:
            print(f"❌ Failed to fetch drivers for session {session_key}: {e}")
            failed_pairs += 1
            continue

        if not drivers:
            print(f"⚠️ No drivers found for session {session_key}")
            continue

        for driver in drivers:
            driver_number = driver.get("driver_number")
            if driver_number is None:
                continue

            if (session_key, driver_number) in processed_pairs:
                print(f"⏩ Skipping already processed session {session_key}, driver {driver_number}")
                continue

            attempted_pairs += 1
            url = f"https://api.openf1.org/v1/location?session_key={session_key}&driver_number={driver_number}"
            print(f"\n📥 [{attempted_pairs}] Fetching {url}")

            try:
                data = requests.get(url, timeout=30).json()
            except Exception as e:
                print(f"⚠️ Request failed for session {session_key}, driver {driver_number}: {e}")
                failed_pairs += 1
                continue

            if not data:
                print(f"⚠️ No location data for driver {driver_number} in session {session_key}")
                skipped_rows += 1
                save_processed_pair(session_key, driver_number)
                continue

            if isinstance(data, dict):
                data = [data]

            required_cols = ["driver_number", "session_key", "date", "x", "y", "z"]
            df = pd.DataFrame(data)

            if not all(col in df.columns for col in required_cols):
                print(f"⚠️ Missing required fields for driver {driver_number} in session {session_key}")
                skipped_rows += 1
                save_processed_pair(session_key, driver_number)
                continue

            df = df[required_cols]
            before_count = len(df)
            df = df.drop_duplicates(subset=["driver_number", "session_key", "x", "y", "z"])
            after_count = len(df)

            if after_count < before_count:
                print(f"ℹ️ Removed {before_count - after_count} duplicates (ignoring date) for driver {driver_number}")

            records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]
            if not records:
                skipped_rows += 1
                save_processed_pair(session_key, driver_number)
                continue

            try:
                cursor.executemany("""
                    INSERT INTO location (
                        driver_number, session_key, date, x, y, z
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, records)
                conn.commit()
                inserted_rows += len(records)
                print(f"✅ Inserted {len(records)} rows (session {session_key}, driver {driver_number}) | Total so far: {inserted_rows}")
                save_processed_pair(session_key, driver_number)
            except Exception as e:
                print(f"❌ Insert failed for session {session_key}, driver {driver_number}: {e}")
                failed_pairs += 1

    print("\n🏁 Location backfill complete!")
    print(f"📊 Pairs attempted: {attempted_pairs}")
    print(f"✅ Total rows inserted: {inserted_rows}")
    print(f"⚠️ Skipped: {skipped_rows}")
    print(f"❌ Failed: {failed_pairs}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    ingest_location()
