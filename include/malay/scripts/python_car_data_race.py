import requests
import snowflake.connector
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

PROGRESS_FILE = "processed_pairs.csv"
CHUNK_HOURS = 1  # each API call will cover 1 hour of data

def load_processed_pairs():
    if os.path.exists(PROGRESS_FILE):
        df = pd.read_csv(PROGRESS_FILE)
        return set(tuple(x) for x in df.values)
    return set()

def save_processed_pair(session_key, driver_number):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"{session_key},{driver_number}\n")

def chunk_timerange(start, end, hours=1):
    """Yield (chunk_start, chunk_end) datetimes for given range."""
    chunks = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(hours=hours), end)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks

def ingest_car_data():
    print("🚗 Starting car telemetry ingestion (chunked by 1 hour)")

    # Fetch sessions
    sessions = requests.get("https://api.openf1.org/v1/sessions").json()
    sessions = [
        s for s in sessions
        if s.get("session_type") == "Race"
        and s.get("session_key")
        and s.get("date_start") and s.get("date_end")
    ]

    # Fetch drivers
    drivers = requests.get("https://api.openf1.org/v1/drivers").json()
    driver_numbers = sorted({d.get("driver_number") for d in drivers if d.get("driver_number") is not None})

    print(f"🧩 Sessions: {len(sessions)} | Drivers: {len(driver_numbers)}")
    total_pairs = len(sessions) * len(driver_numbers)
    print(f"🔁 Total combinations: {total_pairs}")

    processed_pairs = load_processed_pairs()
    print(f"📂 Already processed: {len(processed_pairs)}")

    # Snowflake connection
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS car_data (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            speed INT,
            rpm INT,
            throttle INT,
            brake INT,
            n_gear INT,
            drs INT
        );
    """)

    expected_cols = ["driver_number","session_key","date","speed","rpm","throttle","brake","n_gear","drs"]
    insert_sql = """
        INSERT INTO car_data (
            driver_number, session_key, date, speed, rpm, throttle, brake, n_gear, drs
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted_rows = 0

    # Main loop
    for s in sessions:
        session_key = s["session_key"]
        start = datetime.fromisoformat(s["date_start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(s["date_end"].replace("Z", "+00:00"))
        chunks = chunk_timerange(start, end, CHUNK_HOURS)

        for driver_number in driver_numbers:
            if (session_key, driver_number) in processed_pairs:
                print(f"⏩ Skipping session {session_key}, driver {driver_number}")
                continue

            print(f"\n🚦 Processing session {session_key}, driver {driver_number} ({len(chunks)} chunks)")

            for chunk_start, chunk_end in chunks:
                start_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                url = f"https://api.openf1.org/v1/car_data?session_key={session_key}&driver_number={driver_number}&date>={start_str}&date<{end_str}"

                print(f"   📥 Fetching chunk {start_str} → {end_str}")
                try:
                    payload = requests.get(url, timeout=30).json()
                except Exception as e:
                    print(f"   ⚠️ Request failed: {e}")
                    continue

                if not payload or (isinstance(payload, dict) and "detail" in payload):
                    print("   ⚠️ No data for this chunk")
                    continue

                if isinstance(payload, dict):
                    payload = [payload]

                df = pd.DataFrame(payload)
                if df.empty:
                    print("   ⚠️ Empty DataFrame")
                    continue

                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None
                df = df[expected_cols]

                records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]
                if not records:
                    print("   ⚠️ No valid records after cleanup")
                    continue

                try:
                    cur.executemany(insert_sql, records)
                    conn.commit()
                    inserted_rows += len(records)
                    print(f"   ✅ Inserted {len(records)} rows | Total so far: {inserted_rows}")
                except Exception as e:
                    print(f"   ❌ Insert failed: {e}")

            save_processed_pair(session_key, driver_number)

    print(f"\n🏁 Finished! Total rows inserted: {inserted_rows}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    ingest_car_data()
