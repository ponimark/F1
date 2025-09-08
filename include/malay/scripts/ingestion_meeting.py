import requests
import snowflake.connector
import pandas as pd
import json

def ingest_meetings():
    print("📅 Starting meeting ingestion...")

    # Step 1: Fetch data from API
    try:
        data = requests.get("https://api.openf1.org/v1/meetings", timeout=30).json()
    except Exception as e:
        print(f"❌ Failed to fetch /meetings: {e}")
        return

    if isinstance(data, dict) and "detail" in data:
        print(f"⚠️ API error: {data['detail']}")
        return
    if not isinstance(data, list):
        print(f"❌ Unexpected type: {type(data)}")
        return

    df = pd.DataFrame(data)
    if df.empty:
        print("⚠️ No meeting data found.")
        return

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
    cur = conn.cursor()
    print("🔐 Connected to Snowflake")

    # Step 3: Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meetings (
            meeting_key INT,
            circuit_key INT,
            circuit_short_name STRING,
            meeting_code STRING,
            location STRING,
            country_key INT,
            country_code STRING,
            country_name STRING,
            meeting_name STRING,
            meeting_official_name STRING,
            gmt_offset STRING,
            date_start TIMESTAMP_TZ,
            year INT
        );
    """)

    # Step 4: Filter already ingested meetings
    cur.execute("SELECT DISTINCT meeting_key FROM meetings")
    existing_keys = {row[0] for row in cur.fetchall()}
    df = df[~df['meeting_key'].isin(existing_keys)]
    if df.empty:
        print("✅ No new meetings to insert.")
        return

    # Step 5: Prepare for insertion
    expected_cols = [
        "meeting_key", "circuit_key", "circuit_short_name", "meeting_code",
        "location", "country_key", "country_code", "country_name",
        "meeting_name", "meeting_official_name", "gmt_offset",
        "date_start", "year"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    df = df[expected_cols]

    # Replace NaNs with None
    records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df.iterrows()]

    insert_sql = """
        INSERT INTO meetings (
            meeting_key, circuit_key, circuit_short_name, meeting_code,
            location, country_key, country_code, country_name,
            meeting_name, meeting_official_name, gmt_offset,
            date_start, year
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cur.executemany(insert_sql, records)
        conn.commit()
        print(f"✅ Inserted {len(records)} meeting records.")
    except Exception as e:
        print(f"❌ Insert failed: {e}")

    cur.close()
    conn.close()
    print("🎯 Meeting ingestion completed.")

if __name__ == "__main__":
    ingest_meetings()
