import requests
import snowflake.connector
import pandas as pd


def ingest_drivers():
    print("🚀 Starting driver ingestion (incremental)...")

    # Step 1: Fetch data from API
    url = "https://api.openf1.org/v1/drivers"
    try:
        response = requests.get(url, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"❌ API request error: {e}")
        return

    print(f"🌐 API status code: {response.status_code}")

    if response.status_code != 200:
        print(f"❌ API request failed. Status: {response.status_code}")
        print("Response text (first 500 chars):", response.text[:500])
        return

    try:
        data = response.json()
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")
        print("Response text (first 500 chars):", response.text[:500])
        return

    if not isinstance(data, list):
        print("❌ Unexpected API response type")
        return

    df = pd.DataFrame(data)
    print(f"📦 Retrieved {len(df)} driver records from API")

    expected_columns = [
        "driver_number",
        "session_key",
        "broadcast_name",
        "full_name",
        "name_acronym",
        "team_name",
        "team_colour",
        "first_name",
        "last_name",
        "headshot_url",
        "country_code"
    ]

    # Add missing columns as None
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Keep only expected columns in correct order
    df = df[expected_columns]

    # Step 2: Connect to Snowflake
    try:
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
    except Exception as e:
        print(f"❌ Snowflake connection error: {e}")
        return

    # Step 3: Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            driver_number INT,
            session_key INT,
            broadcast_name STRING,
            full_name STRING,
            name_acronym STRING,
            team_name STRING,
            team_colour STRING,
            first_name STRING,
            last_name STRING,
            headshot_url STRING,
            country_code STRING
        );
    """)

    # Step 4: Get existing driver_numbers
    cur.execute("SELECT driver_number FROM drivers")
    existing_driver_numbers = {row[0] for row in cur.fetchall() if row[0] is not None}

    # Step 5: Filter only new drivers
    new_df = df[~df["driver_number"].isin(existing_driver_numbers)]

    if new_df.empty:
        print("⏭️ No new drivers to insert.")
    else:
        insert_query = """
            INSERT INTO drivers (
                driver_number, session_key, broadcast_name, full_name, name_acronym, 
                team_name, team_colour, first_name, last_name, headshot_url, country_code
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in new_df.iterrows()]
        cur.executemany(insert_query, records)
        print(f"✅ Inserted {len(new_df)} new driver records into Snowflake")

    cur.close()
    conn.close()


if __name__ == "__main__":
    ingest_drivers()
