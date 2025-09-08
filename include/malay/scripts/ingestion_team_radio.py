import requests
import snowflake.connector
import pandas as pd

def ingest_team_radio():
    print("📡 Starting incremental team radio ingestion...")

    # 1) Fetch sessions (Race + Qualifying)
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
        CREATE TABLE IF NOT EXISTS team_radio (
            driver_number INT,
            session_key INT,
            date TIMESTAMP_TZ,
            recording_url STRING
        );
    """)

    # 4) Load existing keys to skip duplicates
    #    Using triples gives fine-grained skipping even if a session is partially loaded.
    cursor.execute("""
        SELECT DISTINCT session_key, driver_number, recording_url
        FROM team_radio
    """)
    existing_triples = {(r[0], r[1], r[2]) for r in cursor.fetchall()}
    print(f"📂 {len(existing_triples)} existing (session, driver, url) triples")

    required_cols = ["driver_number", "session_key", "date", "recording_url"]
    total_inserted = 0

    # 5) Process sessions
    for s in target_sessions:
        session_key = s["session_key"]
        print(f"📥 Fetching team radio for session {session_key}")

        try:
            payload = requests.get(
                f"https://api.openf1.org/v1/team_radio?session_key={session_key}",
                timeout=45
            ).json()
        except Exception as e:
            print(f"❌ Failed to fetch team_radio for session {session_key}: {e}")
            continue

        # Handle API error dicts like {"detail": "..."}
        if isinstance(payload, dict):
            if "detail" in payload:
                print(f"⚠️ API error for session {session_key}: {payload.get('detail')}")
                continue
            payload = [payload]

        if not payload or not isinstance(payload, list):
            print(f"⚠️ No/invalid team_radio payload for session {session_key}")
            continue

        df = pd.DataFrame(payload)
        if df.empty:
            print(f"⚠️ Empty team_radio payload for session {session_key}")
            continue

        # Ensure required columns exist & order
        for c in required_cols:
            if c not in df.columns:
                df[c] = None
        df = df[required_cols]

        # 🧹 Deduplicate in-payload ignoring date
        before = len(df)
        df = df.drop_duplicates(subset=["driver_number", "session_key", "recording_url"])
        removed = before - len(df)
        if removed:
            print(f"ℹ️ Removed {removed} dups in payload (ignoring date) for session {session_key}")

        if df.empty:
            print(f"⚠️ Nothing to insert after payload dedup for session {session_key}")
            continue

        # 🔎 Incremental skip: remove rows already present in Snowflake
        mask_new = ~df.apply(
            lambda r: (r["session_key"], r["driver_number"], r["recording_url"]) in existing_triples,
            axis=1
        )
        df_new = df[mask_new]

        if df_new.empty:
            print(f"⏭️ All team_radio rows for session {session_key} already loaded")
            continue

        # Prepare records (NaN → None)
        records = [tuple(row.where(pd.notnull(row), None).values) for _, row in df_new.iterrows()]
        try:
            cursor.executemany("""
                INSERT INTO team_radio (
                    driver_number, session_key, date, recording_url
                ) VALUES (%s, %s, %s, %s)
            """, records)
            conn.commit()
            total_inserted += len(records)
            print(f"✅ Inserted {len(records)} new rows for session {session_key} | Total: {total_inserted}")
            # Update in-memory set so subsequent sessions skip new duplicates
            for r in records:
                existing_triples.add((r[1], r[0], r[3]))  # (session_key, driver_number, recording_url)
        except Exception as e:
            print(f"❌ Insert failed for session {session_key}: {e}")

    print(f"🎉 Total new team_radio rows inserted: {total_inserted}")
    cursor.close()
    conn.close()
