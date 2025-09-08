from include.malay.scripts.f1_snowflake_ingestion_drivers import ingest_drivers
from include.malay.scripts.f1_snowflake_ingestion_laps import ingest_laps
from include.malay.scripts.ingestion_pit import ingest_pit
from include.malay.scripts.ingestion_position import ingest_position
from include.malay.scripts.ingestion_car_data import ingest_car_data_incremental
from include.malay.scripts.ingestion_intervals import ingest_intervals
from include.malay.scripts.ingestion_stints import ingest_stints
from include.malay.scripts.ingestion_weather import ingest_weather
from include.malay.scripts.ingestion_race_control import ingest_race_control
from include.malay.scripts.ingestion_team_radio import ingest_team_radio
from include.malay.scripts.ingestion_location import ingest_location
from include.malay.scripts.ingestion_sessions import ingest_sessions
from include.malay.scripts.ingestion_meeting import ingest_meetings


def ingest_drivers_to_snowflake():
    print("✅ Snowflake ingestion DAG started")
    ingest_drivers()
    print("🏁 Task completed successfully!")


def ingest_laps_to_snowflake():
    print("🏎️ Lap ingestion task started...")
    ingest_laps()
    print("✅ Lap ingestion task completed successfully!")


def ingest_pits_to_snowflake():
    print("🏎️ Pit ingestion task started...")
    ingest_pit()
    print("✅ Pit ingestion task completed successfully!")

def ingest_positions_to_snowflake():
    print("🏎️ Position ingestion task started...")
    ingest_position()
    print("✅ Position ingestion task completed successfully!")

def ingest_car_data_incremental_to_snowflake():
    print("🏎️ Car Data ingestion task started...")
    ingest_car_data_incremental()
    print("✅ Car Data ingestion task completed successfully!")

def ingest_intervals_to_snowflake():
    print("🏎️ Intervals ingestion task started...")
    ingest_intervals()
    print("✅ Intervals ingestion task completed successfully!")

def ingest_stints_to_snowflake():
    print("🏎️ Stints ingestion task started...")
    ingest_stints()
    print("✅ Stints ingestion task completed successfully!")

def ingest_weather_to_snowflake():
    print("🏎️ weather ingestion task started...")
    ingest_weather()
    print("✅ weather ingestion task completed successfully!")

def ingest_race_control_to_snowflake():
    print("🏎️ race control ingestion task started...")
    ingest_race_control()
    print("✅ race control ingestion task completed successfully!")

def ingest_team_radio_to_snowflake():
    print("🏎️ Team Radio ingestion task started...")
    ingest_team_radio()
    print("✅ Team Radio ingestion task completed successfully!")

def ingest_location_to_snowflake():
    print("🏎️ Location ingestion task started...")
    ingest_location()
    print("✅ Location ingestion task completed successfully!")

def ingest_sessions_to_snowflake():
    print("🏎️ Session ingestion task started...")
    ingest_sessions()
    print("✅ Session ingestion task completed successfully!")

def ingest_Meetings_to_snowflake():
    print("✅ Meetings ingestion DAG started")
    ingest_meetings()
    print("🏁 Meetings Task completed successfully!")









