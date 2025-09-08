
  create view "f1"."f1_data"."int_tyre_stint__dbt_tmp"
    
    
  as (
    

WITH stints_base AS (
    SELECT
        s.*,
        d.team_name,
        ses.circuit_short_name,
        ses.country_name,
        ses.session_type
    FROM "f1"."f1_data"."stg_stints" s
    LEFT JOIN "f1"."f1_data"."stg_drivers" d ON s.driver_number = d.driver_number
        AND s.session_key = d.session_key
    LEFT JOIN "f1"."f1_data"."stg_sessions" ses ON s.session_key = ses.session_key
    WHERE ses.session_type = 'Race' -- Focus on race sessions
),

stint_durations AS (
    SELECT
        *,
        lap_end - lap_start + 1 as stint_length
    FROM stints_base
)

SELECT * FROM stint_durations
  );