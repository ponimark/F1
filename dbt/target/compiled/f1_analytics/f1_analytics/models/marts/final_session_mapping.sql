

SELECT DISTINCT
    EXTRACT(YEAR FROM date_start)::text || ' - ' || country_name || ' - ' || session_name AS session_display_name,
    session_key
FROM "f1"."f1_data"."stg_sessions"