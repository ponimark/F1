

SELECT
    session_key,
    driver_number,
    COUNT(*) AS pit_stop_count
FROM "f1"."f1_data"."stg_pit"
GROUP BY session_key, driver_number