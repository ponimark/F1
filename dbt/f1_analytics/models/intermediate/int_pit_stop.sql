{{ config(materialized='view') }}

SELECT
    session_key,
    driver_number,
    COUNT(*) AS pit_stop_count
FROM {{ ref('stg_pit') }}
GROUP BY session_key, driver_number