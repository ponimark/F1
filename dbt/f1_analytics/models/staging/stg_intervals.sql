{{ config(materialized='view') }}


   SELECT
    session_key,
    driver_number,
    date,
    CASE
        WHEN gap_to_leader = 'None' OR gap_to_leader LIKE '%L%' THEN NULL
        ELSE gap_to_leader
    END AS gap_to_leader,
    CASE
        WHEN interval = 'None' OR interval LIKE '%L%' THEN NULL
        ELSE interval
    END AS gap_to_next
FROM {{ source('raw', 'intervals') }}