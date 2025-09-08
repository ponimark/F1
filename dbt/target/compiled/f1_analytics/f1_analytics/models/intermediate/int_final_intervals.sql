SELECT
    session_key,
    driver_number,
    MAX(CASE
        WHEN gap_to_leader LIKE '%L%' THEN NULL
        ELSE CAST(gap_to_leader AS FLOAT)
    END) AS max_gap_to_leader,
    MAX(CASE
        WHEN gap_to_next LIKE '%L%' THEN NULL
        ELSE CAST(gap_to_next AS FLOAT)
    END) AS max_gap_to_next
FROM "f1"."f1_data"."stg_intervals"
GROUP BY session_key, driver_number