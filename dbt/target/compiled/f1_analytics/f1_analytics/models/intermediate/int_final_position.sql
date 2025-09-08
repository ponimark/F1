

WITH ranked AS (
    SELECT
        session_key,
        driver_number,
        position,
        date AS latest_date,
        ROW_NUMBER() OVER (
            PARTITION BY session_key, driver_number
            ORDER BY date DESC
        ) AS rn
    FROM "f1"."f1_data"."stg_position"
)
SELECT
    session_key,
    driver_number,
    position,
    latest_date
FROM ranked
WHERE rn = 1