{{ config(materialized='view') }}

WITH first_position_record AS (
    SELECT *
    FROM (
        SELECT
            session_key,
            driver_number,
            position AS first_position,
            date,
            ROW_NUMBER() OVER (
                PARTITION BY session_key, driver_number
                ORDER BY date
            ) AS rn
        FROM stg_position
    ) t
    WHERE rn = 1
),
lap_metrics AS (
    SELECT
        l.lap_number,
        l.session_key,
        l.driver_number,
        l.date_start,
        l.date_end,
        l.is_pit_out_lap,
        l.duration_sector_1,
        l.duration_sector_2,
        l.duration_sector_3,
        fpr.first_position,
        MIN(i.gap_to_leader) AS min_gap_to_leader,
        MAX(i.gap_to_leader) AS max_gap_to_leader,
        ROUND(MIN(i.gap_to_next)::numeric,3) AS min_gap_to_next,
        ROUND(MAX(i.gap_to_next)::numeric,3) AS max_gap_to_next,
        sp.position AS start_position,
        ep.position AS end_position
    FROM {{ ref('stg_laps') }} l
    LEFT JOIN first_position_record fpr
        ON l.session_key = fpr.session_key
        AND l.driver_number = fpr.driver_number
    LEFT JOIN {{ ref('stg_intervals') }} i
        ON i.session_key = l.session_key
        AND i.driver_number = l.driver_number
        AND date_trunc('second', i.date) BETWEEN l.date_start AND l.date_end
    LEFT JOIN {{ ref('int_lap_position') }} sp
        ON l.session_key = sp.session_key
        AND l.driver_number = sp.driver_number
        AND l.lap_number = sp.lap_number
        AND sp.start_position_rank = 1
    LEFT JOIN {{ ref('int_lap_position') }} ep
        ON l.session_key = ep.session_key
        AND l.driver_number = ep.driver_number
        AND l.lap_number = ep.lap_number
        AND ep.end_position_rank = 1
    GROUP BY
        l.lap_number, l.session_key, l.driver_number, l.date_start,
        l.date_end, l.is_pit_out_lap,
        l.duration_sector_1, l.duration_sector_2, l.duration_sector_3,
        fpr.first_position, sp.position, ep.position
),
filled_positions AS (
    SELECT
        *,
        CASE
            WHEN lap_number = 1 THEN first_position
            ELSE COALESCE(
                start_position,
                MAX(start_position) OVER (
                    PARTITION BY session_key, driver_number
                    ORDER BY lap_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                first_position
            )
        END AS lap_start_position,
        CASE
            WHEN lap_number = 1 THEN first_position
            ELSE COALESCE(
                end_position,
                MAX(end_position) OVER (
                    PARTITION BY session_key, driver_number
                    ORDER BY lap_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                first_position
            )
        END AS lap_end_position
    FROM lap_metrics
)
SELECT
    lap_number,
    session_key,
    driver_number,
    date_start AS lap_start_time,
    date_end AS lap_end_time,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    min_gap_to_leader,
    max_gap_to_leader,
    min_gap_to_next,
    max_gap_to_next,
    lap_start_position,
    lap_end_position,
    first_position AS race_start_position
FROM filled_positions
