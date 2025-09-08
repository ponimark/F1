{{ config(materialized='view') }}

select
    session_key,
    driver_number,
    date_trunc('second', date_start) as date_start,
    date_start + interval '1 second' * lap_duration as date_end,
    lap_duration,
    lap_number,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3
from {{ source('raw','laps') }}
where lap_duration is not null
