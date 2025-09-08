{{ config(materialized='view') }}

select
        session_key,
        driver_number,
        min(lap_duration) as fastest_lap
from {{ ref('stg_laps') }}
group by 1,2