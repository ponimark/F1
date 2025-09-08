{{ config(materialized='view') }}

    select
            meeting_key,
            meeting_name,
            meeting_official_name,
            country_name,
            circuit_short_name,
            date_start,
            year
from {{ source('raw','meetings') }}