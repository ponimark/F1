{{ config(materialized='view') }}

    select *
    from {{ source('raw','stints') }}
    where compound is not null and lap_start is not null and lap_end is not null