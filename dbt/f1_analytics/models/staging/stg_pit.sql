{{ config(materialized='view') }}

    select *
    from {{ source('raw','pit') }}
    where pit_duration is not null