
  create view "f1"."f1_data"."stg_position__dbt_tmp"
    
    
  as (
    

    WITH source AS (
    SELECT * FROM "f1"."f1_data"."position"
)
SELECT
    session_key,
    driver_number,
    position,
    date
FROM source
  );