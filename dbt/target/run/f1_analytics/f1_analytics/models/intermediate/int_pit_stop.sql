
  create view "f1"."f1_data"."int_pit_stop__dbt_tmp"
    
    
  as (
    

SELECT
    session_key,
    driver_number,
    COUNT(*) AS pit_stop_count
FROM "f1"."f1_data"."stg_pit"
GROUP BY session_key, driver_number
  );