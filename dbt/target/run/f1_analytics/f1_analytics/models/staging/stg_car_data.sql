
  create view "f1"."f1_data"."stg_car_data__dbt_tmp"
    
    
  as (
    

SELECT
    session_key,
    driver_number,
    date,
    rpm,
    speed,
    throttle,
    brake
FROM "f1"."f1_data"."car_data"
  );