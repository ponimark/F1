
  create view "f1"."f1_data"."int_best_laps__dbt_tmp"
    
    
  as (
    

select
        session_key,
        driver_number,
        min(lap_duration) as fastest_lap
from "f1"."f1_data"."stg_laps"
group by 1,2
  );