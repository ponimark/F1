
  create view "f1"."f1_data"."stg_meeting__dbt_tmp"
    
    
  as (
    

    select
            meeting_key,
            meeting_name,
            meeting_official_name,
            country_name,
            circuit_short_name,
            date_start,
            year
from "f1"."f1_data"."meetings"
  );