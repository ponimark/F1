
  create view "f1"."f1_data"."stg_weather__dbt_tmp"
    
    
  as (
    


select *
FROM "f1"."f1_data"."weather"
  );