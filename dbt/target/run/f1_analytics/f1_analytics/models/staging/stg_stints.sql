
  create view "f1"."f1_data"."stg_stints__dbt_tmp"
    
    
  as (
    

    select *
    from "f1"."f1_data"."stints"
    where compound is not null and lap_start is not null and lap_end is not null
  );