
  create view "f1"."f1_data"."stg_pit__dbt_tmp"
    
    
  as (
    

    select *
    from "f1"."f1_data"."pit"
    where pit_duration is not null
  );