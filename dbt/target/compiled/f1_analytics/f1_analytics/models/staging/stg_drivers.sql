

SELECT
  CAST(driver_number AS INT)           AS driver_number,
  BROADCAST_NAME                       as broadcast_name,
  first_name,
  last_name,
  FULL_NAME                            AS full_name,
  name_acronym,
  team_name,
  team_colour,
  country_code,
  session_key
from "f1"."f1_data"."drivers"