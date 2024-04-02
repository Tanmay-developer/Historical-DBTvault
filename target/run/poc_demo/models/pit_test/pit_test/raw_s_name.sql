create or replace view ndb.raw_s_name
  
  as
    select distinct
v_h.cli_id,
v_h.rcrd_src_nm,
v_s_name.name,
v_s_name.ts
from ndb.raw_h_cli v_h
LEFT JOIN ndb.name2 v_s_name
ON v_h.cli_id = v_s_name.cli_id
