select distinct
v_h.cli_id,
v_h.rcrd_src_nm,
v_s_name.name,
v_s_name.ts
from {{ref('raw_h_cli')}} v_h
LEFT JOIN ndb.name2 v_s_name
ON v_h.cli_id = v_s_name.cli_id
