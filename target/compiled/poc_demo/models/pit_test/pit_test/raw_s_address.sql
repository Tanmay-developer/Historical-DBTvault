select distinct
v_h.cli_id,
v_h.rcrd_src_nm,
v_s_address.addr,
v_s_address.ts
from ndb.raw_h_cli v_h
LEFT JOIN ndb.address2 v_s_address
ON v_h.cli_id = v_s_address.cli_id