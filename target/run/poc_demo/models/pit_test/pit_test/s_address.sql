
    insert into table ndb.s_address
    select `hsh_ky_cli_cd`, `rcrd_hsh_id`, `addr`, `EFFECTIVE_FROM`, `ld_dt_tm`, `rcrd_src_nm` from s_address__dbt_tmp

