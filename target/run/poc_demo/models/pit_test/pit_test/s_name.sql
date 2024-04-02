
    insert into table ndb.s_name
    select `hsh_ky_cli_cd`, `rcrd_hsh_id`, `name`, `EFFECTIVE_FROM`, `ld_dt_tm`, `rcrd_src_nm` from s_name__dbt_tmp

