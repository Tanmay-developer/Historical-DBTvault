
    insert into table ndb.h_cli
    select `hk_cli_cd`, `cli_id`, `ld_dt_tm`, `rcrd_src_nm` from h_cli__dbt_tmp

