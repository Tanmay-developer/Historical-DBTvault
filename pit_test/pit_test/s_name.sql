{{ config(materialized='incremental',

file_format="hudi",

unique_key="hsh_ky_cli_cd,ld_dt_tm",

 options={ "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ld_dt_tm',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.table.base.file.format':'PARQUET'})

}}


{% set source_model =  "v_stg_s_name" %}

{% set src_pk  =   "hsh_ky_cli_cd" %}

{% set src_hashdiff = "rcrd_hsh_id" %}

{% set src_payload =  ["name"] %}
{% set src_eff = "EFFECTIVE_FROM" %}
{% set src_ldts =   {"source_column": "ld_dt_tm" , "alias" : "ld_dt_tm"} %}
{% set src_source =  { "source_column": "rcrd_src_nm" , "alias" : "rcrd_src_nm"} %}


{{ dbtvault.sat(src_pk = src_pk  , src_hashdiff = src_hashdiff, src_payload = src_payload,
                src_eff = src_eff,  src_ldts = src_ldts ,
                src_source = src_source , source_model = source_model) }}
