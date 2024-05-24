{{ config(materialized='incremental',
incremental_strategy='merge',
    file_format='hudi',
    unique_key="src_pk,EFFECTIVE_FROM",
    options={
        'type':'cow'
    }
    )
}}

{%- set yaml_metadata -%}

src_tbls:
  customer_addr:
    db:
      DB: kdb
    pk:
      PK: cstmr_id
    ldts:
      LDTS: ldts
    table_type:
      TABLE_TYPE: driver
    columns:
      - addr
  customer_phn:
    db:
      DB: kdb
    pk:
      PK: cstmr_id
    ldts:
      LDTS: ldts
    table_type:
      TABLE_TYPE: driver
    columns:
      - phone_no

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set src_tbls = metadata_dict['src_tbls'] %}

{{ generic_scd(src_tbls=src_tbls) }}