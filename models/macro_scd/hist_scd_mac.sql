{{ config(materialized='incremental') }}


{%- set yaml_metadata -%}
src_mdl: customer_addr
src_pk: cstmr_id
src_db: kdb
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
      
src_ldts: psa_record_load_datetime
{%- endset -%}






{% set metadata_dict = fromyaml(yaml_metadata) %}


{% set src_mdl = metadata_dict['src_mdl'] %}
{% set src_pk = metadata_dict['src_pk'] %}
{% set src_db = metadata_dict['src_db'] %}
{% set src_tbls = metadata_dict['src_tbls'] %}
{% set src_ldts = metadata_dict['src_ldts'] %}


{{ tscd(src_mdl=src_mdl, src_pk=src_pk, src_db = src_db,
                  src_tbls=src_tbls, src_ldts=src_ldts) }}