{%- set yaml_metadata -%}
source_model: h_cli
src_pk: hk_cli_cd
as_of_dates_table: as_of_date
satellites:
  s_address:
    pk:
      PK: hsh_ky_cli_cd
    ldts:
      LDTS: EFFECTIVE_FROM
    columns:
      - addr
  s_name:
    pk:
      PK: hsh_ky_cli_cd
    ldts:
      LDTS: EFFECTIVE_FROM
    columns:
      - name
stage_tables:
  v_stg_s_address: EFFECTIVE_FROM
  v_stg_s_name: EFFECTIVE_FROM
src_ldts: ld_dt_tm
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set src_pk = metadata_dict['src_pk'] %}
{% set as_of_dates_table = metadata_dict['as_of_dates_table'] %}
{% set satellites = metadata_dict['satellites'] %}
{% set stage_tables = metadata_dict['stage_tables'] %}
{% set src_ldts = metadata_dict['src_ldts'] %}

{{ dbtvault.pit(source_model=source_model, src_pk=src_pk,
                   as_of_dates_table=as_of_dates_table,
                   satellites=satellites,
                   stage_tables=stage_tables,
                   src_ldts=src_ldts) }}

