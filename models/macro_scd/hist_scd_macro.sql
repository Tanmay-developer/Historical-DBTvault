{%- set yaml_metadata -%}
source_model: etli_applications_psa
src_pk: etli_applicationnumber
src_db: dataverse_preprod_mirror_d365_psa
satellites:
  etli_applications_psa:
    db:
      DB: dataverse_preprod_mirror_d365_psa
    pk:
      PK: etli_applicationnumber
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driver
    columns:
      - etli_proposername
      - etli_policystatusname
      - etli_branchcode
  tagrp_psa:
    db:
      DB: dataverse_preprod_mirror_ing_psa
    pk:
      PK: pol_id
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driven
    columns:
      - agt_id
src_ldts: psa_record_load_datetime
{%- endset -%}



{% set metadata_dict = fromyaml(yaml_metadata) %}


{% set source_model = metadata_dict['source_model'] %}
{% set src_pk = metadata_dict['src_pk'] %}
{% set src_db = metadata_dict['src_db'] %}
{% set satellites = metadata_dict['satellites'] %}
{% set src_ldts = metadata_dict['src_ldts'] %}



{{ tscd(source_model=source_model, src_pk=src_pk, src_db = src_db,
                  satellites=satellites, src_ldts=src_ldts) }}