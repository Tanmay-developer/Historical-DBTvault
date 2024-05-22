{%- set yaml_metadata -%}
src_mdl: etli_applications_psa
src_pk: etli_applicationnumber
src_db: dataverse_preprod_mirror_d365_psa
src_tbls:
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
      - etli_ifetlistaff
      - etli_smokerstatus
      - etli_brokercode
      - etli_isonline
      - etli_instastp
      - etli_writingagentcode
      - etli_sumassured
      - etli_sourcename
      - etli_hni
      - etli_area
      - etli_corporatebrokerbranchcode
      - etli_dmcacode
      - etli_physical_branch_code
      - etli_servicingagentcode
      - etli_subcode
  tpol_psa:
    db:
      DB: dataverse_preprod_mirror_ing_psa
    pk:
      PK: pol_id
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driver
    columns:
      - pol_cstat_cd
      - pol_mprem_amt
      - plan_id
      - serv_tax_amt
      - pol_bill_mode_cd
      - pol_bill_typ_cd
  tcvg_psa:
    db:
      DB: dataverse_preprod_mirror_ing_psa
    pk:
      PK: pol_id
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driver
    columns:
      - cvg_sum_ins_amt
      - cvg_mat_xpry_dur
      - cvg_stbl_1_cd
      - cvg_num
  etli_applicationproducts_psa:
    db:
      DB: dataverse_preprod_mirror_d365_psa
    pk:
      PK: etli_applicationname
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driver
    columns:
      - etli_applicationname
      - etli_productname
      - etli_product
      - etli_payoutoptionname
      - etli_uwsuc
      - etli_planoptionname
      - etli_bimodelpremium
      - etli_frequencyofpremiumpaymentname
      - etli_policytermsname
      - etli_premiumpaytermname
  etli_riders_psa:
    db:
      DB: dataverse_preprod_mirror_d365_psa
    pk:
      PK: etli_applicationnumbername
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driver
    columns:
      - etli_applicationnumbername
      - etli_bipremium
      - etli_productrider
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
  etli_payments_psa:
    db:
      DB: dataverse_preprod_mirror_d365_psa
    pk:
      PK: etli_applicationno
    ldts:
      LDTS: psa_record_effective_from_datetime
    table_type:
      TABLE_TYPE: driven
    columns:
      - etli_paymentmodesname
      - etli_receiptdate
      - etli_receiptstatusname
      - etli_paymentreasonname
      - etli_total_base
      - etli_firstname
      - etli_lastname
      - etli_receiptnumber
      - etli_suspenseamount_base
      - etli_recorddate
      - etli_applicationno
      - etli_igst
      - etli_cgst
      - createdon


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