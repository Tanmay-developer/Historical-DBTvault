{% set yaml_metadata %}

source_model: 'raw_h_cli'
derived_columns:

    ld_dt_tm: CURRENT_TIMESTAMP


hashed_columns:
    hk_cli_cd: ["cli_id"]

{% endset %}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set derived_columns = metadata_dict['derived_columns'] %}
{% set source_model = metadata_dict['source_model'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                    hashed_columns=hashed_columns,
                ranked_columns=none) }}
