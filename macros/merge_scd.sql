{%- macro generic_scd(src_tbls) -%}


WITH as_of_dates AS (
  {% for satellite, attributes in src_tbls.items() %}
    {% set pk = attributes['pk']['PK'] %}
    {% set ldts = attributes['ldts']['LDTS'] %}
    {%- set sat_db = attributes['db']['DB'] -%}
    {%- set sat_type = attributes['table_type']['TABLE_TYPE'] -%}
    {%- if sat_type == "driver" -%}
        select {{ pk }} as src_pk, {{ ldts }} as EFFECTIVE_FROM from {{ source(sat_db, satellite) }}
        {% if is_incremental() %}
            where {{ ldts }} > cast('2024-05-25 14:20:00.0' as timestamp)
        union
        select {{ pk }} as src_pk, max({{ ldts }}) as EFFECTIVE_FROM from {{ source(sat_db, satellite) }} group by {{ pk }}
        {%- endif -%}
        {%- else -%}
        select {{ pk }} as src_pk, max({{ ldts }}) as EFFECTIVE_FROM from {{ source(sat_db, satellite) }} 
        {% if is_incremental() %}
            where {{ ldts }} > (select max({{ ldts }}) from {{ source(sat_db, satellite) }})
        {%- endif -%}
        group by {{ pk }}
    {%- endif -%}
    {% if not loop.last %}
    union
    {% endif %}
  {% endfor %}
),


historical_data as (
    SELECT 
    a.src_pk,
    a.EFFECTIVE_FROM,
    {% for sat_name in src_tbls -%}
        {%set columns_iterable = (src_tbls[sat_name]['columns'] | list) -%}
        {%- for column in columns_iterable -%}
            COALESCE(
                {{ ( sat_name | lower ~ '_src' ) }}.{{ column }},
                LEAD({{ ( sat_name | lower ~ '_src' ) }}.{{ column }}) IGNORE NULLS OVER (PARTITION BY a.src_pk ORDER BY a.EFFECTIVE_FROM DESC)
            ) AS {{ column }}{{ "," }}
        {% endfor -%}
    {% endfor -%}
    CURRENT_TIMESTAMP as LOAD_DATETIME
    from as_of_dates a
    {% for sat_name in src_tbls -%}
        {%- set sat_pk_name = (src_tbls[sat_name]['pk'].keys() | list )[0] -%}
        {%- set sat_ldts_name = (src_tbls[sat_name]['ldts'].keys() | list )[0] -%}
        {%- set sat_db_name = (src_tbls[sat_name]['db'].keys() | list )[0] -%}
        {%- set sat_pk = (src_tbls[sat_name]['pk'][sat_pk_name]) -%}
        {%- set sat_ldts = (src_tbls[sat_name]['ldts'][sat_ldts_name]) -%}
        {%- set sat_db = src_tbls[sat_name]['db'][sat_db_name] -%}
        LEFT JOIN {{ source(sat_db, sat_name) }} AS {{ ( sat_name | lower ~ '_src' ) }}
        {{ "ON" | indent(4) }} a.src_pk = {{ ( sat_name | lower ~ '_src' ) }}.{{ sat_pk }}
        {{ "AND" | indent(4) }} a.EFFECTIVE_FROM = {{ ( sat_name | lower ~ '_src' ) }}.{{ sat_ldts }} 
    {% endfor -%}
)


select * from historical_data


{%- endmacro -%}