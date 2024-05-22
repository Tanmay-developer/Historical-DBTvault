{%- macro tscd(src_pk, src_tbls, src_ldts, src_db, src_mdl) -%}


{%- set src_pk = src_pk -%}
{%- set src_ldts = src_ldts -%}

{#- Setting ghost values to replace NULLS -#}
{%- set ghost_pk = '0000000000000000' -%}
{%- set ghost_date = '1900-01-01 00:00:00.000' %}


{#- Setting the new AS_OF dates CTE name -#}
{%- set new_as_of_dates_cte = 'as_of_dates' -%}


WITH as_of_date AS (
  {% for satellite, attributes in src_tbls.items() %}
    {% set pk = attributes['pk']['PK'] %}
    {% set ldts = attributes['ldts']['LDTS'] %}
    {%- set sat_db = attributes['db']['DB'] -%}
    {%- set sat_type = attributes['table_type']['TABLE_TYPE'] -%}
    {%- if sat_type == "driver" -%}
    select {{ pk }} as {{ src_pk}}, {{ ldts }} as EFFECTIVE_FROM from {{ source(sat_db, satellite) }}
    {%- else -%}
    select {{ pk }} as {{ src_pk}}, max({{ ldts }}) as EFFECTIVE_FROM from {{ source(sat_db, satellite) }} group by {{ pk }}
    {%- endif -%}
    {% if not loop.last %}
    union
    {% endif %}
  {% endfor %}
),

as_of_dates AS (
    SELECT distinct {{ src_pk }}, EFFECTIVE_FROM as AS_OF_DATE FROM as_of_date
),

new_rows_as_of_dates AS (
    SELECT
        a.{{ src_pk }},
        b.AS_OF_DATE
    FROM {{ source(src_db, src_mdl) }} AS a
    LEFT JOIN {{ new_as_of_dates_cte }} AS b
    ON a.{{ src_pk }} = b.{{ src_pk }}
),

new_rows AS (
    SELECT
        a.{{ src_pk }},
        a.AS_OF_DATE,

    {%- for sat_name in src_tbls -%}
        {%- set sat_pk_name = (src_tbls[sat_name]['pk'].keys() | list )[0] -%}
        {%- set sat_ldts_name = (src_tbls[sat_name]['ldts'].keys() | list )[0] -%}
        {%- set sat_pk = (src_tbls[sat_name]['pk'][sat_pk_name]) -%}
        {%- set sat_ldts = (src_tbls[sat_name]['ldts'][sat_ldts_name]) %}
        {%- if target.type == "sqlserver" -%}
            COALESCE(MAX({{ ( sat_name | lower ~ '_src' ) }}.{{ sat_pk }}), 
            CONVERT(BINARY(16), '{{ ghost_pk }}', 2)) AS {{ ( sat_name | upper ~ '_' ~ sat_pk_name | upper ) }},
        {%- else -%}
            COALESCE(MAX({{ ( sat_name | lower ~ '_src' ) }}.{{ sat_pk }}), 
            CAST('{{ ghost_pk }}' AS STRING)) AS {{ ( sat_name | upper ~ '_' ~ sat_pk_name | upper ) }},
        {%- endif -%}
        COALESCE(MAX({{ ( sat_name | lower ~ '_src' ) }}.{{ sat_ldts }}), 
        CAST('{{ ghost_date }}' AS timestamp)) AS {{ ( sat_name | upper ~ '_' ~ sat_ldts_name | upper ) }}
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM new_rows_as_of_dates AS a

    {% for sat_name in src_tbls -%}
        {%- set sat_pk_name = (src_tbls[sat_name]['pk'].keys() | list )[0] -%}
        {%- set sat_ldts_name = (src_tbls[sat_name]['ldts'].keys() | list )[0] -%}
        {%- set sat_db_name = (src_tbls[sat_name]['db'].keys() | list )[0] -%}
        {%- set sat_pk = (src_tbls[sat_name]['pk'][sat_pk_name]) -%}
        {%- set sat_ldts = (src_tbls[sat_name]['ldts'][sat_ldts_name]) -%}
        {%- set sat_db = src_tbls[sat_name]['db'][sat_db_name] -%}
        LEFT JOIN {{ source(sat_db, sat_name) }} AS {{ ( sat_name | lower ~ '_src' ) }}
        {{ "ON" | indent(4) }} a.{{ src_pk }} = {{ ( sat_name | lower ~ '_src' ) }}.{{ sat_pk }}
        {{ "AND" | indent(4) }} {{ ( sat_name | lower ~ '_src' ) }}.{{ sat_ldts }} <= a.AS_OF_DATE
    {% endfor -%}

    GROUP BY
        a.{{ src_pk }}, a.AS_OF_DATE
),
temp as (
    select 
    a.{{ src_pk }},
    a.AS_OF_DATE as start_date,
    lead(a.as_of_date) over (partition by a.{{ src_pk }} order by a.as_of_date asc) as end_date,
    {% for sat_name in src_tbls -%}
        {%set columns_iterable = (src_tbls[sat_name]['columns'] | list) -%}
        {%- for column in columns_iterable -%}
            {{ ( sat_name | lower ~ '_src' ) }}.{{ column }},
        {% endfor -%}
    {% endfor -%}
    CURRENT_TIMESTAMP as LOAD_DATETIME
    from new_rows a
    {% for sat_name in src_tbls -%}
        {%- set sat_pk_name = (src_tbls[sat_name]['pk'].keys() | list )[0] -%}
        {%- set sat_ldts_name = (src_tbls[sat_name]['ldts'].keys() | list )[0] -%}
        {%- set sat_db_name = (src_tbls[sat_name]['db'].keys() | list )[0] -%}
        {%- set sat_pk = (src_tbls[sat_name]['pk'][sat_pk_name]) -%}
        {%- set sat_ldts = (src_tbls[sat_name]['ldts'][sat_ldts_name]) -%}
        {%- set sat_db = src_tbls[sat_name]['db'][sat_db_name] -%}
        LEFT JOIN {{ source(sat_db, sat_name) }} AS {{ ( sat_name | lower ~ '_src' ) }}
        {{ "ON" | indent(4) }} a.{{ src_pk }} = {{ ( sat_name | lower ~ '_src' ) }}.{{ sat_pk }}
        {{ "AND" | indent(4) }} {{ ( sat_name | lower ~ '_src' ) }}.{{ sat_ldts }} = a.{{ ( sat_name | upper ~ '_' ~ sat_ldts_name | upper ) }}
    {% endfor -%}
),
pit AS (
    SELECT * FROM temp
)

SELECT DISTINCT * FROM pit

{%- endmacro -%}