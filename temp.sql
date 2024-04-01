
{%set columns_iterable = dbtvault.escape_column_names(satellites[sat_name]['columns'] | list) -%}
temp as (
    select 
    {{ dbtvault.prefix([src_pk], 'a') }},
    a.AS_OF_DATE,
    lead(a.as_of_date) over (partition by a.{{ dbtvault.prefix([src_pk], 'a') }} order by a.as_of_date asc) as end_date,
    {% for sat_name in satellites -%}
        {%set columns_iterable = dbtvault.escape_column_names(satellites[sat_name]['columns'] | list) -%}
        {%- for column in columns_iterable -%}
            {{ column }}
        {{- ',' if not loop.last -}}
        {% endfor -%}
    {{- ',' if not loop.last -}}    
    {% endfor -%}
    from new_rows a
    {% for sat_name in satellites -%}
        {%- set sat_pk_name = (satellites[sat_name]['pk'].keys() | list )[0] -%}
        {%- set sat_ldts_name = (satellites[sat_name]['ldts'].keys() | list )[0] -%}
        {%- set sat_pk = dbtvault.escape_column_names(satellites[sat_name]['pk'][sat_pk_name]) -%}
        {%- set sat_ldts = dbtvault.escape_column_names(satellites[sat_name]['ldts'][sat_ldts_name]) -%}
        LEFT JOIN {{ ref(sat_name) }} AS {{ dbtvault.escape_column_names( sat_name | lower ~ '_src' ) }}
        {{ "ON" | indent(4) }} a.{{ src_pk }} = {{ dbtvault.escape_column_names( sat_name | lower ~ '_src' ) }}.{{ sat_pk }}
        {{ "AND" | indent(4) }} {{ dbtvault.escape_column_names( sat_name | lower ~ '_src' ) }}.{{ sat_ldts }} = {{ dbtvault.escape_column_names( sat_name | lower ~ '_src' ) }}.{{ dbtvault.escape_column_names( sat_name | upper ~ '_' ~ sat_ldts_name | upper ) }}
    {% endfor -%}
)