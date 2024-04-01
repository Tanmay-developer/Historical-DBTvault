{{ config(
    file_format='iceberg',
    unique_key='application_id',
    partition_by='category',
    materialized='incremental',
    incremental_strategy='merge',
    schema='spark_catalog.ndb'
) }}

select *
from {{ source('ndb','ntable') }}
order by category
