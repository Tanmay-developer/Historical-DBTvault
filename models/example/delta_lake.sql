{{ config(
    file_format='delta',
    unique_key='application_id',
    partition_by='category',
    materialized='incremental',
    incremental_strategy='merge'
) }}

select *
from {{ source('ndb','ntable') }}