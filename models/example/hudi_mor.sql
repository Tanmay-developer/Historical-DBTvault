{{ config(
    file_format='hudi',
    unique_key='application_id',
    partition_by='category',
    materialized='incremental',
    incremental_strategy='merge',
    options={
       'type': 'mor',
       'primaryKey': 'application_id',
       'preCombineField': 'watermark'
   }
) }}

select *
from {{ source('ndb','ntable') }}