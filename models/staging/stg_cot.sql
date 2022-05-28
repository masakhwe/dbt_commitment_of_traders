{{
    config(materialized='view' )
}}

select
    *
from {{source('committment_of_traders', 'cot')}}
limit 100