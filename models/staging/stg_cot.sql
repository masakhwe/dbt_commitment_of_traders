{{
    config(materialized='view' )
}}

select
    {{ dbt_utils.surrogate_key(['Market_and_Exchange_Names', 'Report_Date']) }} as report_id,
    {{ remove_exchange_name('Market_and_Exchange_Names')}} as market_name,
    *
from {{ source('staging', 'cot') }}

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
