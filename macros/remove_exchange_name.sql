{#
    This macro removes the exchange names
#}

{% macro remove_exchange_name(given_exchange_name) -%}
    lower( {{given_exchange_name}})
{%- endmacro %}