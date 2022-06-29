{{
    config(materialized='incremental' )
}}

select
    {{ dbt_utils.surrogate_key(['market_name', 'report_date']) }} as report_id,
    CAST(report_date as DATE) as report_date,
    market_name,
    exchange_name,
    CAST(open_interest_all as INTEGER) as open_interest_all,
    CAST(dealer_positions_long_all as INTEGER) as dealer_positions_long_all,
    CAST(dealer_positions_short_all as INTEGER) as dealer_positions_short_all,
    CAST(dealer_positions_spread_all as INTEGER) as dealer_positions_spread_all,
    CAST(asset_mgr_positions_long_all as INTEGER) as asset_mgr_positions_long_all,
    CAST(asset_mgr_positions_short_all as INTEGER) as asset_mgr_positions_short_all,
    CAST(asset_mgr_positions_spread_all as INTEGER)as asset_mgr_positions_spread_all,
    CAST(lev_money_positions_long_all as INTEGER) as lev_money_positions_long_all,
    CAST(lev_money_positions_short_all as INTEGER) as lev_money_positions_short_all,
    CAST(lev_money_positions_spread_all as INTEGER) as lev_money_positions_spread_all,
    REGEXP_REPLACE(change_in_open_interest_all, '[.]', '0') as change_in_open_interest_all,
    REGEXP_REPLACE(change_in_dealer_long_all, '[.]', '0') as change_in_dealer_long_all,
    REGEXP_REPLACE(change_in_dealer_short_all, '[.]', '0') as change_in_dealer_short_all,
    REGEXP_REPLACE(change_in_dealer_spread_all, '[.]', '0') as change_in_dealer_spread_all,
    REGEXP_REPLACE(change_in_asset_mgr_long_all, '[.]', '0') as change_in_asset_mgr_long_all,
    REGEXP_REPLACE(change_in_asset_mgr_short_all, '[.]', '0') as change_in_asset_mgr_short_all,
    REGEXP_REPLACE(change_in_asset_mgr_spread_all, '[.]', '0') as change_in_asset_mgr_spread_all,
    REGEXP_REPLACE(change_in_lev_money_long_all, '[.]', '0') as change_in_lev_money_long_all,
    REGEXP_REPLACE(change_in_lev_money_short_all, '[.]', '0') as change_in_lev_money_short_all,
    REGEXP_REPLACE(change_in_lev_money_spread_all, '[.]', '0') as change_in_lev_money_spread_all,
    CAST(pct_of_open_interest_all as NUMERIC) as pct_of_open_interest_all,
    CAST(pct_of_oi_dealer_long_all as NUMERIC) as pct_of_oi_dealer_long_all,
    CAST(pct_of_oi_dealer_short_all as NUMERIC) as pct_of_oi_dealer_short_all,
    CAST(pct_of_oi_dealer_spread_all as NUMERIC) as pct_of_oi_dealer_spread_all,
    CAST(pct_of_oi_asset_mgr_long_all as NUMERIC) as pct_of_oi_asset_mgr_long_all,
    CAST(pct_of_oi_asset_mgr_short_all as NUMERIC) as pct_of_oi_asset_mgr_short_all,
    CAST(pct_of_oi_asset_mgr_spread_all as NUMERIC) as pct_of_oi_asset_mgr_spread_all,
    CAST(pct_of_oi_lev_money_long_all as NUMERIC) as pct_of_oi_lev_money_long_all,
    CAST(pct_of_oi_lev_money_short_all as NUMERIC) as pct_of_oi_lev_money_short_all,
    CAST(pct_of_oi_lev_money_spread_all as NUMERIC) as pct_of_oi_lev_money_spread_all,
    CAST(traders_tot_all as INTEGER) as traders_tot_all,
     REGEXP_REPLACE(traders_dealer_long_all, '[.]', '0') as traders_dealer_long_all,
     REGEXP_REPLACE(traders_dealer_short_all, '[.]', '0') as traders_dealer_short_all,
     REGEXP_REPLACE(traders_dealer_spread_all, '[.]', '0') as traders_dealer_spread_all,
     REGEXP_REPLACE(traders_asset_mgr_long_all, '[.]', '0') as traders_asset_mgr_long_all,
     REGEXP_REPLACE(traders_asset_mgr_short_all, '[.]', '0') as traders_asset_mgr_short_all,
     REGEXP_REPLACE(traders_asset_mgr_spread_all, '[.]', '0') as traders_asset_mgr_spread_all,
     REGEXP_REPLACE(traders_lev_money_long_all, '[.]', '0') as traders_lev_money_long_all,
     REGEXP_REPLACE(traders_lev_money_short_all, '[.]', '0') as traders_lev_money_short_all,
     REGEXP_REPLACE(traders_lev_money_spread_all, '[.]', '0') as traders_lev_money_spread_all
FROM {{ source('staging', 'cot') }}
WHERE 1 = 1
    {% if is_incremental() %}
      AND report_date >= (SELECT max(report_date) FROM {{ this }})
    {% endif %}
