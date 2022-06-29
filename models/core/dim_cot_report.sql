{{
    config(materialized='table')
}}

select
    report_id,
    report_date,
    market_name,
    exchange_name,
    open_interest_all,
    dealer_positions_long_all,
    dealer_positions_short_all,
    dealer_positions_spread_all,
    asset_mgr_positions_long_all,
    asset_mgr_positions_short_all,
    asset_mgr_positions_spread_all,
    lev_money_positions_long_all,
    lev_money_positions_short_all,
    lev_money_positions_spread_all,
    CAST(change_in_open_interest_all as INTEGER) as change_in_open_interest_all,
    CAST(change_in_dealer_long_all as INTEGER) as change_in_dealer_long_all,
    CAST(change_in_dealer_short_all as INTEGER) as change_in_dealer_short_all,
    CAST(change_in_dealer_spread_all as INTEGER) as change_in_dealer_spread_all,
    CAST(change_in_asset_mgr_long_all as INTEGER) as change_in_asset_mgr_long_all,
    CAST(change_in_asset_mgr_short_all as INTEGER) as change_in_asset_mgr_short_all,
    CAST(change_in_asset_mgr_spread_all as INTEGER) as change_in_asset_mgr_spread_all,
    CAST(change_in_lev_money_long_all as INTEGER) as change_in_lev_money_long_all,
    CAST(change_in_lev_money_short_all as INTEGER) as change_in_lev_money_short_all,
    CAST(change_in_lev_money_spread_all as INTEGER) as change_in_lev_money_spread_all,
    pct_of_open_interest_all,
    pct_of_oi_dealer_long_all,
    pct_of_oi_dealer_short_all,
    pct_of_oi_dealer_spread_all,
    pct_of_oi_asset_mgr_long_all,
    pct_of_oi_asset_mgr_short_all,
    pct_of_oi_asset_mgr_spread_all,
    pct_of_oi_lev_money_long_all,
    pct_of_oi_lev_money_short_all,
    pct_of_oi_lev_money_spread_all,
    traders_tot_all,
    CAST(traders_dealer_long_all as INTEGER) as traders_dealer_long_all,
    CAST(traders_dealer_short_all as INTEGER) as traders_dealer_short_all,
    CAST(traders_dealer_spread_all as INTEGER) as traders_dealer_spread_all,
    CAST(traders_asset_mgr_long_all as INTEGER) as traders_asset_mgr_long_all,
    CAST(traders_asset_mgr_short_all as INTEGER) as traders_asset_mgr_short_all,
    CAST(traders_asset_mgr_spread_all as INTEGER) as traders_asset_mgr_spread_all,
    CAST(traders_lev_money_long_all as INTEGER) as traders_lev_money_long_all,
    CAST(traders_lev_money_short_all as INTEGER) as traders_lev_money_short_all,
    CAST(traders_lev_money_spread_all  as INTEGER) as traders_lev_money_spread_all
from {{ ref('stg_cot')}}