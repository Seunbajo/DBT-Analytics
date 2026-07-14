{{
  config(
    materialized='table'
  )
}}

with commission as (
    select 
        *
        , format_date("%Y-%m", date) as month
        , case
            when volume_bonus_applied = true then 'Yes'
            else 'No'
        end as volume_bonus_applied_two
    from {{ ref('stg_corridor_commission') }}

)

, monthly_threshold as (
  select
    *,
    case
        when monthly_principal_usd > 500000 then 'monthly txn > 500k'
        else 'monthly txn <= 500k'
    end as txn_size
from (
    select
        format_date("%Y-%m", date) as month,
        round(sum(principal_usd), 2) as monthly_principal_usd
    from commission
    group by 1
) as corridor_txn
)

, trans as (
  SELECT
  cc.txn_id
  , cc.date
  , cc.month
  , cc.corridor
  , cc.principal_usd
  , cc.stated_rate
  , cc.stated_commission_usd
  , cc.min_applied
  , cc.volume_bonus_applied_two as volume_bonus_applied
  , mt.txn_size
  , case
        when corridor in ('SL', 'LR', 'GN') and txn_size = 'monthly txn > 500k' then round(principal_usd * (0.008 + 0.0005), 2)
        when corridor in ('SL', 'LR', 'GN') and txn_size = 'monthly txn <= 500k' then round(principal_usd * 0.008 , 2)
        when corridor in ('SN', 'CI', 'GH') and txn_size = 'monthly txn > 500k' then round(principal_usd * (0.006 + 0.0005), 2)
        when corridor in ('SN', 'CI', 'GH') and txn_size = 'monthly txn <= 500k' then round(principal_usd * 0.006, 2)
        when corridor in ('NG', 'KE') and txn_size = 'monthly txn > 500k' then round(principal_usd * (0.008 + 0.0005), 2)
        when corridor in ('NG', 'KE') and txn_size = 'monthly txn <= 500k' then round(principal_usd * 0.005, 2)
    else 0
    end as recalculated_commission
  , case
        when corridor in ('SL', 'LR', 'GN') then 0.008
        when corridor in ('SN', 'CI', 'GH') then 0.006
        when corridor in ('NG', 'KE') then 0.005
    else 0
    end as commission_rate
from commission as cc
left join monthly_threshold as mt
    on cc.month = mt.month
) 

    select 
  * 
    , case
    -- Wrong corridor rate takes precedence
    when stated_rate != commission_rate then 'wrong_corridor_rate'
    -- Correct rate, check commission amount
    when stated_rate = commission_rate and stated_commission_usd = recalculated_commission then 'correct'
    when stated_rate = commission_rate and stated_commission_usd < recalculated_commission then 'systematic_underpayment'
    when stated_rate = commission_rate and stated_commission_usd > recalculated_commission then 'systematic_overpayment'
  else 'unclassified'
end as error_type
from trans