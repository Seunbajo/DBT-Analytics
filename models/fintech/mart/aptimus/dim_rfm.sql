{{
  config(
    materialized='table'
  )
}}

with customer_transactions as (
    select
        customer_id
        , transaction_type
        , amount
        , txn_date
    from {{ ref('stg_aptimus_transactions') }}
    where txn_date <= '2026-06-30'   -- exclude cutoff rows
)

, customer_table as (
    select 
        customer_id
        , acquisition_channel
        , kyc_tier
    from {{ ref('stg_aptimus_customers') }}
)

, rfm_base as (
    select
        customer_id
        , date_diff('2026-06-30', max(txn_date), day) as recency_days
        , count(*) as frequency
        , sum(
            case when transaction_type = 'DEPOSIT' then amount
                 when transaction_type = 'WITHDRAWAL' then -amount
                 else 0
            end
        ) as monetary
    from customer_transactions
    group by customer_id

)

, rfm_scored as (
    select
        customer_id
        , recency_days
        , frequency
        , monetary
        -- recency: lower days = better, so reverse the quintile (5 = most recent)
        , 6 - ntile(5) over (order by recency_days) as recency_score
        , ntile(5) over (order by frequency) as frequency_score
        , ntile(5) over (order by monetary) as monetary_score
    from rfm_base
)

select
    customer_table.customer_id
    , customer_table.acquisition_channel
    , customer_table.kyc_tier
    , rfm_scored.recency_days
    , rfm_scored.frequency
    , rfm_scored.monetary
    , rfm_scored.recency_score
    , rfm_scored.frequency_score
    , rfm_scored.monetary_score
    , rfm_scored.recency_score + rfm_scored.frequency_score + rfm_scored.monetary_score as rfm_total
    , case
        when rfm_scored.recency_score >= 4 and rfm_scored.frequency_score >= 4 and rfm_scored.monetary_score >= 4
            then 'Champions'
        when rfm_scored.recency_score >= 4 and rfm_scored.frequency_score >= 3
            then 'Loyal'
        when rfm_scored.recency_score >= 4 and rfm_scored.frequency_score <= 2
            then 'New / Promising'
        when rfm_scored.recency_score <= 2 and rfm_scored.frequency_score >= 4 and rfm_scored.monetary_score >= 4
            then 'At Risk'
        when rfm_scored.recency_score <= 2 and rfm_scored.frequency_score <= 2
            then 'Dormant / Lost'
        else 'Standard'
    end as rfm_segment
from rfm_scored
left join customer_table
    on rfm_scored.customer_id = customer_table.customer_id
order by rfm_total desc