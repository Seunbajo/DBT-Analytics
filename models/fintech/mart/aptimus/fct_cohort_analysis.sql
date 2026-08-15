{{
  config(
    materialized='table'
  )
}}

with customers as (
    select
        customer_id
        , format_date('%Y-%m', signup_date) as cohort_month
        , coalesce(acquisition_channel, 'Unknown') as acquisition_channel
    from {{ ref('stg_aptimus_customers') }}
)
, cohort_sizes as (
    select
        cohort_month
        , acquisition_channel
        , count(distinct customer_id) as cohort_size
    from customers
    group by cohort_month, acquisition_channel
)
, transactions as (
    select
        customer_id
        , format_date('%Y-%m', txn_date) as txn_month
    from {{ ref('stg_aptimus_transactions') }}
    where txn_date <= '2026-06-30'
    group by customer_id, format_date('%Y-%m', txn_date)
)
, activity as (
    select
        c.customer_id
        , c.cohort_month
        , c.acquisition_channel
        , date_diff(
            parse_date('%Y-%m', t.txn_month)
            , parse_date('%Y-%m', c.cohort_month)
            , month
          ) as month_on_book
    from customers c
    join transactions t
        on c.customer_id = t.customer_id
    where t.txn_month >= c.cohort_month
)
, retained_counts as (
    select
        cohort_month
        , acquisition_channel
        , month_on_book
        , count(distinct customer_id) as active_customers
    from activity
    group by cohort_month, acquisition_channel, month_on_book
)
select
    cs.cohort_month
    , cs.acquisition_channel
    , r.month_on_book
    , cs.cohort_size                                                   
    , coalesce(r.active_customers, 0) as active_customers              
    , safe_divide(coalesce(r.active_customers, 0), cs.cohort_size) as retention_pct  
from cohort_sizes cs
left join retained_counts r
    on cs.cohort_month = r.cohort_month
    and cs.acquisition_channel = r.acquisition_channel
order by cs.cohort_month, cs.acquisition_channel, r.month_on_book