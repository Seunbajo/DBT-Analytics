{{
  config(
    materialized='table',
    description='customer segmentation analysis for marketing insights including revenue, retention, and transaction patterns'
  )
}}

with customer_transactions as (
  select
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_segment,
    c.signup_date,
    c.kyc_status,
    cn.country_name as customer_country,
    t.transaction_id,
    t.transaction_amount,
    t.transaction_fee,
    t.payment_method,
    t.transaction_status,
    t.transaction_datetime,
    t.source_country,
    t.destination_country
  from {{ ref('stg_transfera_customers') }} as c
  left join {{ ref('stg_transfera_transactions') }} as t
    on c.customer_id = t.customer_id
  left join {{ ref('stg_transfera_countries') }} as cn
    on c.country_code = cn.country_code
  where t.transaction_status = 'Successful'
),

customer_metrics as (
  select
    customer_id
    , full_name
    , email
    , customer_segment
    , customer_country
    , signup_date
    , kyc_status
    , count(distinct transaction_id) as total_transactions
    , sum(transaction_amount) as total_transaction_value
    , avg(transaction_amount) as avg_transaction_value
    , sum(transaction_fee) as total_fees_paid
    , min(transaction_datetime) as first_transaction_date
    , max(transaction_datetime) as last_transaction_date
    , date_diff(current_date(), date(max(transaction_datetime)), day) as days_since_last_transaction
    , date_diff(date(max(transaction_datetime)), date(min(transaction_datetime)), day) as customer_lifetime_days
    , count(distinct date(transaction_datetime)) as active_transaction_days
    , count(distinct format_date('%y-%m', transaction_datetime)) as active_months
    , count(distinct payment_method) as payment_methods_used
    , string_agg(distinct payment_method, ', ') as payment_methods_list
    , count(distinct destination_country) as unique_destination_countries
    , date_diff(current_date(), date(signup_date), day) as days_since_signup
  from customer_transactions
  group by 
    customer_id,
    full_name,
    email,
    customer_segment,
    customer_country,
    signup_date,
    kyc_status
)

, customer_classification as (
  select
    *
    , case 
      when active_months > 0 then round(total_transactions / active_months, 2)
      else 0 
    end as avg_transactions_per_month
    , case
      when total_transaction_value >= 50000 then 'high-value'
      when total_transaction_value >= 10000 then 'medium-value'
      when total_transaction_value >= 1000 then 'low-value'
      else 'minimal-value'
    end as value_tier
    , case
      when days_since_last_transaction <= 30 then 'active'
      when days_since_last_transaction <= 90 then 'at-risk'
      when days_since_last_transaction <= 180 then 'dormant'
      else 'churned'
    end as activity_status
    , case
      when total_transactions >= 50 then 'very frequent'
      when total_transactions >= 20 then 'frequent'
      when total_transactions >= 5 then 'occasional'
      else 'rare'
    end as frequency_tier
    , least(100, round(
      (total_transactions * 2) +
      (case when days_since_last_transaction <= 30 then 20 else 0 end) +
      (active_months * 3) +
      (payment_methods_used * 5)
    , 0)) as engagement_score
    , case
      when total_transactions > 1 and days_since_last_transaction <= 90 then true
      else false
    end as is_retained
  from customer_metrics
)

, final as (
  select
    *
    , concat(
        case activity_status
        when 'active' then 'r1'
        when 'at-risk' then 'r2'
        when 'dormant' then 'r3'
        else 'r4'
        end,
        '-',
        case frequency_tier
        when 'very frequent' then 'f1'
        when 'frequent' then 'f2'
        when 'occasional' then 'f3'
        else 'f4'
        end,
        '-',
        case value_tier
        when 'high-value' then 'm1'
        when 'medium-value' then 'm2'
        when 'low-value' then 'm3'
        else 'm4'
        end
    ) as rfm_segment
    , current_timestamp() as analysis_timestamp
from customer_classification
order by total_transaction_value desc
) 

select * from final 