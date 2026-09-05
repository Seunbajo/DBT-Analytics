{{
  config(
    materialized='table'
  )
}}

with customer_metrics as (
  select 
    ws.user_id,
    min(date(o.order_date)) as first_purchase_date,
    max(date(o.order_date)) as last_purchase_date,
    count(distinct o.order_id) as total_orders,
    sum(o.order_value) as total_revenue,
    round(avg(o.order_value), 2) as avg_order_value,
    date_diff(max(date(o.order_date)), min(date(o.order_date)), day) as customer_lifespan_days
    from {{ ref('stg_ec_website_sessions') }} ws
    inner join {{ ref('stg_ec_orders') }} o
    on ws.session_id = o.session_id
  where ws.user_id is not null
  group by ws.user_id
)

select 
  user_id,
  first_purchase_date,
  last_purchase_date,
  total_orders,
  total_revenue,
  avg_order_value,
  customer_lifespan_days,
  -- frequency segment
  case 
    when total_orders = 1 then 'One-time Buyer'
    when total_orders between 2 and 3 then 'Repeat Buyer'
    else 'Frequent Buyer'
  end as frequency_segment,
  -- monetary segment
  case 
    when total_revenue >= 500 then 'High Value'
    when total_revenue >= 200 then 'Medium Value'
    else 'Low Value'
  end as monetary_segment,
  -- purchase pattern
  case 
    when total_orders = 1 then 'Single Purchase'
    when customer_lifespan_days <= 7 then 'Quick Repeat (0-7 days)'
    when customer_lifespan_days <= 14 then 'Medium Repeat (8-14 days)'
    else 'Long Repeat (15+ days)'
  end as purchase_pattern
from customer_metrics