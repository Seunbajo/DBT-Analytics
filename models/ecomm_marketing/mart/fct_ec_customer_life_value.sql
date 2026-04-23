{{
  config(
    materialized='table'
  )
}}

with customer_orders as (
  select 
    ws.user_id,
    o.order_id,
    o.order_date,
    o.order_value,
    row_number() over (partition by ws.user_id order by o.order_date) as order_number
  from {{ ref('stg_ec_website_sessions') }} ws
  inner join {{ ref('stg_ec_orders') }} o
    on ws.session_id = o.session_id
  where ws.user_id is not null
)

select 
  user_id,
  count(*) as total_orders,
  sum(order_value) as lifetime_value,
  max(case when order_number = 1 then order_value end) as first_order_value,
  max(case when order_number = 1 then order_date end) as first_order_date,
  max(order_date) as last_order_date,
  round(avg(order_value), 2) as avg_order_value,
  date_diff(
    max(date(order_date)), 
    max(case when order_number = 1 then date(order_date) end), 
    day
  ) as days_active
from customer_orders
group by user_id