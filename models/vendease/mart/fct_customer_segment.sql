{{
    config(
        materialized = 'table'
        , tags = ['vendease_customer_info']
    )
}}

with customers as (
    select * from {{ ref('stg_vendease_customers') }}
)

, orders as (
    select * from {{ ref('stg_vendease_orders') }}
)

, order_items as (
    select * from {{ ref('stg_vendease_order_items') }}
)

, rfm as (
    select 
        customers.customer_unique_id,
        max(orders.order_purchase_timestamp) as last_purchase_date,
        count(orders.order_id) as frequency,
        sum(order_items.price) as amount_spent
    from customers
    join orders on customers.customer_id = orders.customer_id
    join order_items on orders.order_id = order_items.order_id
    group by customer_unique_id
)

, final as (
    select 
    *, 
    case 
        when amount_spent between 10001 and 15000 then 'High Spender' 
        when amount_spent between 5001 and 10000 then 'High Medium Spender'
        when amount_spent between 3001 and 5000 then 'Medium Spender'
        when amount_spent between 1001 and 3000 then 'Low Medium Spender'
        when amount_spent between 0 and 1000 then 'Low Spender'
    end as buckets
from rfm
)

select * from final