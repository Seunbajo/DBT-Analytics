{{
    config(
        materialized = 'table'
        , tags = ['vendease_customer_info']
    )
}}

with customers as (
    select * from {{ ref('stg_vendease_customers') }}
)

, customer_orders as (
    select * from {{ ref('stg_vendease_orders') }}
)

, order_items as (
    select * from {{ ref('stg_vendease_order_items') }}
)

, orders as (
    select 
    order_id
    , customer_id
    , order_purchase_timestamp 
    , extract(DAYOFWEEK from order_purchase_timestamp) as day_of_week
    , format_date('%a', order_purchase_timestamp) as name_of_day
    from customer_orders
)

, highest_purchase_day as (
    select
    customers.customer_unique_id
    , orders.day_of_week
    , orders.name_of_day
    , SUM(order_items.price) AS total_price
    from customers
    left join orders on orders.customer_id = customers.customer_id
    left join order_items ON order_items.order_id = orders.order_id
    group by customer_unique_id, day_of_week, name_of_day
)

, final as (
    select 
    * 
    , rank() over (partition by customer_unique_id order by total_price desc) as rank_number
    from highest_purchase_day 
)

select * from final