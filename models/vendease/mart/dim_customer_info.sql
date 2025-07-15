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

--get the day of week and name of day for each order
, orders as (
    select 
    order_id
    , customer_id
    , order_purchase_timestamp 
    , extract(DAYOFWEEK from order_purchase_timestamp) as day_of_week
    , format_date('%a', order_purchase_timestamp) as name_of_day
    from customer_orders
)

--group orders by customers and day of the week
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

-- rank every customers order
, rank_order as (
    select 
        *
        , rank() over (partition by customer_unique_id order by total_price desc) as rank_number
    from highest_purchase_day 
)

--get the third order value for each customer
, third_order_value as (
    select 
        customer_unique_id
        , total_price as third_order_v
    from rank_order
    where rank_number = 3
)

, final as (
    select 
    rank_order.*
    , coalesce(third_order_value.third_order_v, 0) as third_value
    from rank_order
    left join third_order_value
    on rank_order.customer_unique_id = third_order_value.customer_unique_id
    order by third_value desc
)

select * from final