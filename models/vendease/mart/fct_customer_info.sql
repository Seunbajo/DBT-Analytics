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

--model to aggregate products bought by city

, aggregate_func as (
  select 
    customers.customer_unique_id
    , customers.customer_city
    , sum(order_items.price) as total_price
    , avg(order_items.price) as avg_price
    , count(orders.order_id) as count_of_orders
  from order_items
  left join orders
  on order_items.order_id = orders.order_id 
  left join customers 
  on orders.customer_id = customers.customer_id
  group by customers.customer_unique_id, customers.customer_city
)

, final as (
    select 
    * 
    from aggregate_func
)

select * from final
order by total_price desc