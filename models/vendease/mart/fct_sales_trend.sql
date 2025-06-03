{{
    config(
        materialized = 'table'
        , tags = ['vendease_customer_info']
    )
}}

with orders as (
    select * from {{ ref('stg_vendease_orders') }}
)

, order_items as (
    select * from {{ ref('stg_vendease_order_items') }}
)

, final as (
    select 
    extract(YEAR from order_purchase_timestamp) as year
    , format_date('%b', order_purchase_timestamp) as name_of_month
    , sum(order_items.price) as total_sales
    from orders
    join order_items on orders.order_id = order_items.order_id
    group by year, name_of_month
    order by year, name_of_month
)

select * from final