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

, products as (
    select * from {{ ref('stg_vendease_products') }}
)

, product_category as (
    select * from {{ ref('stg_vendease_product_category') }}
)

, sellers as (
    select * from {{ ref('stg_vendease_sellers') }}
)

, order_payment as (
    select * from {{ ref('stg_vendease_order_payments') }}
)
-- Model to customers, order and prices  

, final as (
    select 
    customers.customer_unique_id
    , customers.customer_city
    , order_items.order_id 
    , products.product_category_name
    , order_items.price
    , sellers.seller_id
    , orders.order_status
    , orders.order_purchase_timestamp
    , order_payment.payment_type
    , order_items.freight_value
    , order_payment.payment_value
    from 
    order_items 
    left join products on order_items.product_id = products.product_id
    left join sellers on order_items.seller_id = sellers.seller_id
    left join orders on order_items.order_id = orders.order_id
    left join customers on orders.customer_id = customers.customer_id
    left join order_payment on orders.order_id = order_payment.order_id
)

select * from final
