{{
    config(
        materialized = 'table'
        , tags = ['ec_order_items']
    )
}}

with order_items as (
    select * from {{ ref('stg_ec_order_items') }}
)

, orders as (
    select * from {{ ref('stg_ec_orders') }}
)

, products as (
    select * from {{ ref('stg_ec_products') }}
)

, final as (
    select
        orders.order_id
        , orders.order_date
        , cast(orders.order_date as date) as order_created_date
        , order_items.order_item_id
        , order_items.unit_price
        , order_items.unit_cost
        , order_items.profit
        , products.sku
        , products.category
    from order_items
    left join orders
        on order_items.order_id = orders.order_id
    left join products
        on order_items.product_id = products.product_id
    
)

select * from final