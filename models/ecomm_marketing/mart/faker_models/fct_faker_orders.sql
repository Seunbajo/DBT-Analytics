{{
    config(
        materialized = 'table'
        , cluster_by = ['country', 'state', 'category']
        , partition_by = {
            'field': 'order_timestamp',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
        , tags = ['fct_orders']
    )
}}

with orders as (
    select * from {{ ref('stg_faker_orders') }}
    where order_status = 'Completed'
)

, order_items as (
    select * from {{ ref('stg_faker_order_items') }}
)

, customers as (
    select * from {{ ref('stg_faker_customers') }}
)

, products as (
    select * from {{ ref('stg_faker_products') }}
)

, campaigns as (
    select * from {{ ref('stg_faker_campaigns') }}
)

, final as (
    select
        customers.customer_name
        , customers.city
        , customers.country
        , customers.state
        , orders.order_status
        , orders.ship_country
        , orders.discount_amount
        , orders.shipping_cost
        , orders.order_timestamp
        , orders.last_click_channel
        , campaigns.campaign_name
        , campaigns.objective
        , order_items.quantity
        , order_items.unit_price
        , order_items.unit_cost
        , products.product_name
        , products.category
        , products.sub_category
    from order_items
    inner join products
        on order_items.product_id = products.product_id
    inner join orders
        on order_items.order_id = orders.order_id
    inner join customers
        on orders.customer_id = customers.customer_id
    inner join campaigns
        on orders.last_click_campaign_id = campaigns.campaign_id  
)

select * from final