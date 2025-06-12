{{
    config(
        materialized = 'table'
        , tags = ['exchange_rate']
    )
}}

with sales as (
    select * from {{ ref('stg_electronic_sales') }}
)

, products as (
    select * from {{ ref('stg_electronic_products') }}
)

, stores as (
    select * from {{ ref('stg_electronic_stores') }}
)

, customers as (
    select * from {{ ref('dim_electronic_customers') }}
)

, final as (
    select
        sales.customer_id
        , customers.customer_name
        , sales.order_number
        , sales.order_date
        , sales.delivery_date
        , sales.quantity
        , products.product_name
        , products.subcategory
        , products.category
        , stores.state
        , stores.country
        , customers.continent
        , customers.city
        , customers.gender
    from sales
    left join stores on sales.store_id = stores.store_id
    left join products on sales.product_id = products.product_id
    left join customers on sales.customer_id = customers.customer_id
)

select * from final