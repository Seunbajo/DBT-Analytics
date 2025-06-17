{{
    config(
        materialized = 'table'
        , tags = ['exchange_rate']
    )
}}

with sales as (
    select 
        customer_id
        , currency_code
        , count(quantity) as quantity
    from {{ ref('stg_electronic_sales') }}
    group by customer_id, currency_code
)

, customers as (
    select * from {{ ref('dim_electronic_customers') }}
)

, final as (
    select 
        sales.customer_id
        , sales.currency_code
        , sales.quantity
        , customers.customer_name
        , customers.city
        , customers.state
        , customers.country
        , customers.gender
    from sales
    left join customers
        on sales.customer_id = customers.customer_id
)

select * from final