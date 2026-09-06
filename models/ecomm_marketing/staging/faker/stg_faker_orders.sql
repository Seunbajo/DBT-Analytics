{{
    config(
        materialized = 'table'
        , tags = ['faker_orders']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_orders') }}
),

final as (
    select
        order_id
        , customer_id
        , cast(order_timestamp as timestamp) as order_timestamp
        , order_status
        , ship_country
        , ship_state
        , ship_city
        , ship_postal_code
        , discount_amount
        , shipping_cost
        , last_click_channel
        , last_click_campaign_id
    from staging
)

select * from final