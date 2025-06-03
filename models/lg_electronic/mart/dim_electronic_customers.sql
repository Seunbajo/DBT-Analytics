{{
    config(
        materialized = 'table'
        , tags = ['exchange_rate']
    )
}}

with staging as (
    select * from {{ ref('stg_electronic_customers')}}
)

, final as (
    select
        customer_id
        , name as customer_name
        , gender
        , city
        , state_code
        , state
        , country
        , continent
        , birthday
    from staging
)

select * from final