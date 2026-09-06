{{
    config(
        materialized = 'table'
        , tags = ['faker_customers']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_customers') }}
),

final as (
    select
        customer_id
        , email
        , phone
        , country
        , state
        , city
        , postal_code
        , signup_date
        , acquisition_channel
        , customer_segment
        , concat(first_name,' ', last_name) as customer_name
    from staging
)

select * from final