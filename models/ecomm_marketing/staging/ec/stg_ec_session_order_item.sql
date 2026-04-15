{{
    config(
        materialized = 'table'
        , tags = ['ec_session_order_item']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'ec_session_order_item') }}
),

final as (
    select * from staging
)

select * from final