{{
    config(
        materialized = 'table'
        , tags = ['partner_wallet_balance']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'corridor_partner_wallet_balance') }}
),

final as (
    select * from staging
)

select * from final