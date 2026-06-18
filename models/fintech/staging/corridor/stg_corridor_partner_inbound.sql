{{
    config(
        materialized = 'table'
        , tags = ['partner_inbound']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'corridor_partner_inbound') }}
),

final as (
    select * from staging
)

select * from final