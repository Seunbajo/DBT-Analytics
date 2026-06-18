{{
    config(
        materialized = 'table'
        , tags = ['partner_outflow']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'corridor_partner_outflow') }}
),

final as (
    select * from staging
)

select * from final