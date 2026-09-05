{{
    config(
        materialized = 'table'
        , tags = ['faker_ad_spend']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_ad_spend') }}
),

final as (
    select * from staging
)

select * from final