{{
    config(
        materialized = 'table'
        , tags = ['outbound_txns']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'corridor_outbound_txns') }}
),

final as (
    select * from staging
)

select * from final