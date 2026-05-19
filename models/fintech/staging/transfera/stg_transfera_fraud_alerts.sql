{{
    config(
        materialized = 'table'
        , tags = ['fraud_alerts']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'transfera_fraud_alerts') }}
),

final as (
    select * from staging
)

select * from final