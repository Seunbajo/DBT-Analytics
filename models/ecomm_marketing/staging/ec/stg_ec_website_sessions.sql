{{
    config(
        materialized = 'table'
        , tags = ['website_sessions']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'ec_website_sessions') }}
),

final as (
    select
        session_id
        , ad_id
        , user_id
        , session_start
        , pages_viewed
        , session_duration_seconds
        , converted
        , device
        , traffic_source
        , medium 
        , city
        , case
            when city in ("Lagos", "Abuja") then "Nigeria"
            when city = "Accra" then "Ghana"
            when city = "London" then "UK"
            when city = "New York" then "USA"
            when city = "Nairobi" then "Kenya"
            else null
            end as country
    from staging
)

select * from final