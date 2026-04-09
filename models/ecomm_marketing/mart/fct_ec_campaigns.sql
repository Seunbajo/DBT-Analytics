{{
    config(
        materialized = 'table'
        , tags = ['ec_campiagns']
    )
}}

with ad_performance as (
    select * from {{ ref('stg_ec_ad_performance') }}
)

, campaigns as (
    select * from {{ ref('stg_ec_campaigns') }}
)

, website_sessions as (
    select 
        ad_id
        , count(pages_viewed) as pages_viewed
        , sum(session_duration_seconds) as session_seconds
    from {{ ref('stg_ec_website_sessions') }}
    group by ad_id
)

, final as (
    select * from ad_performance
)

select * from final