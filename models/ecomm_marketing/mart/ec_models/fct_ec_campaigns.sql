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

, final as (
    select
        ad_performance.ad_id
        , ad_performance.date
        , ad_performance.impressions
        , ad_performance.clicks
        , ad_performance.ad_spend
        , campaigns.campaign_name
        , campaigns.channel
        , campaigns.start_date
        , campaigns.end_date
    from ad_performance
    left join campaigns
       on ad_performance.campaign_id = campaigns.campaign_id
)

select * from final