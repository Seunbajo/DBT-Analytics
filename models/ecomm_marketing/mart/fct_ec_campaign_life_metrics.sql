{{
  config(
    materialized='table'
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
        c.campaign_id,
        c.campaign_name,
        c.channel,
        c.start_date as campaign_start_date,
        c.end_date as campaign_end_date,
        -- Campaign lifetime metrics
        count(distinct ap.ad_id) as total_ads,
        sum(ap.impressions) as total_impressions,
        sum(ap.clicks) as total_clicks,
        sum(ap.ad_spend) as total_ad_spend,
        safe_divide(sum(ap.clicks), sum(ap.impressions)) as overall_ctr,
        safe_divide(sum(ap.ad_spend), sum(ap.clicks)) as overall_cpc,
        min(ap.date) as first_ad_date,
        max(ap.date) as last_ad_date,
        date_diff(max(ap.date), min(ap.date), day) + 1 as campaign_duration_days
from campaigns c
left join ad_performance ap 
  on c.campaign_id = ap.campaign_id
group by 1,2,3,4,5
) 

select * from final