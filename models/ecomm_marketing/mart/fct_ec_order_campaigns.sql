{{
  config(
    materialized='table',
    partition_by={
      "field": "session_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['campaign_id', 'channel', 'country', 'device']
  )
}}

with sessions as (
  select
    session_id,
    ad_id,
    user_id,
    session_start,
    date(session_start) as session_date,
    pages_viewed,
    session_duration_seconds,
    cast(converted as string) as converted,
    country,
    city,
    device,
    traffic_source,
    medium
  from {{ ref('stg_ec_website_sessions') }}
)

, orders as (
  select
    order_id,
    session_id,
    order_value,
    order_date
  from {{ ref('stg_ec_orders') }}
)

, order_items as (
  select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    unit_cost,
    revenue,
    cost,
    profit
  from {{ ref('stg_ec_order_items') }}
)

, products as (
  select
    product_id,
    sku,
    category,
    price as product_list_price,
    cost as product_cost,
    margin as product_margin
  from {{ ref('stg_ec_products') }}
)

, ad_performance as (
  select
    ad_id,
    campaign_id,
    date as ad_date,
    impressions,
    clicks,
    ad_spend
  from {{ ref('stg_ec_ad_performance') }}
)

, campaigns as (
  select
    campaign_id,
    campaign_name,
    channel,
    start_date as campaign_start_date,
    end_date as campaign_end_date
  from {{ ref('stg_ec_campaigns') }}
)

-- Aggregate ad spend to session level (daily ad spend / daily clicks * session)
, session_ad_metrics as (
  select
    s.session_id,
    s.ad_id,
    ap.campaign_id,
    ap.impressions,
    ap.clicks,
    ap.ad_spend,
    -- Calculate cost per click for attribution
    safe_divide(ap.ad_spend, ap.clicks) as cost_per_click
  from sessions s
  left join ad_performance ap 
    on s.ad_id = ap.ad_id 
    and DATE(s.session_start) = ap.ad_date
)

-- Create session-order-item grain
, session_order_items as (
  select
    s.session_id,
    s.ad_id,
    s.user_id,
    s.session_start,
    s.session_date,
    s.pages_viewed,
    s.session_duration_seconds,
    s.converted,
    s.country,
    s.city,
    s.device,
    s.traffic_source,
    s.medium,
    -- Order level
    o.order_id,
    o.order_value,
    o.order_date,
    -- Order item level
    oi.order_item_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.unit_cost,
    oi.revenue,
    oi.cost,
    oi.profit,
    -- Product attributes
    p.sku,
    p.category as product_category,
    p.product_list_price,
    p.product_cost,
    p.product_margin,
    -- Ad & Campaign metrics
    sam.campaign_id,
    sam.impressions,
    sam.clicks,
    sam.ad_spend,
    sam.cost_per_click,
    -- Campaign attributes
    c.campaign_name,
    c.channel,
    c.campaign_start_date,
    c.campaign_end_date
  from sessions s
  left join orders o on s.session_id = o.session_id
  left join order_items oi on o.order_id = oi.order_id
  left join products p on oi.product_id = p.product_id
  left join session_ad_metrics sam on s.session_id = sam.session_id
  left join campaigns c on sam.campaign_id = c.campaign_id
)

, final as (
    select
        -- Primary Keys
        session_id,
        order_id,
        order_item_id,
        
        -- Foreign Keys
        ad_id,
        campaign_id,
        user_id,
        product_id,
        
        -- Dates & Timestamps
        session_start,
        session_date,
        order_date,
        date(order_date) as order_date_only,
        campaign_start_date,
        campaign_end_date,
        
        -- Dimensions
        campaign_name,
        channel,
        product_category,
        sku,
        country,
        city,
        device,
        traffic_source,
        medium,
        
        -- Session Metrics
        pages_viewed,
        session_duration_seconds,
        converted,
        
        -- Ad Performance Metrics (daily aggregates)
        impressions,
        clicks,
        ad_spend,
        cost_per_click,
        
        -- Transaction Metrics (item level)
        quantity,
        unit_price,
        unit_cost,
        revenue,
        cost,
        profit,
        
        -- Product Reference Prices
        product_list_price,
        product_cost,
        product_margin,
        
        -- Order Level Metrics
        order_value,
        
        -- Calculated Metrics for Attribution
        case 
            when order_id is not null then cost_per_click 
            else 0 
        end as attributed_ad_cost,
        
        -- Conversion Flags
        case when order_id is not null then 1 else 0 end as has_order,
        case when order_item_id is not null then 1 else 0 end as has_order_item,
        
        -- Time to Conversion
        case
            when order_date is not null 
            then timestamp_diff(order_date, session_start, hour)
        end as hours_to_conversion,
        
        -- ROI Calculation Fields
        case 
            when cost_per_click > 0 
            then safe_divide(profit, cost_per_click) 
        end as roas_per_session
from session_order_items
)

select * from final
