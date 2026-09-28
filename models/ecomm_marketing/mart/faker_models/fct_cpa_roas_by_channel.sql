{{
  config(
    materialized='table'
  )
}}

with revenue_by_channel as (
    select
        o.last_click_channel as channel
        , count(distinct o.order_id) as orders
        , sum(oi.quantity * oi.unit_price) as revenue
    from {{ ref('stg_faker_orders') }} as o
    inner join {{ ref('stg_faker_order_items') }} as oi
        on o.order_id = oi.order_id
    where o.order_status = 'Completed'
    group by o.last_click_channel
),

spend_by_channel as (
    select
        camp.channel
        , sum(a.spend) as spend
    from {{ ref('stg_faker_ad_spend') }} as a
    inner join {{ ref('stg_faker_campaigns') }} as camp
        on a.campaign_id = camp.campaign_id
    group by camp.channel
)

select
    r.channel
    , r.orders
    , round(r.revenue, 2) as revenue
    , round(coalesce(s.spend, 0), 2) as spend
    , round(coalesce(s.spend, 0) / nullif(r.orders, 0), 2) as cpa
    , round(r.revenue / nullif(s.spend, 0), 2) as roas
from revenue_by_channel as r
left join spend_by_channel as s
    on r.channel = s.channel
order by revenue desc