{{
  config(
    materialized='table'
  )
}}


with journey as (
    select
        t.order_id
        , t.channel
        , t.touchpoint_sequence
        , min(t.touchpoint_sequence) over (partition by t.order_id) as first_seq
        , max(t.touchpoint_sequence) over (partition by t.order_id) as last_seq
        , count(*) over (partition by t.order_id) as journey_length
    from {{ ref('stg_faker_customer_touchpoints') }} as t
    where t.order_id is not null
),

weighted as (
    select
        j.order_id
        , j.channel
        , case
            when j.journey_length = 1 then 1.0
            when j.journey_length = 2 then 0.5
            when j.touchpoint_sequence = j.first_seq then 0.4
            when j.touchpoint_sequence = j.last_seq then 0.4
            else 0.2 / nullif(j.journey_length - 2, 0)
        end as weight
    from journey as j
),

order_revenue as (
    select
        oi.order_id
        , sum(oi.quantity * oi.unit_price) as revenue
    from {{ ref('stg_faker_order_items') }} as oi
    inner join {{ ref('stg_faker_orders') }} as o
        on oi.order_id = o.order_id
    where o.order_status = 'Completed'
    group by oi.order_id
)

select
    w.channel
    , round(sum(w.weight), 4) as attributed_orders
    , round(sum(r.revenue * w.weight), 2) as attributed_revenue
from weighted as w
inner join order_revenue as r
    on w.order_id = r.order_id
group by w.channel
order by attributed_revenue desc