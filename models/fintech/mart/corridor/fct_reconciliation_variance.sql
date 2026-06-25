{{
  config(
    materialized='table'
  )
}}

with cauridor_outbound_txn as (
    select
        *
        -- , row_number() over (partition by txn_id order by date_sent desc) as row_cc
    from {{ ref('stg_corridor_outbound_txns') }}
),

partner_soa as (
    select * except(row_tt)
    from (
        select
            *,
            row_number() over (
                partition by cauridor_txn_id
                order by partner_ref
            ) as row_tt
        from {{ ref('stg_corridor_partner_inbound') }}
    )
    where row_tt = 1
),

trans as (
    select
        c.txn_id,
        c.amount_ngn as cauridor_amount_ngn,
        s.amount_ngn as soa_amount_ngn,
        c.date_sent,
        s.date_received,
        c.sender_imto,
        c.status_our_system,
        c.beneficiary_ref,
        s.status_partner,
        s.cauridor_txn_id,
        s.partner_ref,
        date_diff(s.date_received, c.date_sent, day) as days_diff
    from cauridor_outbound_txn as c
    full join partner_soa as s
        on c.txn_id = s.cauridor_txn_id
    -- where s.row_tt = 1
    -- and c.row_cc = 1
),

duplicate_txns as (
    select
        cauridor_txn_id
    from {{ ref('stg_corridor_partner_inbound') }}
    where cauridor_txn_id is not null
    group by cauridor_txn_id
    having count(*) > 1
),

classify as (
    select
        t.*,
        case
            when t.txn_id in (
                select cauridor_txn_id
                from duplicate_txns
            ) then 'Duplicated on partner side'

            when t.cauridor_amount_ngn = t.soa_amount_ngn
                and t.txn_id = t.cauridor_txn_id
                and t.date_sent = t.date_received
            then 'Received both sides'

            when t.cauridor_amount_ngn > t.soa_amount_ngn
                and t.txn_id = t.cauridor_txn_id
                and t.date_sent = t.date_received
            then 'Partial Settlement'

            when t.cauridor_amount_ngn > t.soa_amount_ngn
                or t.soa_amount_ngn is null
            then 'Missing on partner side'

            when t.soa_amount_ngn > t.cauridor_amount_ngn
                or t.cauridor_amount_ngn is null
            then 'Prior Period item'

            when t.cauridor_amount_ngn < t.soa_amount_ngn
                and t.txn_id = t.cauridor_txn_id
            then 'Over settlement'

            when t.txn_id = t.cauridor_txn_id
                and t.cauridor_amount_ngn = t.soa_amount_ngn
                and t.date_received > t.date_sent
            then 'Timing difference (t+1 or later)'

            else 'Unclassified'
        end as classify_txn
    from trans t
)

select
    txn_id,
    cauridor_txn_id,
    partner_ref,
    date_sent,
    date_received,
    days_diff,
    cauridor_amount_ngn,
    soa_amount_ngn,
    round(cauridor_amount_ngn - soa_amount_ngn, 2) as amount_difference,
    status_our_system,
    status_partner,
    sender_imto,
    classify_txn
from classify
-- where classify_txn != 'Received both sides'
order by classify_txn, date_sent, txn_id