with receipt_transactions as (
    select * from {{ ref('int_finance__receipt_transactions') }}
),

calendar_dates as (
    select distinct
        payment_date as metric_date
    from receipt_transactions
    where payment_date is not null
),

daily_summary as (
    select
        payment_date as metric_date,
        count(distinct phieu_thu_id) as receipts_today,
        sum(receipt_amount) as gross_receipt_amount_today,
        sum(net_receipt_amount) as net_receipt_amount_today,
        sum(total_discount_and_exemption_amount) as total_discount_and_exemption_today,
        sum(refund_amount) as refund_amount_today
    from receipt_transactions
    where payment_date is not null
    group by 1
),

monthly_summary as (
    select
        payment_month as month_start,
        count(distinct phieu_thu_id) as receipts_in_month,
        sum(receipt_amount) as gross_receipt_amount_month,
        sum(net_receipt_amount) as net_receipt_amount_month,
        sum(total_discount_and_exemption_amount) as total_discount_and_exemption_month,
        sum(refund_amount) as refund_amount_month
    from receipt_transactions
    where payment_month is not null
    group by 1
)

select
    c.metric_date,
    cast(date_trunc('month', c.metric_date) as date) as month_start,
    date_format(cast(date_trunc('month', c.metric_date) as date), 'yyyy-MM') as stat_month,
    coalesce(d.receipts_today, 0) as receipts_today,
    coalesce(d.gross_receipt_amount_today, 0) as gross_receipt_amount_today,
    coalesce(d.net_receipt_amount_today, 0) as net_receipt_amount_today,
    coalesce(d.total_discount_and_exemption_today, 0) as total_discount_and_exemption_today,
    coalesce(d.refund_amount_today, 0) as refund_amount_today,
    coalesce(m.receipts_in_month, 0) as receipts_in_month,
    coalesce(m.gross_receipt_amount_month, 0) as gross_receipt_amount_month,
    coalesce(m.net_receipt_amount_month, 0) as net_receipt_amount_month,
    coalesce(m.total_discount_and_exemption_month, 0) as total_discount_and_exemption_month,
    coalesce(m.refund_amount_month, 0) as refund_amount_month
from calendar_dates c
left join daily_summary d
    on c.metric_date = d.metric_date
left join monthly_summary m
    on cast(date_trunc('month', c.metric_date) as date) = m.month_start
