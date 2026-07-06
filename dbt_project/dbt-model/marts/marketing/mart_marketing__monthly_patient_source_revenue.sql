with attributed_visits as (
    select * from {{ ref('int_marketing__patient_source_attribution') }}
),

receipts as (
    select * from {{ ref('int_finance__receipt_transactions') }}
),

source_receipts as (
    select
        r.phieu_thu_id,
        r.nb_dot_dieu_tri_id,
        av.nb_thong_tin_id,
        av.nguon_nb_id,
        av.code_nguon_nb,
        av.ten_nguon_nb,
        av.has_referrer,
        av.nhom_nguon,
        r.payment_time,
        r.payment_date,
        r.receipt_amount,
        r.refund_amount,
        r.net_receipt_amount
    from receipts r
    inner join attributed_visits av
        on r.nb_dot_dieu_tri_id = av.dot_dieu_tri_id
    where av.has_referrer = true
),

source_revenue as (
    select
        cast(payment_time as timestamp) as stat_time,
        cast(payment_date as date) as stat_date,
        extract(year from payment_date) as stat_year,
        extract(month from payment_date) as stat_month,
        cast(date_trunc('week', cast(payment_date as timestamp)) as date) as stat_week,
        cast(date_trunc('month', cast(payment_date as timestamp)) as date) as stat_month_start,
        cast(date_trunc('quarter', cast(payment_date as timestamp)) as date) as stat_quarter_start,
        cast(date_trunc('year', cast(payment_date as timestamp)) as date) as stat_year_start,
        'T' || lpad(cast(extract(month from payment_date) as string), 2, '0') as month_label,

        phieu_thu_id,
        nb_dot_dieu_tri_id,
        nb_thong_tin_id,
        nguon_nb_id,
        code_nguon_nb,
        ten_nguon_nb,
        has_referrer,
        nhom_nguon,

        receipt_amount as gross_revenue,
        refund_amount,
        net_receipt_amount as net_revenue
    from source_receipts
)

select *
from source_revenue
