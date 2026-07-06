with attributed_visits as (
    select * from {{ ref('int_marketing__referral_attribution') }}
),

receipts as (
    select * from {{ ref('int_finance__receipt_transactions') }}
),

referrer_receipts as (
    select
        r.phieu_thu_id,
        r.nb_dot_dieu_tri_id,
        av.nb_thong_tin_id,
        av.nguoi_gioi_thieu_id,
        av.code_nguoi_gioi_thieu,
        av.ten_nguoi_gioi_thieu,
        av.nguon_nb_id,
        av.code_nguon_nb,
        av.ten_nguon_nb,
        r.payment_time,
        r.payment_date,
        r.receipt_amount,
        r.refund_amount,
        r.net_receipt_amount
    from receipts r
    inner join attributed_visits av
        on r.nb_dot_dieu_tri_id = av.dot_dieu_tri_id
),

referrer_revenue as (
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
        nguoi_gioi_thieu_id,
        code_nguoi_gioi_thieu,
        ten_nguoi_gioi_thieu,
        nguon_nb_id,
        code_nguon_nb,
        ten_nguon_nb,

        receipt_amount as gross_revenue,
        refund_amount,
        net_receipt_amount as net_revenue
    from referrer_receipts
)

select *
from referrer_revenue
