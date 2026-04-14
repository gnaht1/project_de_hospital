with pharmacy_orders as (
    select
        dv.dich_vu_id,
        dv.dot_dieu_tri_id,
        cast(dv.order_time as timestamp) as order_time,
        dv.payment_status,
        dv.phieu_thu_id,
        dv.trang_thai_hoan,
        coalesce(dv.so_luong, 0) as quantity,
        (
            coalesce(dv.tien_bh_thanh_toan, 0) +
            coalesce(dv.tien_nb_cung_chi_tra, 0) +
            coalesce(dv.tien_nb_phu_thu, 0) +
            coalesce(dv.tien_nb_tu_tra, 0)
        ) as total_medication_amount
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    where dv.loai_dich_vu_id = 90
      and dv.is_active = true
      and dv.is_deleted = false
),

valid_payments as (
    select
        phieu_thu_id,
        cast(thoi_gian_thanh_toan as timestamp) as payment_time
    from {{ ref('stg_hospital_core__ct_phieu_thu') }}
    where active = true
      and deleted = 0
      and trang_thai_thanh_toan = 50
      and thoi_gian_thanh_toan is not null
),

pharmacy_enriched as (
    select
        po.dich_vu_id,
        po.dot_dieu_tri_id,
        po.order_time,
        cast(date_trunc('day', po.order_time) as date) as order_date,
        po.payment_status,
        po.trang_thai_hoan,
        po.quantity,
        po.total_medication_amount,
        vp.payment_time,
        cast(date_trunc('day', vp.payment_time) as date) as payment_date,
        cast(date_trunc('month', vp.payment_time) as date) as payment_month
    from pharmacy_orders po
    left join valid_payments vp
        on po.phieu_thu_id = vp.phieu_thu_id
),

calendar_dates as (
    select
        order_date as metric_date
    from pharmacy_enriched
    where order_date is not null

    union

    select
        payment_date as metric_date
    from pharmacy_enriched
    where payment_date is not null
),

pending_daily as (
    select
        order_date as metric_date,
        count(distinct case
            when coalesce(trang_thai_hoan, 0) <> 40
                 and coalesce(payment_status, 0) = 50
                 and payment_time is not null
            then dot_dieu_tri_id
        end) as pending_prescriptions
    from pharmacy_enriched
    where order_date is not null
    group by 1
),

dispensed_daily as (
    select
        payment_date as metric_date,
        count(distinct case
            when coalesce(trang_thai_hoan, 0) <> 40
            then dot_dieu_tri_id
        end) as dispensed_prescriptions_today,
        sum(case
            when coalesce(trang_thai_hoan, 0) <> 40
            then total_medication_amount
            else 0
        end) as medication_revenue_today
    from pharmacy_enriched
    where payment_date is not null
    group by 1
),

monthly_revenue as (
    select
        payment_month as month_start,
        sum(case
            when coalesce(trang_thai_hoan, 0) <> 40 then total_medication_amount
            else 0
        end) as medication_revenue_month
    from pharmacy_enriched
    where payment_month is not null
    group by 1
)

select
    c.metric_date,
    cast(date_trunc('month', c.metric_date) as date) as month_start,
    coalesce(p.pending_prescriptions, 0) as pending_prescriptions,
    coalesce(d.dispensed_prescriptions_today, 0) as dispensed_prescriptions_today,
    coalesce(d.medication_revenue_today, 0) as medication_revenue_today,
    coalesce(m.medication_revenue_month, 0) as medication_revenue_month
from calendar_dates c
left join pending_daily p
    on c.metric_date = p.metric_date
left join dispensed_daily d
    on c.metric_date = d.metric_date
left join monthly_revenue m
    on cast(date_trunc('month', c.metric_date) as date) = m.month_start
