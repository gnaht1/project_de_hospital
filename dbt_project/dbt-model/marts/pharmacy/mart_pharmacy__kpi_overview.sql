with pharmacy_orders as (
    select
        dv.dich_vu_id,
        dv.dot_dieu_tri_id,
        cast(dv.order_time as timestamp) as order_time,
        cast(date_trunc('day', dv.order_time) as date) as order_date,
        dv.payment_status,
        dv.phieu_thu_id,
        dv.trang_thai_hoan,
        coalesce(dv.so_luong, 0) as quantity,
        coalesce(dv.tien_bh_thanh_toan, 0) as insurance_covered_amount,
        (
            coalesce(dv.tien_bh_thanh_toan, 0) +
            coalesce(dv.tien_nb_cung_chi_tra, 0) +
            coalesce(dv.tien_nb_phu_thu, 0) +
            coalesce(dv.tien_nb_tu_tra, 0)
        ) as total_medication_amount
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    where dv.loai_dich_vu_id = 90
      and dv.is_active = true
      and dv.is_deleted = 0
),

valid_payments as (
    select
        phieu_thu_id,
        payment_time,
        cast(date_trunc('day', payment_time) as date) as payment_date,
        cast(date_trunc('month', payment_time) as date) as payment_month
    from (
        select
            phieu_thu_id,
            cast(thoi_gian_thanh_toan as timestamp) as payment_time,
            row_number() over (
                partition by phieu_thu_id
                order by cast(thoi_gian_thanh_toan as timestamp) desc
            ) as rn
        from {{ ref('stg_hospital_core__ct_phieu_thu') }}
        where active = true
          and deleted = 0
          and trang_thai_thanh_toan = 50
          and thoi_gian_thanh_toan is not null
    ) t
    where rn = 1
),

pharmacy_enriched as (
    select
        po.dich_vu_id,
        po.dot_dieu_tri_id,
        po.order_time,
        po.order_date,
        po.payment_status,
        po.trang_thai_hoan,
        po.quantity,
        po.total_medication_amount,
        po.insurance_covered_amount,
        vp.payment_time,
        vp.payment_date,
        vp.payment_month,
        case
            when coalesce(po.trang_thai_hoan, 0) = 40 then 1
            else 0
        end as is_refunded,
        case
            when coalesce(po.payment_status, 0) = 50 and vp.payment_time is not null then 1
            else 0
        end as is_paid
    from pharmacy_orders po
    left join valid_payments vp
        on po.phieu_thu_id = vp.phieu_thu_id
),

calendar_dates as (
    select order_date as metric_date
    from pharmacy_enriched
    where order_date is not null

    union

    select payment_date as metric_date
    from pharmacy_enriched
    where payment_date is not null
),

pending_current as (
    select
        order_date as metric_date,
        count(distinct case
            when is_refunded = 0 and is_paid = 1
            then dot_dieu_tri_id
        end) as pending_prescriptions
    from pharmacy_enriched
    where order_date is not null
    group by 1
),

dispensed_today as (
    select
        payment_date as metric_date,
        count(distinct case
            when is_refunded = 0 and is_paid = 1
            then dot_dieu_tri_id
        end) as dispensed_prescriptions_today
    from pharmacy_enriched
    where payment_date is not null
    group by 1
),

monthly_financials as (
    select
        payment_month as month_start,
        sum(case
            when is_refunded = 0 and is_paid = 1 then total_medication_amount
            else 0
        end) as medication_revenue_month,
        sum(case
            when is_refunded = 0 and is_paid = 1 then insurance_covered_amount
            else 0
        end) as insurance_covered_amount_month
    from pharmacy_enriched
    where payment_month is not null
    group by 1
)

select
    c.metric_date,
    cast(date_trunc('month', c.metric_date) as date) as month_start,
    coalesce(p.pending_prescriptions, 0) as pending_prescriptions,
    coalesce(d.dispensed_prescriptions_today, 0) as dispensed_prescriptions_today,
    coalesce(m.medication_revenue_month, 0) as medication_revenue_month,
    coalesce(m.insurance_covered_amount_month, 0) as insurance_covered_amount_month
from calendar_dates c
left join pending_current p
    on c.metric_date = p.metric_date
left join dispensed_today d
    on c.metric_date = d.metric_date
left join monthly_financials m
    on cast(date_trunc('month', c.metric_date) as date) = m.month_start
