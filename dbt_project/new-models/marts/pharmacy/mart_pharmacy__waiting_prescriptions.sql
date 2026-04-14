with prescription_lines as (
    select * from {{ ref('int_pharmacy__prescription_lines') }}
),

aggregated_prescriptions as (
    select
        md5(
            concat_ws(
                '||',
                cast(dot_dieu_tri_id as string),
                cast(order_date as string),
                cast(coalesce(prescribing_doctor_id, -1) as string)
            )
        ) as prescription_id,
        order_date as metric_date,
        dot_dieu_tri_id,
        patient_id,
        coalesce(patient_code, cast(dot_dieu_tri_id as string)) as patient_code,
        coalesce(patient_name, 'Không rõ') as patient_name,
        coalesce(prescribing_doctor_name, 'Không rõ') as prescribing_doctor_name,
        count(dich_vu_id) as item_count,
        sum(line_amount) as total_amount,
        max(coalesce(payment_status, 0)) as payment_status,
        max(case when payment_time is not null then 1 else 0 end) as has_valid_payment,
        max(case when coalesce(trang_thai_hoan, 0) = 40 then 1 else 0 end) as is_refunded,
        max(payment_time) as latest_payment_time
    from prescription_lines
    where order_date is not null
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    prescription_id,
    metric_date,
    dot_dieu_tri_id,
    patient_id,
    patient_code,
    patient_name,
    prescribing_doctor_name,
    item_count,
    total_amount,
    case
        when payment_status = 50 and has_valid_payment = 1 then 'Đã TT'
        else 'Chưa TT'
    end as payment_status_label,
    case
        when is_refunded = 1 then 'Hoàn trả'
        when payment_status = 0 then 'Chờ TT'
        when has_valid_payment = 1 then 'Chờ cấp'
        else 'Chờ xác nhận'
    end as fulfillment_status_label,
    latest_payment_time
from aggregated_prescriptions
