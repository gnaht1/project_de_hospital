with medication_transactions as (
    select
        dv.dich_vu_id,
        dv.dot_dieu_tri_id,
        cast(dv.order_time as timestamp) as order_time,
        cast(date_trunc('day', dv.order_time) as date) as order_date,
        dv.bac_si_chi_dinh_id,
        dv.payment_status,
        dv.phieu_thu_id,
        dv.trang_thai_hoan,
        (
            coalesce(dv.tien_bh_thanh_toan, 0) +
            coalesce(dv.tien_nb_cung_chi_tra, 0) +
            coalesce(dv.tien_nb_phu_thu, 0) +
            coalesce(dv.tien_nb_tu_tra, 0)
        ) as line_amount
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

episodes as (
    select
        dot_dieu_tri_id,
        nb_thong_tin_id
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

patients as (
    select
        patient_id,
        patient_code,
        patient_name
    from {{ ref('stg_hospital_core__dm_benh_nhan') }}
),

doctors as (
    select
        nhan_vien_id,
        doctor_name
    from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

enriched_lines as (
    select
        mt.dich_vu_id,
        mt.dot_dieu_tri_id,
        mt.order_time,
        mt.order_date,
        ep.nb_thong_tin_id as patient_id,
        pt.patient_code,
        pt.patient_name,
        mt.bac_si_chi_dinh_id as prescribing_doctor_id,
        dc.doctor_name as prescribing_doctor_name,
        mt.payment_status,
        mt.phieu_thu_id,
        mt.trang_thai_hoan,
        mt.line_amount,
        vp.payment_time,
        cast(date_trunc('day', vp.payment_time) as date) as payment_date
    from medication_transactions mt
    left join valid_payments vp
        on mt.phieu_thu_id = vp.phieu_thu_id
    left join episodes ep
        on mt.dot_dieu_tri_id = ep.dot_dieu_tri_id
    left join patients pt
        on ep.nb_thong_tin_id = pt.patient_id
    left join doctors dc
        on mt.bac_si_chi_dinh_id = dc.nhan_vien_id
)

select
    dich_vu_id,
    dot_dieu_tri_id,
    order_time,
    order_date,
    patient_id,
    patient_code,
    patient_name,
    prescribing_doctor_id,
    coalesce(prescribing_doctor_name, 'Không rõ') as prescribing_doctor_name,
    payment_status,
    phieu_thu_id,
    trang_thai_hoan,
    line_amount,
    payment_time,
    payment_date
from enriched_lines
