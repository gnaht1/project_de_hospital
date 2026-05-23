with valid_payments as (
    select
        phieu_thu_id,
        cast(thoi_gian_thanh_toan as timestamp) as payment_time
    from {{ ref('stg_hospital_core__ct_phieu_thu') }}
    where active = true
      and deleted = 0
      and trang_thai_thanh_toan = 50
      and thoi_gian_thanh_toan is not null
),

service_lines as (
    select
        dich_vu_id,
        phieu_thu_id,
        loai_dich_vu_id,
        trang_thai_hoan,
        (
            coalesce(tien_bh_thanh_toan, 0) +
            coalesce(tien_nb_cung_chi_tra, 0) +
            coalesce(tien_nb_phu_thu, 0) +
            coalesce(tien_nb_tu_tra, 0)
        ) as total_amount
    from {{ ref('stg_hospital_core__ct_dich_vu') }}
    where is_active = true
      and is_deleted = false
      and phieu_thu_id is not null
),

service_types as (
    select
        loai_dich_vu_id,
        service_type_name
    from {{ ref('stg_hospital_core__dm_loai_dich_vu') }}
),

paid_service_transactions as (
    select
        sl.dich_vu_id,
        vp.phieu_thu_id,
        vp.payment_time,
        cast(date_trunc('day', vp.payment_time) as date) as payment_date,
        cast(date_trunc('month', vp.payment_time) as date) as payment_month,
        sl.loai_dich_vu_id,
        st.service_type_name,
        sl.total_amount
    from service_lines sl
    inner join valid_payments vp
        on sl.phieu_thu_id = vp.phieu_thu_id
    left join service_types st
        on sl.loai_dich_vu_id = st.loai_dich_vu_id
    where coalesce(sl.trang_thai_hoan, 0) <> 40
)

select * from paid_service_transactions
