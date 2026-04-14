with receipts as (
    select
        phieu_thu_id,
        nb_dot_dieu_tri_id,
        cast(thoi_gian_thanh_toan as timestamp) as payment_time,
        cast(date_trunc('day', thoi_gian_thanh_toan) as date) as payment_date,
        cast(date_trunc('month', thoi_gian_thanh_toan) as date) as payment_month,
        cast(thoi_gian_tao_phieu as timestamp) as created_time,
        thu_ngan_id,
        coalesce(thanh_tien, 0) as receipt_amount,
        coalesce(tien_giam_gia, 0) as discount_amount,
        (
            coalesce(tien_mien_giam_dich_vu, 0) +
            coalesce(tien_mien_giam_phieu_thu, 0) +
            coalesce(tien_mien_giam_phieu_thu_nhap_vao, 0)
        ) as exemption_amount,
        coalesce(tien_hoan_tra, 0) as refund_amount,
        coalesce(phan_tram_mien_giam, 0) as discount_percent,
        loai_phieu_thu,
        loai_mien_giam
    from {{ ref('stg_hospital_core__ct_phieu_thu') }}
    where active = true
      and deleted = 0
      and trang_thai_thanh_toan = 50
      and thoi_gian_thanh_toan is not null
),

enriched_receipts as (
    select
        phieu_thu_id,
        nb_dot_dieu_tri_id,
        payment_time,
        payment_date,
        payment_month,
        created_time,
        thu_ngan_id,
        receipt_amount,
        discount_amount,
        exemption_amount,
        refund_amount,
        discount_amount + exemption_amount as total_discount_and_exemption_amount,
        receipt_amount - refund_amount as net_receipt_amount,
        discount_percent,
        loai_phieu_thu,
        loai_mien_giam
    from receipts
)

select * from enriched_receipts
