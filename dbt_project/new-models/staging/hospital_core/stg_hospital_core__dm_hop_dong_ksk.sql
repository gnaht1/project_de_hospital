with source as (
    -- Read raw contract data
    select * from {{ source('raw_hospital', 'dm_hop_dong_ksk_iceberg') }}
),

renamed_and_casted as (
    select
        id as hop_dong_id,
        code_hop_dong,
        ten as ten_hop_dong,
        so_hop_dong,
        cast(ngay_hieu_luc as timestamp) as ngay_hieu_luc,
        cast(thoi_gian_thanh_ly as timestamp) as thoi_gian_thanh_ly,
        cast(coalesce(trang_thai, 0) as int) as trang_thai_hop_dong,
        coalesce(tien_chua_thanh_toan, 0) as tien_chua_thanh_toan,
        coalesce(tien_da_thanh_toan, 0) as tien_da_thanh_toan,
        coalesce(tien_du_kien, 0) as tien_du_kien,
        coalesce(tien_du_kien_sau_giam, 0) as tien_du_kien_sau_giam,
        coalesce(tien_giam_gia, 0) as tien_giam_gia,
        coalesce(tien_mien_giam_dich_vu, 0) as tien_mien_giam_dich_vu,
        coalesce(tien_mien_giam_hop_dong, 0) as tien_mien_giam_hop_dong,
        coalesce(tien_thuc_te, 0) as tien_thuc_te,
        coalesce(tien_thuc_te_sau_giam, 0) as tien_thuc_te_sau_giam,
        coalesce(phan_tram_mien_giam, 0) as phan_tram_mien_giam,
        cast(coalesce(chot_thanh_toan_dv_ksk, 0) as int) as chot_thanh_toan_dv_ksk,
        nguoi_gioi_thieu_id,
        nguon_nb_id,
        created_at,
        updated_at,
        active as is_active,
        deleted as is_deleted
    from source
    -- Filter out soft-deleted records early
    where deleted = 0 
      and active = true
)

select * from renamed_and_casted
