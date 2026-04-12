with source as (
    -- Read raw service details
    select * from {{ source('raw_hospital', 'ct_dich_vu_iceberg') }}
),

renamed_and_casted as (
    select
        id as dich_vu_id,
        loai_dich_vu as loai_dich_vu_id,
        -- Use indication time for volume tracking
        cast(thoi_gian_chi_dinh as timestamp) as order_time,
        tien_bh_thanh_toan,
        tien_nb_cung_chi_tra,
        tien_nb_phu_thu,
        tien_nb_tu_tra,
        phieu_thu_id,
        khoa_chi_dinh_id,
        nb_dot_dieu_tri_id as dot_dieu_tri_id,
        bac_si_chi_dinh_id,
        trang_thai_hoan,
        so_luong,
        gia_goc,
        cast(nb_chuyen_khoa_id as int) as nb_chuyen_khoa_id,
        active as is_active,
        deleted as is_deleted
    from source
    -- Filter out deleted/inactive records early
    where deleted = 0 
      and active = true
)

select * from renamed_and_casted