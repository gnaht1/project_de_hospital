with source as (
    select * from {{ source('raw_hospital', 'ct_kham_suc_khoe_iceberg') }}
),

renamed_and_casted as (
    select
        id as kham_suc_khoe_id,
        hop_dong_ksk_id as hop_dong_id,
        ma_nhan_vien,
        chuc_vu,
        phong_ban,
        dia_diem_kham,
        ngoai_vien as is_offsite,
        cast(trang_thai as int) as trang_thai_ksk,
        cast(tu_thoi_gian_kham as timestamp) as thoi_gian_bat_dau_kham,
        cast(den_thoi_gian_kham as timestamp) as thoi_gian_ket_thuc_kham,
        cast(thoi_gian_hoan_thanh as timestamp) as thoi_gian_hoan_thanh,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
)

select * from renamed_and_casted
