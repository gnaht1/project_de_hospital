with source as (
    select * from {{ source('raw_hospital', 'ct_dv_kham_ket_luan_iceberg') }}
),

renamed_and_casted as (
    select
        id as dv_kham_ket_luan_id,
        nb_dot_dieu_tri_id,
        cast(huong_dieu_tri as int) as huong_dieu_tri_id,
        cast(ket_qua_dieu_tri as int) as ket_qua_dieu_tri_id,
        cast(thoi_gian_ket_luan as timestamp) as thoi_gian_ket_luan,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
)

select * from renamed_and_casted
