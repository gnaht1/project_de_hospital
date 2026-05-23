with source as (
    select * from {{ source('raw_hospital', 'ct_dv_kham_iceberg') }}
),

renamed_and_casted as (
    select
        id as dv_kham_id,
        nb_dot_dieu_tri_id,
        cast(bac_si_kham_id as int) as bac_si_kham_id,
        cast(bac_si_ket_luan_id as int) as bac_si_ket_luan_id,
        cast(dot_kham_moi as boolean) as dot_kham_moi,
        cast(thoi_gian_kham as timestamp) as thoi_gian_kham,
        cast(thoi_gian_ket_luan as timestamp) as thoi_gian_ket_luan,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        active as is_active,
        deleted as is_deleted
    from source
    where deleted = 0
      and active = true
)

select * from renamed_and_casted
