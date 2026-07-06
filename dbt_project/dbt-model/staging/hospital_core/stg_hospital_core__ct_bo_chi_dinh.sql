with source as (
    select * from {{ source('raw_hospital', 'ct_bo_chi_dinh_iceberg') }}
),

renamed_and_casted as (
    select
        id as nb_bo_chi_dinh_id,
        nb_dot_dieu_tri_id,
        bo_chi_dinh_id,
        cast(thoi_gian_chi_dinh as timestamp) as order_set_time,
        active as is_active,
        deleted as is_deleted,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where deleted = 0
      and active = true
      and bo_chi_dinh_id is not null
)

select * from renamed_and_casted
