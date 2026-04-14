with source as (
    select * from {{ source('raw_hospital', 'dm_bo_chi_dinh_iceberg') }}
),

renamed_and_casted as (
    select
        id as bo_chi_dinh_id,
        code_bo_chi_dinh,
        ten as ten_bo_chi_dinh,
        cast(hop_dong_ksk_id as int) as hop_dong_id,
        active as is_active,
        deleted as is_deleted,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where deleted = 0
      and active = true
)

select * from renamed_and_casted
