with source as (
    select * from {{ source('raw_hospital', 'dm_nguoi_gioi_thieu_iceberg') }}
),

renamed_and_casted as (
    select
        id as nguoi_gioi_thieu_id,
        code_nguoi_gioi_thieu,
        ten as ten_nguoi_gioi_thieu,
        ds_nguon_nb_id,
        active as is_active,
        deleted as is_deleted,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where deleted = 0
      and active = true
)

select * from renamed_and_casted
