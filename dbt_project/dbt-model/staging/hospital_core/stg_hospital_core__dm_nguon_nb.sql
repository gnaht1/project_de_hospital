with source as (
    select * from {{ source('raw_hospital', 'dm_nguon_nb_iceberg') }}
),

renamed_and_casted as (
    select
        id as nguon_nb_id,
        code_nguon_nb,
        ten as ten_nguon_nb,
        nguoi_gioi_thieu as has_referrer,
        nhom_nguon,
        active as is_active,
        deleted as is_deleted,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where deleted = 0
      and active = true
)

select * from renamed_and_casted
