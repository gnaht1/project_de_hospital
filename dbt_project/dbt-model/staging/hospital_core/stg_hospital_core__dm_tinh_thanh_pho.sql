with source as (
    select * from {{ source('raw_hospital', 'dm_tinh_thanh_pho_iceberg') }}
),

renamed_and_casted as (
    select
        id as tinh_thanh_pho_id,
        code_province,
        ten as tinh_thanh_pho_name,
        ma_tcqg as tinh_thanh_pho_ma_tcqg,
        active as is_active,
        deleted as is_deleted,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where deleted = 0
      and active = true
)

select * from renamed_and_casted
