with source as (
    select * from {{ source('raw_hospital', 'dm_quan_huyen_iceberg') }}
),

renamed_and_casted as (
    select
        id as quan_huyen_id,
        ten as quan_huyen_name,
        code_quan_huyen,
        ma_tcqg as quan_huyen_ma_tcqg,
        tinh_thanh_pho_id,
        active as is_active,
        deleted as is_deleted,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where deleted = 0
      and active = true
)

select * from renamed_and_casted
