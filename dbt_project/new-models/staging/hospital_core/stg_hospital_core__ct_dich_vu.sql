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
        active as is_active,
        deleted as is_deleted
    from source
    -- Filter out deleted/inactive records early
    where deleted = 0 
      and active = true
)

select * from renamed_and_casted