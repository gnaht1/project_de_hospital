with source as (
    -- Read raw contract data
    select * from {{ source('raw_hospital', 'dm_hop_dong_ksk_iceberg') }}
),

renamed_and_casted as (
    select
        id as hop_dong_id,
        
        -- Parse date
        created_at as ngay_hieu_luc,
        
        -- Audit fields
        active as is_active,
        deleted as is_deleted
        
    from source
    -- Filter out soft-deleted records early
    where deleted = 0 
      and active = true
)

select * from renamed_and_casted