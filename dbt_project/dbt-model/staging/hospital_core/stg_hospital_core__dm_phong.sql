with source as (
    -- Read raw room dimension data
    select * from {{ source('raw_hospital', 'dm_phong_iceberg') }}
),

parsed_rooms as (
    select
        id as phong_id,
        code_phong as room_code,
        ten as room_name,
        
        -- Keep department ID for future filtering on the dashboard
        khoa_id, 
        
        active as is_active,
        deleted as is_deleted
    from source
    -- Keep only active rooms
    where deleted = 0 
      and active = true
)

select * from parsed_rooms