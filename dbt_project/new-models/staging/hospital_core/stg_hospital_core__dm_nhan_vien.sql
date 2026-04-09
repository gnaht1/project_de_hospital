with source as (
    -- Read raw employee data
    select * from {{ source('raw_hospital', 'dm_nhan_vien_iceberg') }}
),

renamed as (
    select
        id as nhan_vien_id,
        hoc_ham_hoc_vi_id,
        -- Extract the timestamp when the employee joined/was created
        cast(created_at as timestamp) as joined_at,
        active as is_active,
        deleted as is_deleted
    from source
    -- Keep only currently active employees for this snapshot
    where deleted = 0 
      and active = true
)

select * from renamed