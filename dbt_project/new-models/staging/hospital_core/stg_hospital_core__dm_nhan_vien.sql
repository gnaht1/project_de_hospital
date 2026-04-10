with source as (
    select * from {{ source('raw_hospital', 'dm_nhan_vien_iceberg') }}
),

parsed_specialties as (
    select
        id as nhan_vien_id,
        ten as doctor_name,
        
        -- Restore the academic title ID for the HR dashboard
        hoc_ham_hoc_vi_id,
        
        -- Remove brackets '[' and ']', split by comma, and take the first element (index 0)
        -- Cast the result to INT to ensure safe JOINs later
        cast(
            split(regexp_replace(ds_chuyen_khoa_id, '\\[|\\]', ''), ',')[0] 
            as int
        ) as primary_chuyen_khoa_id,
        
        cast(created_at as timestamp) as joined_at,
        active as is_active,
        deleted as is_deleted
    from source
    -- Keep only active doctors
    where deleted = 0 
      and active = true
)

select * from parsed_specialties