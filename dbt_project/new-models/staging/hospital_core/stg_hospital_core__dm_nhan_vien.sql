with source as (
    select * from {{ source('raw_hospital', 'dm_nhan_vien_iceberg') }}
),

parsed_specialties as (
    select
        id as nhan_vien_id,
        ten as doctor_name,
        hoc_ham_hoc_vi_id,
        ds_chuyen_khoa_id,
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
