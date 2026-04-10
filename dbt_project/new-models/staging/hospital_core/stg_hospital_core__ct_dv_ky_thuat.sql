with source as (
    select * from {{ source('raw_hospital', 'ct_dv_ky_thuat_iceberg') }}
),

renamed_and_casted as (
    select
        -- In a 1:1 inheritance model, the ID itself is the service ID
        id as dich_vu_id,
        
        phong_thuc_hien_id,
        trang_thai,
        
        active as is_active,
        deleted as is_deleted
    from source
    where deleted = 0 
      and active = true
)

select * from renamed_and_casted