with source as (
    -- Read raw department data
    select * from {{ source('raw_hospital', 'dm_khoa_iceberg') }}
)

select
    id as khoa_id,
    ten as department_name,
    active as is_active,
    deleted as is_deleted
from source
-- Keep only active departments
where deleted = 0 
  and active = true