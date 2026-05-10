with source as (
    select * from {{ source('raw_hospital', 'dm_chuyen_khoa_iceberg') }}
)

select
    id as chuyen_khoa_id,
    code_chuyen_khoa as specialty_code,
    ten as specialty_name,
    active as is_active,
    deleted as is_deleted
from source
-- Lọc bỏ các chuyên khoa đã bị xóa hoặc ngừng hoạt động
where deleted = 0 
  and active = true