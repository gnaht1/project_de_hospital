with source as (
    -- Read service categories
    select * from {{ source('raw_hospital', 'dm_loai_dich_vu_iceberg') }}
)

select
    id as loai_dich_vu_id,
    ten as service_type_name
from source