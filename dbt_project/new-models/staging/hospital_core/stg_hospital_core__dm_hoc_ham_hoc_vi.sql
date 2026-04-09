with source as (
    -- Read raw academic titles/degrees
    select * from {{ source('raw_hospital', 'dm_hoc_ham_hoc_vi_iceberg') }}
)

select
    id as hoc_ham_hoc_vi_id,
    ten as title_name,
    code_hoc_ham as title_code
from source