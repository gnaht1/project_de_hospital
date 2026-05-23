with source as (
    select * from {{ source('raw_hospital', 'dm_doi_tuong_kcb_iceberg') }}
)

select
    id as doi_tuong_kcb_id,
    ten as patient_type_name
from source