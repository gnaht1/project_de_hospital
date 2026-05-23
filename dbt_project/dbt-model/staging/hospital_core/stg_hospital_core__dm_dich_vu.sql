WITH source AS (
    SELECT * FROM {{ source('raw_hospital', 'dm_dich_vu_iceberg') }}
)

SELECT 
    id AS dich_vu_id,
    ten AS ten_dich_vu,
    nhom_dich_vu_cap1_id,
    loai_dich_vu AS loai_dich_vu_id
FROM source