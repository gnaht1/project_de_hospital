-- models/staging/stg_dm_loai_dich_vu.sql
-- Extract and standardize service category data
SELECT 
    sub_categories.service_category_id,
    sub_categories.category_name
FROM (
    SELECT 
        id AS service_category_id,
        ten AS category_name
    FROM {{ source('core_his', 'dm_loai_dich_vu_iceberg') }}
) AS sub_categories