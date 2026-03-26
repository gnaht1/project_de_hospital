-- models/marts/dim_service_categories.sql
-- Create service category dimension using subquery
SELECT 
    sub_dim.service_category_id,
    sub_dim.category_name
FROM (
    SELECT 
        service_category_id,
        category_name
    FROM {{ ref('stg_dm_loai_dich_vu') }}
) AS sub_dim