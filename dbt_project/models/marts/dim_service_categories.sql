-- models/marts/dim_service_categories.sql
-- Create service category dimension with CLS grouping
SELECT 
    service_category_id,
    category_name,
    CASE 
        WHEN service_category_id = 20 THEN 'Xét Nghiệm'
        WHEN service_category_id = 30 THEN 'CĐHA'
        ELSE 'Khác'
    END AS cls_group
FROM {{ ref('stg_dm_loai_dich_vu') }}