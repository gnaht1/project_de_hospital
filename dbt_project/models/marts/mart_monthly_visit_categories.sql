-- models/marts/mart_monthly_visit_categories.sql
-- Aggregate monthly encounters and categorize them into specific UI buckets
SELECT 
    sub_encounters.admission_month,
    sub_encounters.visit_category,
    COUNT(sub_encounters.encounter_id) AS total_visits
FROM (
    SELECT 
        id AS encounter_id,
        -- Extract Year-Month for grouping (e.g., '2025-01')
        SUBSTR(CAST(thoi_gian_vao_vien AS STRING), 1, 7) AS admission_month,
        
        -- Categorize visits based on boolean flags first, then patient type
        CASE 
            WHEN cap_cuu = true THEN 'Cấp cứu'
            WHEN kham_suc_khoe = true THEN 'KSK Đoàn'
            WHEN doi_tuong_kcb = 1 THEN 'Ngoại trú'
            ELSE 'Khác' -- Gom tất cả các ID 2,3,4,5,6,7,8,9,10 vào chung 1 nhóm "Khác" để chart không bị nát
        END AS visit_category
    FROM {{ source('core_his', 'ct_dot_dieu_tri_iceberg') }}
    WHERE deleted = 0 
      AND thoi_gian_vao_vien IS NOT NULL
) AS sub_encounters
GROUP BY 
    sub_encounters.admission_month, 
    sub_encounters.visit_category