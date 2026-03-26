-- Extract and standardize treatment episodes data
-- Filter out deleted records at the staging level
SELECT 
    sub_encounters.encounter_id,
    sub_encounters.patient_key,
    sub_encounters.department_key,
    sub_encounters.admission_date,
    sub_encounters.is_emergency,
    sub_encounters.patient_type_code
FROM (
    SELECT 
        id AS encounter_id,
        nb_thong_tin_id AS patient_key,
        khoa_id AS department_key,
        -- Convert string time to date format
        CAST(SUBSTR(thoi_gian_vao_vien, 1, 10) AS DATE) AS admission_date,
        cap_cuu AS is_emergency,
        doi_tuong AS patient_type_code
    FROM {{ source('core_his', 'ct_dot_dieu_tri_iceberg') }}
    WHERE deleted = 0 
      AND thoi_gian_vao_vien IS NOT NULL
) AS sub_encounters