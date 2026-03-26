-- Join fact table with patient type dimension without custom bucketing
SELECT 
    sub_join.encounter_id,
    sub_join.admission_date,
    sub_join.patient_key,
    sub_join.patient_type_name AS classification_group
FROM (
    SELECT 
        f.encounter_id,
        f.admission_date,
        f.patient_key,
        -- Use COALESCE to handle cases where patient_type_code is null or missing in dimension
        COALESCE(d.patient_type_name, 'Chưa xác định') AS patient_type_name
    FROM {{ ref('fact_encounters') }} AS f
    LEFT JOIN {{ ref('dim_patient_types') }} AS d 
        ON f.patient_type_code = d.patient_type_key
) AS sub_join