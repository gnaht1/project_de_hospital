-- Create encounter fact table using standardized columns from staging
SELECT 
    sub_encounter.encounter_id,
    sub_encounter.patient_key,
    sub_encounter.department_key,
    sub_encounter.admission_date,
    sub_encounter.is_emergency,
    sub_encounter.patient_type_code
FROM (
    SELECT 
        encounter_id,
        patient_key,
        department_key,
        admission_date,
        is_emergency,
        patient_type_code
    FROM {{ ref('stg_ct_dot_dieu_tri') }}
) AS sub_encounter