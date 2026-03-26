-- Create dimension table for patient types
SELECT 
    sub_dim.patient_type_key,
    sub_dim.patient_type_name
FROM (
    SELECT 
        patient_type_key,
        patient_type_name
    FROM {{ ref('stg_dm_doi_tuong_kcb') }}
) AS sub_dim