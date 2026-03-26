-- Create patient dimension using standardized columns from staging
SELECT 
    sub_patient.patient_key,
    sub_patient.patient_code,
    sub_patient.full_name,
    sub_patient.phone_number
FROM (
    SELECT 
        patient_key,
        patient_code,
        full_name,
        phone_number
    FROM {{ ref('stg_dm_benh_nhan') }}
) AS sub_patient