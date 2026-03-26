-- Extract and standardize patient types (healthcare targets)
SELECT 
    sub_types.patient_type_key,
    sub_types.patient_type_name
FROM (
    SELECT 
        id AS patient_type_key,
        ten AS patient_type_name
    FROM {{ source('core_his', 'dm_doi_tuong_kcb_iceberg') }}
) AS sub_types