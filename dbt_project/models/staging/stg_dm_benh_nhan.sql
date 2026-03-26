-- Extract and standardize patient catalog data using a subquery
SELECT 
    sub_patients.patient_key,
    sub_patients.patient_code,
    sub_patients.full_name,
    sub_patients.full_name_no_marks,
    sub_patients.birth_date,
    sub_patients.phone_number,
    sub_patients.email_address,
    sub_patients.workplace,
    sub_patients.created_at,
    sub_patients.updated_at
FROM (
    -- Select specific columns from the raw iceberg table and rename them for consistency
    SELECT 
        nb_thong_tin_id AS patient_key,
        ma_nb AS patient_code,
        ten_nb AS full_name,
        ten_nb_khong_dau AS full_name_no_marks,
        ngay_sinh AS birth_date,
        so_dien_thoai AS phone_number,
        email AS email_address,
        noi_lam_viec AS workplace,
        created_at,
        updated_at
    FROM {{ source('core_his', 'dm_benh_nhan_iceberg') }}
) AS sub_patients