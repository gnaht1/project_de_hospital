with source as (
    -- Read raw patient data
    select * from {{ source('raw_hospital', 'dm_benh_nhan_iceberg') }}
),

parsed_patients as (
    select
        nb_thong_tin_id as patient_id,
        ma_nb as patient_code,
        ten_nb as patient_name,
        ten_nb_khong_dau as patient_name_no_accents,
        ngay_sinh as date_of_birth,
        so_dien_thoai as phone_number,
        email,
        noi_lam_viec as workplace,
        created_at,
        updated_at
    from source
)

select * from parsed_patients
