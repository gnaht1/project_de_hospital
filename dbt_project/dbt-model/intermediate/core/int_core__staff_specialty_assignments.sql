with staff as (
    select * from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

titles as (
    select * from {{ ref('stg_hospital_core__dm_hoc_ham_hoc_vi') }}
),

specialties as (
    select * from {{ ref('stg_hospital_core__dm_chuyen_khoa') }}
),

staff_specialty_exploded as (
    select
        s.nhan_vien_id,
        s.doctor_name,
        s.hoc_ham_hoc_vi_id,
        trim(specialty_id_text) as specialty_id_text
    from staff s
    lateral view explode(
        split(
            regexp_replace(coalesce(s.ds_chuyen_khoa_id, ''), '\\[|\\]|\\s', ''),
            ','
        )
    ) exploded_specialties as specialty_id_text
),

staff_specialty_cleaned as (
    select
        nhan_vien_id,
        doctor_name,
        hoc_ham_hoc_vi_id,
        cast(specialty_id_text as int) as chuyen_khoa_id
    from staff_specialty_exploded
    where specialty_id_text <> ''
),

classified_assignments as (
    select
        ssc.nhan_vien_id,
        ssc.doctor_name,
        ssc.chuyen_khoa_id,
        sp.specialty_name,
        case
            when lower(coalesce(t.title_code, '')) like '%dr%' then 'Bác sĩ'
            else 'Điều dưỡng/KTV'
        end as staff_group,
        case
            when lower(coalesce(t.title_code, '')) like '%dr%' then true
            else false
        end as is_doctor
    from staff_specialty_cleaned ssc
    left join titles t
        on ssc.hoc_ham_hoc_vi_id = t.hoc_ham_hoc_vi_id
    left join specialties sp
        on ssc.chuyen_khoa_id = sp.chuyen_khoa_id
)

select
    nhan_vien_id,
    doctor_name,
    chuyen_khoa_id,
    coalesce(specialty_name, 'Chưa phân chuyên khoa') as specialty_name,
    staff_group,
    is_doctor
from classified_assignments
