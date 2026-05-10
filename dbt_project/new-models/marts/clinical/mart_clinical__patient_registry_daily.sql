with patient_profiles as (
    select * from {{ ref('stg_hospital_core__dm_benh_nhan') }}
),

daily_registrations as (
    select
        date_trunc('day', cast(created_at as timestamp)) as stat_date,
        count(patient_id) as new_patients_registered
    from patient_profiles
    group by 1
)

select * from daily_registrations