with patient_visits as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

patient_types as (
    select * from {{ ref('stg_hospital_core__dm_doi_tuong_kcb') }}
),

categorized_patients as (
    select
        extract(year from v.created_at) as stat_year,
        extract(month from v.created_at) as stat_month,
        extract(day from v.created_at) as stat_day,
        cast(date_trunc('day', cast(v.created_at as timestamp)) as date) as stat_date,
        v.nb_thong_tin_id,
        case
            when v.is_health_check = true then 'KSK Doan'
            else coalesce(dt.patient_type_name, 'Dich vu')
        end as patient_category
    from patient_visits v
    left join patient_types dt
        on v.doi_tuong_kcb_id = dt.doi_tuong_kcb_id
    where v.is_active = true
      and v.created_at is not null
),

daily_summary as (
    select
        stat_year,
        stat_month,
        stat_day,
        stat_date,
        patient_category,
        count(distinct nb_thong_tin_id) as total_patients
    from categorized_patients
    group by 1, 2, 3, 4, 5
)

select *
from daily_summary
