with stg_dot_dieu_tri as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

stg_doi_tuong_kcb as (
    select * from {{ ref('stg_hospital_core__dm_doi_tuong_kcb') }}
),

joined_visits as (
    select
        d.dot_dieu_tri_id,
        d.admission_time,
        
        -- Extract the month and year for aggregation
        extract(month from d.admission_time) as visit_month_num,
        extract(year from d.admission_time) as visit_year,
        
        d.is_emergency,
        d.is_health_check,
        k.patient_type_name
        
    from stg_dot_dieu_tri d
    left join stg_doi_tuong_kcb k
        on d.doi_tuong_kcb_id = k.doi_tuong_kcb_id
    where d.admission_time is not null
),

classified_visits as (
    select
        dot_dieu_tri_id,
        admission_time,
        visit_month_num,
        visit_year,
        
        -- Priority classification based on flags, falling back to the dictionary name
        case 
            when is_emergency = true then 'Cấp cứu'
            when is_health_check = true then 'KSK Đoàn'
            when lower(patient_type_name) like '%nội trú%' then 'Nội trú'
            when lower(patient_type_name) like '%ngoại trú%' then 'Ngoại trú'
            else coalesce(patient_type_name, 'Khác') 
        end as visit_type
        
    from joined_visits
)

select * from classified_visits