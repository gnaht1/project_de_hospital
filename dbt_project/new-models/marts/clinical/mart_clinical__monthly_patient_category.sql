with patient_visits as (
    -- Fetch raw treatment data
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

patient_types as (
    -- Fetch the newly created dictionary table for patient types
    select * from {{ ref('stg_hospital_core__dm_doi_tuong_kcb') }}
),

categorized_patients as (
    select
        extract(year from v.admission_time) as stat_year,
        extract(month from v.admission_time) as stat_month,
        date_trunc('month', v.admission_time) as stat_date,
        
        v.nb_thong_tin_id,
        
        -- Priority 1: Health Check flag
        -- Priority 2: Use the exact name from the dictionary table (e.g., 'BHYT', 'Viện phí')
        -- Fallback: If ID is missing or null, default to 'Dịch vụ'
        case 
            when v.is_health_check = true then 'KSK Đoàn'
            else coalesce(dt.patient_type_name, 'Dịch vụ')
        end as patient_category
        
    from patient_visits v
    
    -- Safely cast both sides to string to avoid mismatch errors (e.g., int vs varchar)
    left join patient_types dt on v.doi_tuong_kcb_id = dt.doi_tuong_kcb_id
    
    where v.is_active = true
),

monthly_summary as (
    -- Aggregate distinct patient counts by month and category
    select
        stat_year,
        stat_month,
        stat_date,
        patient_category,
        count(distinct nb_thong_tin_id) as total_patients
    from categorized_patients
    group by 1, 2, 3, 4
)

select * from monthly_summary