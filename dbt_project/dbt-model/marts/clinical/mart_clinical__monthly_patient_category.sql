with patient_visits as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

patient_types as (
    select * from {{ ref('stg_hospital_core__dm_doi_tuong_kcb') }}
),

date_window_options as (
    select 'TODAY' as date_window_key, 'Hôm nay' as date_window_label, 1 as window_days, 1 as sort_order
    union all
    select 'LAST_7_DAYS' as date_window_key, '7 ngày' as date_window_label, 7 as window_days, 2 as sort_order
    union all
    select 'LAST_30_DAYS' as date_window_key, '30 ngày' as date_window_label, 30 as window_days, 3 as sort_order
),

categorized_patients as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        extract(year from v.admission_time) as stat_year,
        extract(month from v.admission_time) as stat_month,
        date_trunc('month', v.admission_time) as stat_date,
        v.nb_thong_tin_id,
        case
            when v.is_health_check = true then 'KSK Đoàn'
            else coalesce(dt.patient_type_name, 'Dịch vụ')
        end as patient_category
    from patient_visits v
    cross join date_window_options w
    left join patient_types dt
        on v.doi_tuong_kcb_id = dt.doi_tuong_kcb_id
    where v.is_active = true
      and datediff(current_date, cast(v.admission_time as date)) between 0 and w.window_days - 1
),

monthly_summary as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        stat_year,
        stat_month,
        stat_date,
        patient_category,
        count(distinct nb_thong_tin_id) as total_patients
    from categorized_patients
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from monthly_summary