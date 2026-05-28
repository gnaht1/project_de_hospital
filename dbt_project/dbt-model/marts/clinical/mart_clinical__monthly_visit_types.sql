with classified_visits as (
    select * from {{ ref('int_clinical__patient_visits_classified') }}
),

labeled_visits as (
    select
        extract(year from created_at) as stat_year,
        extract(month from created_at) as stat_month,
        date_trunc('month', cast(created_at as timestamp)) as stat_date,
        
        case 
            when is_new_patient = 1 then 'Khám mới'
            when is_return_patient = 1 then 'Tái khám'
            else 'Khác'
        end as visit_type,
        
        dot_dieu_tri_id
    from classified_visits
),

monthly_counts as (
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        visit_type,
        count(dot_dieu_tri_id) as type_visits
    from labeled_visits
    group by 1, 2, 3, 4, 5
),

percentage_calc as (
    select
        stat_year,
        stat_month,
        stat_date,
        month_label,
        visit_type,
        type_visits,
        sum(type_visits) over (partition by stat_year, stat_month) as total_month_visits,
        cast(type_visits as double) / sum(type_visits) over (partition by stat_year, stat_month) as visit_percentage
    from monthly_counts
)

select * from percentage_calc
