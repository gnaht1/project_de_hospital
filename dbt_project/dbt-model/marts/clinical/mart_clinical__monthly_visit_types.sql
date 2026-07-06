with classified_visits as (
    select * from {{ ref('int_clinical__patient_visits_classified') }}
),

labeled_visits as (
    select
        extract(year from created_at) as stat_year,
        extract(month from created_at) as stat_month,
        extract(day from created_at) as stat_day,
        cast(date_trunc('day', cast(created_at as timestamp)) as date) as stat_date,
        case
            when is_new_patient = 1 then 'Kham moi'
            when is_return_patient = 1 then 'Tai kham'
            else 'Khac'
        end as visit_type,
        dot_dieu_tri_id
    from classified_visits
    where created_at is not null
),

daily_counts as (
    select
        stat_year,
        stat_month,
        stat_day,
        stat_date,
        visit_type,
        count(dot_dieu_tri_id) as type_visits
    from labeled_visits
    group by 1, 2, 3, 4, 5
),

percentage_calc as (
    select
        stat_year,
        stat_month,
        stat_day,
        stat_date,
        visit_type,
        type_visits,
        sum(type_visits) over (partition by stat_date) as total_day_visits,
        cast(type_visits as double) / sum(type_visits) over (partition by stat_date) as visit_percentage
    from daily_counts
)

select *
from percentage_calc
