with attributed_visits as (
    select * from {{ ref('int_marketing__referral_attribution') }}
),

referrer_visits as (
    select
        cast(admission_time as timestamp) as stat_time,
        cast(visit_date as date) as stat_date,
        extract(year from visit_date) as stat_year,
        extract(month from visit_date) as stat_month,
        cast(date_trunc('week', cast(visit_date as timestamp)) as date) as stat_week,
        cast(date_trunc('month', cast(visit_date as timestamp)) as date) as stat_month_start,
        cast(date_trunc('quarter', cast(visit_date as timestamp)) as date) as stat_quarter_start,
        cast(date_trunc('year', cast(visit_date as timestamp)) as date) as stat_year_start,
        'T' || lpad(cast(extract(month from visit_date) as string), 2, '0') as month_label,

        dot_dieu_tri_id,
        nb_thong_tin_id,
        nguoi_gioi_thieu_id,
        code_nguoi_gioi_thieu,
        ten_nguoi_gioi_thieu,
        nguon_nb_id,
        code_nguon_nb,
        ten_nguon_nb,

        1 as total_visits,
        is_new_patient as new_patient_visits,
        is_return_patient as return_patient_visits
    from attributed_visits
)

select *
from referrer_visits
