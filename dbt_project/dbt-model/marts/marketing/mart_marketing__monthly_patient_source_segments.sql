with attributed_visits as (
    select * from {{ ref('int_marketing__patient_source_attribution') }}
),

patient_source_segments as (
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
        nguon_nb_id,
        code_nguon_nb,
        ten_nguon_nb,
        has_referrer,
        nhom_nguon,

        1 as total_visits,
        is_new_patient as new_patient_visits,
        is_return_patient as return_patient_visits,
        case when is_new_patient = 1 then nb_thong_tin_id end as new_patient_id,
        case when is_return_patient = 1 then nb_thong_tin_id end as return_patient_id
    from attributed_visits
)

select *
from patient_source_segments
