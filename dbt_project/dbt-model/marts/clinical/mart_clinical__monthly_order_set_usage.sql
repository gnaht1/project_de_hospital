with registrations as (
    select * from {{ ref('int_clinical__order_set_registrations') }}
),

daily_usage as (
    select
        extract(year from event_time) as stat_year,
        extract(month from event_time) as stat_month,
        extract(day from event_time) as stat_day,
        cast(date_trunc('day', cast(event_time as timestamp)) as date) as stat_date,

        bo_chi_dinh_id,
        code_bo_chi_dinh,
        ten_bo_chi_dinh,
        hop_dong_id,

        count(nb_bo_chi_dinh_id) as total_registrations,
        count(distinct nb_dot_dieu_tri_id) as total_visits_registered,
        count(distinct nb_thong_tin_id) as unique_patients_registered,
        min(event_time) as first_registration_time,
        max(event_time) as latest_registration_time
    from registrations
    where event_time is not null
      and ten_bo_chi_dinh is not null
    group by
        extract(year from event_time),
        extract(month from event_time),
        extract(day from event_time),
        cast(date_trunc('day', cast(event_time as timestamp)) as date),
        bo_chi_dinh_id,
        code_bo_chi_dinh,
        ten_bo_chi_dinh,
        hop_dong_id
)

select *
from daily_usage
