with doctor_visits as (
    select * from {{ ref('int_clinical__doctor_productivity') }}
),

aggregated as (
    select
        -- Extract time dimensions for Superset native filters
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        'T' || lpad(cast(extract(month from order_time) as string), 2, '0') as month_label,
        
        doctor_id,
        doctor_name,
        
        -- Calculate productivity (total visits per doctor)
        count(dich_vu_id) as total_visits
        
    from doctor_visits
    group by 1, 2, 3, 4, 5
)

select * from aggregated