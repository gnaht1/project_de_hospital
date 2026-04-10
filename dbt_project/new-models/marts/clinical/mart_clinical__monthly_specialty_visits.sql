with visits as (
    select * from {{ ref('int_clinical__specialty_visits') }}
),

aggregated as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        'T' || lpad(cast(extract(month from order_time) as string), 2, '0') as month_label,
        specialty_code,
        specialty_name,
        
        -- Đếm số lượt khám
        count(dich_vu_id) as total_visits
        
    from visits
    group by 1, 2, 3, 4, 5
)

select * from aggregated