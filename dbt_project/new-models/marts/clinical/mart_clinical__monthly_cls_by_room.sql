with cls_services as (
    select * from {{ ref('int_clinical__cls_by_room') }}
),

aggregated as (
    select
        -- Extract time dimensions for Superset native filters
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        'T' || lpad(cast(extract(month from order_time) as string), 2, '0') as month_label,
        
        room_name,
        execution_status,
        
        -- Count the total volume of paraclinical services
        count(dich_vu_id) as total_volume
        
    from cls_services
    group by 1, 2, 3, 4, 5
)

select * from aggregated