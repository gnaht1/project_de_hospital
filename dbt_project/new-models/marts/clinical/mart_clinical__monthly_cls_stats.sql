with classified_services as (
    select * from {{ ref('int_clinical__cls_services_classified') }}
),

monthly_aggregation as (
    select
        order_year as stat_year,
        order_month_num,
        -- Pad with zero for correct chronological sorting in BI tools
        'T' || lpad(cast(order_month_num as string), 2, '0') as month_label,
        service_group,
        
        -- Aggregate the volume
        count(dich_vu_id) as total_volume
        
    from classified_services
    group by 
        order_year,
        order_month_num,
        service_group
)

select
    stat_year,
    order_month_num,
    month_label,
    service_group,
    total_volume
from monthly_aggregation
order by 
    stat_year desc, 
    order_month_num asc