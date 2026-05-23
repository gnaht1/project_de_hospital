with agg as (

    select
        cast(date_trunc('day', order_time) as date) as stat_date,
        coalesce(nhom_dich_vu_cap1, 'Không xác định') as service_group,
        count(*) as total_services
    from {{ ref('int_clinical__cls_services_classified') }}
    group by 1, 2

)

select *
from agg
order by stat_date desc, total_services desc