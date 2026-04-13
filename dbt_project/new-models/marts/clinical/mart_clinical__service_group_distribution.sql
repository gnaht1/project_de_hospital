with agg as (

    select
        coalesce(nhom_dich_vu_cap1, 'Không xác định') as service_group,
        count(*) as total_services
    from {{ ref('int_clinical__cls_services_classified') }}
    group by 1

)

select *
from agg
order by total_services desc