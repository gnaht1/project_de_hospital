with staff_title_groups as (
    select * from {{ ref('int_core__staff_title_groups') }}
),

aggregated as (
    select
        title_group,
        display_order,
        count(distinct nhan_vien_id) as staff_count
    from staff_title_groups
    group by 1, 2
)

select
    title_group,
    staff_count,
    display_order
from aggregated
order by display_order
