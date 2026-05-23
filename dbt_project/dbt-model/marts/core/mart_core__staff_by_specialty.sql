with assignments as (
    select * from {{ ref('int_core__staff_specialty_assignments') }}
),

aggregated as (
    select
        specialty_name,
        staff_group,
        count(distinct nhan_vien_id) as staff_count
    from assignments
    group by 1, 2
)

select
    specialty_name,
    staff_group,
    staff_count,
    case
        when staff_group = 'Bác sĩ' then 1
        else 2
    end as series_order
from aggregated
