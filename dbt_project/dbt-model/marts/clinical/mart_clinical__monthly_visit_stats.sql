with classified_visits as (
    select * from {{ ref('int_clinical__visit_types_classified') }}
),

monthly_aggregation as (
    select
        visit_year as stat_year,
        visit_month_num,
        -- Create a formatted label like 'T01', 'T02' using STRING
        'T' || lpad(cast(visit_month_num as string), 2, '0') as month_label,
        visit_type,
        
        -- Count the number of visits
        count(dot_dieu_tri_id) as total_visits
        
    from classified_visits
    group by 
        visit_year,
        visit_month_num,
        visit_type
)

select
    stat_year,
    visit_month_num,
    month_label,
    visit_type,
    total_visits
from monthly_aggregation
-- Order chronologically for BI tools
order by 
    stat_year desc, 
    visit_month_num asc