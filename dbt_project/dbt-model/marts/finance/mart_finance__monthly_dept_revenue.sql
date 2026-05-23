with monthly_aggregated as (
    select
        extract(year from thoi_gian_thanh_toan) as stat_year,
        extract(month from thoi_gian_thanh_toan) as stat_month,
        department_name,
        -- Convert to Millions VND
        sum(doanh_thu_dich_vu) / 1000000.0 as revenue_m_vnd
    from {{ ref('int_finance__revenue_by_department') }}
    group by 1, 2, 3
),

lagged_data as (
    select
        stat_year,
        stat_month,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        department_name,
        
        -- Current month metric
        revenue_m_vnd as current_month_revenue,
        
        -- Previous month metric using Window Function
        lag(revenue_m_vnd) over (
            partition by department_name
            order by stat_year, stat_month
        ) as last_month_revenue
        
    from monthly_aggregated
)

select * from lagged_data