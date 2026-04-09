with classified_staff as (
    select * from {{ ref('int_core__staff_classified') }}
),

-- Step 1: Count new joins per month
monthly_new_hires as (
    select
        extract(year from joined_at) as stat_year,
        extract(month from joined_at) as stat_month,
        
        -- Count total new staff and new doctors for this specific month
        count(nhan_vien_id) as new_total_staff,
        sum(case when is_doctor = true then 1 else 0 end) as new_doctor_staff
        
    from classified_staff
    group by 
        extract(year from joined_at),
        extract(month from joined_at)
),

-- Step 2: Calculate cumulative sum (Running Total) across months
cumulative_staff as (
    select
        stat_year,
        stat_month,
        
        -- Format month label with leading zero (e.g., 'T01', 'T02')
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        
        -- Running total for all staff
        sum(new_total_staff) over (
            order by stat_year, stat_month 
            rows between unbounded preceding and current row
        ) as total_active_staff,
        
        -- Running total for doctors only
        sum(new_doctor_staff) over (
            order by stat_year, stat_month 
            rows between unbounded preceding and current row
        ) as total_active_doctors
        
    from monthly_new_hires
)

select * from cumulative_staff
order by stat_year desc, stat_month asc