with pharmacy_data as (
    select * from {{ ref('int_pharmacy__medication_transactions') }}
),

daily_stats as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        extract(day from order_time) as stat_day,
        cast(date_trunc('day', cast(order_time as timestamp)) as date) as stat_date,
        count(distinct dot_dieu_tri_id) as dispensed_prescriptions,
        count(distinct case when net_patient_amount > 0 then dot_dieu_tri_id end) as paid_prescriptions,
        count(distinct case when net_patient_amount > 0 then cast(dot_dieu_tri_id as string) || '||' || cast(dich_vu_id as string) end) as paid_medication_lines
    from pharmacy_data
    where order_time is not null
    group by 1, 2, 3, 4
)

select *
from daily_stats
