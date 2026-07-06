with contract_performance as (
    select * from {{ ref('int_ksk__contract_performance') }}
),

active_contracts as (
    select
        hop_dong_id,
        code_hop_dong,
        ten_hop_dong,
        so_hop_dong,
        ngay_hieu_luc,
        thoi_gian_thanh_ly,
        created_at,
        updated_at,
        coalesce(
            ngay_hieu_luc,
            created_at,
            latest_completion_time,
            thoi_gian_thanh_ly
        ) as contract_stat_time,
        latest_completion_time,
        trang_thai_hop_dong,
        package_count,
        package_name_list,
        total_employees_registered,
        total_employees_examined,
        total_health_check_records,
        case
            when tien_thuc_te_sau_giam > 0 then tien_thuc_te_sau_giam
            when tien_thuc_te > 0 then tien_thuc_te
            when tien_du_kien_sau_giam > 0 then tien_du_kien_sau_giam
            else tien_du_kien
        end as contract_revenue_amount,
        coalesce(tien_chua_thanh_toan, 0) as outstanding_amount,
        coalesce(tien_da_thanh_toan, 0) as paid_amount,
        coalesce(tien_du_kien, 0) as expected_revenue_amount,
        coalesce(tien_du_kien_sau_giam, 0) as expected_revenue_after_discount_amount,
        coalesce(tien_thuc_te, 0) as actual_revenue_amount,
        coalesce(tien_thuc_te_sau_giam, 0) as actual_revenue_after_discount_amount
    from contract_performance
    where trang_thai_hop_dong = 40
),

contract_revenue as (
    select
        cast(contract_stat_time as timestamp) as stat_time,
        cast(date_trunc('day', contract_stat_time) as date) as stat_date,
        extract(year from contract_stat_time) as stat_year,
        extract(month from contract_stat_time) as stat_month,
        cast(date_trunc('week', contract_stat_time) as date) as stat_week,
        cast(date_trunc('month', contract_stat_time) as date) as stat_month_start,
        cast(date_trunc('quarter', contract_stat_time) as date) as stat_quarter_start,
        cast(date_trunc('year', contract_stat_time) as date) as stat_year_start,
        date_format(cast(date_trunc('month', contract_stat_time) as date), 'yyyy-MM') as stat_month_label,

        hop_dong_id,
        cast(code_hop_dong as string) as contract_code,
        ten_hop_dong as contract_name,
        so_hop_dong as contract_number,
        case
            when package_count = 0 then 'Chưa cấu hình gói'
            when package_count = 1 then package_name_list
            else concat(cast(package_count as string), ' gói')
        end as package_summary,
        package_count,
        package_name_list,
        total_employees_registered,
        total_employees_examined,
        total_health_check_records,
        'Đang TH' as contract_status_label,
        trang_thai_hop_dong,
        ngay_hieu_luc,
        thoi_gian_thanh_ly,
        created_at,
        updated_at,
        latest_completion_time,

        contract_revenue_amount,
        outstanding_amount,
        paid_amount,
        expected_revenue_amount,
        expected_revenue_after_discount_amount,
        actual_revenue_amount,
        actual_revenue_after_discount_amount
    from active_contracts
)

select *
from contract_revenue
