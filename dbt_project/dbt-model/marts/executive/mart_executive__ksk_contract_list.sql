with contract_performance as (
    select * from {{ ref('int_ksk__contract_performance') }}
)

select
    hop_dong_id,
    cast(code_hop_dong as string) as contract_code,
    ten_hop_dong as contract_name,
    case
        when package_count = 0 then 'Chưa cấu hình gói'
        when package_count = 1 then package_name_list
        else concat(cast(package_count as string), ' gói')
    end as package_summary,
    total_employees_registered,
    total_employees_examined,
    case
        when tien_thuc_te_sau_giam > 0 then tien_thuc_te_sau_giam
        when tien_thuc_te > 0 then tien_thuc_te
        when tien_du_kien_sau_giam > 0 then tien_du_kien_sau_giam
        else tien_du_kien
    end as contract_revenue_amount,
    case
        when trang_thai_hop_dong = 40 then 'Đang TH'
        else cast(trang_thai_hop_dong as string)
    end as contract_status_label,
    ngay_hieu_luc,
    thoi_gian_thanh_ly,
    latest_completion_time
from contract_performance
