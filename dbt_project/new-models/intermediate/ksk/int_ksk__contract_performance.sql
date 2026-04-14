with contracts as (
    select * from {{ ref('stg_hospital_core__dm_hop_dong_ksk') }}
),

health_checks as (
    select * from {{ ref('stg_hospital_core__ct_kham_suc_khoe') }}
),

employee_stats as (
    select
        hop_dong_id,
        count(distinct ma_nhan_vien) as total_employees_examined,
        count(kham_suc_khoe_id) as total_health_check_records,
        max(thoi_gian_hoan_thanh) as latest_completion_time
    from health_checks
    where hop_dong_id is not null
    group by 1
)

select
    c.hop_dong_id,
    c.code_hop_dong,
    c.ten_hop_dong,
    c.so_hop_dong,
    c.ngay_hieu_luc,
    c.thoi_gian_thanh_ly,
    c.trang_thai_hop_dong,
    c.tien_chua_thanh_toan,
    c.tien_da_thanh_toan,
    c.tien_du_kien,
    c.tien_du_kien_sau_giam,
    c.tien_giam_gia,
    c.tien_mien_giam_dich_vu,
    c.tien_mien_giam_hop_dong,
    c.tien_thuc_te,
    c.tien_thuc_te_sau_giam,
    c.phan_tram_mien_giam,
    c.chot_thanh_toan_dv_ksk,
    coalesce(e.total_employees_examined, 0) as total_employees_examined,
    coalesce(e.total_health_check_records, 0) as total_health_check_records,
    e.latest_completion_time
from contracts c
left join employee_stats e
    on c.hop_dong_id = e.hop_dong_id
