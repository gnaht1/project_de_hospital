with contract_performance as (
    select * from {{ ref('int_ksk__contract_performance') }}
),

health_checks as (
    select * from {{ ref('stg_hospital_core__ct_kham_suc_khoe') }}
),

active_contracts as (
    select
        hop_dong_id,
        ngay_hieu_luc,
        coalesce(tien_thuc_te_sau_giam, tien_thuc_te, 0) as contract_revenue_amount,
        coalesce(tien_chua_thanh_toan, 0) as outstanding_amount
    from contract_performance
    where trang_thai_hop_dong = 40
),

kpi_active_contracts as (
    select
        count(distinct hop_dong_id) as active_contracts
    from active_contracts
),

kpi_examined_today as (
    select
        count(distinct case
            when h.ma_nhan_vien is not null then concat(cast(h.hop_dong_id as string), '||', h.ma_nhan_vien)
        end) as total_examined_employees
    from health_checks h
    inner join active_contracts ac
        on h.hop_dong_id = ac.hop_dong_id
    where cast(h.thoi_gian_hoan_thanh as date) = current_date
),

kpi_revenue_this_month as (
    select
        sum(case
            when cast(date_trunc('month', ngay_hieu_luc) as date) = cast(date_trunc('month', current_date) as date)
            then contract_revenue_amount
            else 0
        end) as contract_revenue_amount
    from active_contracts
),

kpi_outstanding_current as (
    select
        sum(outstanding_amount) as outstanding_amount
    from active_contracts
)

select
    current_date as snapshot_date,
    cast(date_trunc('month', current_date) as date) as month_start,
    date_format(cast(date_trunc('month', current_date) as date), 'yyyy-MM') as stat_month,
    coalesce(a.active_contracts, 0) as active_contracts,
    coalesce(e.total_examined_employees, 0) as total_examined_employees,
    coalesce(r.contract_revenue_amount, 0) as contract_revenue_amount,
    coalesce(o.outstanding_amount, 0) as outstanding_amount
from kpi_active_contracts a
cross join kpi_examined_today e
cross join kpi_revenue_this_month r
cross join kpi_outstanding_current o
