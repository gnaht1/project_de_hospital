with contract_performance as (
    select * from {{ ref('int_ksk__contract_performance') }}
),

snapshot_metrics as (
    select
        current_date as snapshot_date,
        count(distinct case
            when trang_thai_hop_dong = 40 then hop_dong_id
        end) as active_contracts,
        sum(case
            when trang_thai_hop_dong = 40 then total_employees_examined
            else 0
        end) as total_examined_employees,
        sum(case
            when trang_thai_hop_dong = 40 then coalesce(tien_thuc_te_sau_giam, tien_thuc_te, 0)
            else 0
        end) as contract_revenue_amount,
        sum(case
            when trang_thai_hop_dong = 40 then tien_chua_thanh_toan
            else 0
        end) as outstanding_amount
    from contract_performance
)

select * from snapshot_metrics
