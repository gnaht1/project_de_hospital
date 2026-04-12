with pharmacy_data as (
    -- Lấy dữ liệu từ bảng Silver vừa tạo
    select * from {{ ref('int_pharmacy__medication_transactions') }}
),

monthly_stats as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        
        -- Cột thời gian để filter trên Superset
        date_trunc('month', cast(order_time as timestamp)) as stat_date,
        
        -- Tử số: Đếm số lượng Toa (đại diện bằng mã đợt điều trị)
        -- Điều kiện > 0 giúp loại bỏ những đơn thuốc đã bị pou4 trả lại hoàn toàn (mã 40)
        count(distinct dot_dieu_tri_id) as dispensed_prescriptions
        
    from pharmacy_data
    -- Lọc bỏ các dòng bị hủy/hoàn (đã được xử lý gán = 0 ở lớp Silver)
    where net_patient_amount > 0 
    group by 1, 2, 3
)

select * from monthly_stats