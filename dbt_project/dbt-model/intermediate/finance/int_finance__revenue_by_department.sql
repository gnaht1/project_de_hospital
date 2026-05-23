with stg_phieu_thu as (
    select * from {{ ref('stg_hospital_core__ct_phieu_thu') }}
    where thoi_gian_thanh_toan is not null
),

-- Bổ sung bảng chi tiết dịch vụ
stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
    -- Chỉ lấy các dịch vụ đã được gắn vào phiếu thu
    where phieu_thu_id is not null
),

stg_khoa as (
    select * from {{ ref('stg_hospital_core__dm_khoa') }}
),

joined_data as (
    select
        pt.phieu_thu_id,
        cast(pt.thoi_gian_thanh_toan as timestamp) as thoi_gian_thanh_toan,
        
        -- Tính tổng doanh thu của TỪNG dịch vụ (BHYT + BN Cùng chi trả + BN Tự trả + Phụ thu)
        -- Sử dụng coalesce để tránh lỗi cộng với giá trị NULL
        (coalesce(dv.tien_bh_thanh_toan, 0) + 
         coalesce(dv.tien_nb_cung_chi_tra, 0) + 
         coalesce(dv.tien_nb_phu_thu, 0) + 
         coalesce(dv.tien_nb_tu_tra, 0)) as doanh_thu_dich_vu,
         
        k.department_name
        
    from stg_phieu_thu pt
    -- Join chi tiết dịch vụ nằm trong phiếu thu đó
    inner join stg_dich_vu dv on pt.phieu_thu_id = dv.phieu_thu_id
    -- Lấy Khoa Chỉ Định (hoặc Khoa Thực Hiện) của dịch vụ
    left join stg_khoa k on dv.khoa_chi_dinh_id = k.khoa_id
    where k.department_name is not null
)

select * from joined_data