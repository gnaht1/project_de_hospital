with stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
),

stg_dot_dieu_tri as (
    -- Lấy bảng Đợt điều trị lên để làm cầu nối
    select 
        dot_dieu_tri_id, 
        nb_thong_tin_id 
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

pharmacy_joined as (
    select
        dv.dich_vu_id,
        dv.dot_dieu_tri_id,
        
        -- KÉO ID BỆNH NHÂN TỪ BẢNG ĐỢT ĐIỀU TRỊ SANG
        dt.nb_thong_tin_id,
        
        -- Dùng thoi_gian_chi_dinh làm trục thời gian chuẩn
        dv.order_time,
        
        -- Xử lý luồng tiền theo trạng thái hoàn
        -- Mã 40: Đã hoàn tiền cho bệnh nhân -> Doanh thu bằng 0
        -- Mã 0, 30: Bình thường hoặc chờ hoàn -> Vẫn tính là doanh thu tạm thời
        case 
            when dv.trang_thai_hoan = 40 then 0
            else coalesce(dv.tien_nb_tu_tra, 0)
        end as net_patient_amount,
        
        coalesce(dv.so_luong, 0) as quantity,
        dv.gia_goc as unit_price,
        dv.loai_dich_vu_id
        
    from stg_dich_vu dv
    left join stg_dot_dieu_tri dt on dv.dot_dieu_tri_id = dt.dot_dieu_tri_id
    
    where dv.loai_dich_vu_id = 90 -- Lọc riêng nhóm Thuốc
      and dv.is_active = true
      and dv.is_deleted = false
)

select * from pharmacy_joined