with cls_services as (
    -- Lấy dữ liệu dịch vụ kỹ thuật theo toàn bộ lịch sử để Superset filter theo stat_date
    select *
    from {{ ref('stg_hospital_core__ct_dv_ky_thuat') }}
    -- Vẫn dùng các mốc thời gian thực tế để đảm bảo độ chính xác
    where coalesce(thoi_gian_lay_so, thoi_gian_tiep_nhan) is not null
),

status_aggregation as (
    select
        date_trunc('day', coalesce(thoi_gian_lay_so, thoi_gian_tiep_nhan)) as stat_date,

        -- 1. CHỜ TIẾP NHẬN: Các mã khởi tạo đầu tiên (VD: Bác sĩ vừa chỉ định, chờ in barcode)
        sum(case when trang_thai in (20, 25) then 1 else 0 end) as cho_tiep_nhan_count,

        -- 2. ĐANG THỰC HIỆN: Dải mã rộng nhất (VD: Đang lấy máu, quay ly tâm, máy đang chạy...)
        sum(case when trang_thai in (40, 43, 46, 50, 60, 63, 66, 70, 80, 90) then 1 else 0 end) as dang_thuc_hien_count,

        -- 3. CÓ KẾT QUẢ: Bao gồm cả trạng thái đã duyệt để đảm bảo logic funnel (co_ket_qua >= da_duyet)
        sum(case when trang_thai in (100, 130, 140, 150, 155) then 1 else 0 end) as co_ket_qua_count,

        -- 4. ĐÃ DUYỆT: Trưởng khoa/Bác sĩ CLS đã ký số (Sign-off)
        sum(case when trang_thai in (155) then 1 else 0 end) as da_duyet_count

    from cls_services
    group by 1
)

select * from status_aggregation