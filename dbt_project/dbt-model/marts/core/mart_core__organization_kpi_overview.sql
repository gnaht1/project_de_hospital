with staff as (
    select * from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

classified_staff as (
    select * from {{ ref('int_core__staff_classified') }}
),

departments as (
    select * from {{ ref('stg_hospital_core__dm_khoa') }}
),

rooms as (
    select * from {{ ref('stg_hospital_core__dm_phong') }}
),

snapshot_metrics as (
    select
        current_date as snapshot_date,
        (select count(distinct nhan_vien_id) from staff) as total_staff,
        (select count(distinct nhan_vien_id) from classified_staff where is_doctor = true) as total_doctors,
        (select count(distinct khoa_id) from departments) as total_departments,
        (select count(distinct phong_id) from rooms) as total_rooms
)

select * from snapshot_metrics
