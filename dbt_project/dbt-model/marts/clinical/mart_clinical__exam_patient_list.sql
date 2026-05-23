with exam_queue as (
    select * from {{ ref('int_clinical__exam_patient_queue') }}
)

select
    metric_date,
    row_number() over (
        partition by metric_date
        order by exam_display_time, dv_kham_id
    ) as stt,
    patient_code,
    patient_name,
    examining_doctor_name,
    department_name,
    exam_display_time,
    date_format(exam_display_time, 'HH:mm') as exam_time_label,
    status_label,
    status_order
from exam_queue
