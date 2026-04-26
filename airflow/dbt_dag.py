from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình cơ bản cho kịch bản chạy
default_args = {
    'owner': 'thang_de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Khởi tạo DAG
with DAG(
    'hospital_lakehouse_dbt_pipeline',
    default_args=default_args,
    description='Tự động hóa dbt chuyển đổi Raw Data sang Dim/Fact cho bệnh viện',
    schedule='*/30 * * * *', # Lập lịch chạy mỗi 30 phút một lần
    start_date=datetime(2026, 4, 15), # Ngày bắt đầu (để dbt chạy ngay lập tức)
    catchup=False, # Không chạy bù các ngày trong quá khứ
    tags=['dbt', 'hospital_lakehouse'],
) as dag:

    # Khai báo Task chạy dbt
    # Dùng đường dẫn tuyệt đối của dbt trong venv để tránh lỗi môi trường của Airflow
    run_dbt_marts = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /root/hospital_de_project/hospital_dbt && /root/dbt_venv/bin/dbt run --profiles-dir /root/.dbt',
    )

    # Nếu sau này Thắng muốn thêm bước test data, có thể tạo thêm task và nối chuỗi:
    # run_dbt_test = BashOperator(...)
    # run_dbt_marts >> run_dbt_test
    
    run_dbt_marts
