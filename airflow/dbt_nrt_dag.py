from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình cơ bản cho kịch bản chạy NRT
default_args = {
    'owner': 'thang_de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'hospital_lakehouse_dbt_nrt_pipeline',
    default_args=default_args,
    description='Chạy dbt NRT mỗi 3 phút cho các chart realtime',
    schedule='*/3 * * * *',
    max_active_runs=1,
    start_date=datetime(2026, 5, 3),
    catchup=False,
    tags=['dbt', 'hospital_lakehouse', 'nrt'],
) as dag:

    run_dbt_nrt_models = BashOperator(
        task_id='run_dbt_nrt_models',
        bash_command=(
            'cd /root/hospital_de_project/hospital_dbt && '
            '/root/dbt_venv/bin/dbt run --profiles-dir /root/.dbt -s '
            '+mart_core__dm_benh_nhan_snapshot '
            '+mart_finance__nrt_revenue_receipts '
            '+mart_finance__nrt_revenue_today_vs_yesterday '
            '+mart_clinical__nrt_specialty_density_hourly'
        ),
    )

    run_dbt_nrt_models
