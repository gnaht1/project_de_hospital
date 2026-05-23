from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cau hinh co ban cho kich ban chay NRT
default_args = {
    'owner': 'thang_de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DBT_PROJECT_DIR = "/root/hospital_de_project/hospital_dbt"
DBT_BIN = "/root/dbt_venv/bin/dbt"
DBT_PROFILES_DIR = "/root/.dbt"

NRT_DBT_SELECTOR = (
    "+mart_core__dm_benh_nhan_snapshot "
    "+mart_finance__nrt_revenue_receipts "
    "+mart_finance__nrt_revenue_today_vs_yesterday "
    "+mart_clinical__nrt_specialty_density_hourly"
)

with DAG(
    'hospital_lakehouse_dbt_nrt_pipeline',
    default_args=default_args,
    description='Chay dbt NRT moi 3 phut cho cac chart realtime',
    schedule='*/3 * * * *',
    max_active_runs=1,
    start_date=datetime(2026, 5, 7),
    catchup=False,
    tags=['dbt', 'hospital_lakehouse', 'nrt'],
) as dag:

    run_dbt_nrt_models = BashOperator(
        task_id='run_dbt_nrt_models',
        pool='dbt_pool',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} -s '
            f'{NRT_DBT_SELECTOR}'
        ),
    )
