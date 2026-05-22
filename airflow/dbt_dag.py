from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cau hinh co ban cho kich ban chay
default_args = {
    'owner': 'thang_de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DBT_PROJECT_DIR = "/root/hospital_de_project/hospital_dbt"
DBT_BIN = "/root/dbt_venv/bin/dbt"
DBT_PROFILES_DIR = "/root/.dbt"

# Cac bang Iceberg batch/full do DAG nay quan ly.
# Khong dua bang NRT vao day de tranh conflict voi dbt_nrt_dag.py.
DBT_ICEBERG_TABLES = [
    "silver_db.int_clinical__cls_by_room",
    "silver_db.int_clinical__cls_services_classified",
    "silver_db.int_clinical__doctor_productivity",
    "silver_db.int_clinical__enriched_episodes",
    "silver_db.int_clinical__exam_patient_queue",
    "silver_db.int_clinical__exam_visit_status",
    "silver_db.int_clinical__patient_visits_classified",
    "silver_db.int_clinical__services_enriched",
    "silver_db.int_clinical__specialty_visits",
    "silver_db.int_clinical__visit_types_classified",
    "silver_db.int_core__staff_classified",
    "silver_db.int_core__staff_specialty_assignments",
    "silver_db.int_core__staff_title_groups",
    "silver_db.int_finance__monthly_revenue_actual",
    "silver_db.int_finance__paid_service_transactions",
    "silver_db.int_finance__receipt_transactions",
    "silver_db.int_finance__revenue_by_department",
    "silver_db.int_finance__revenue_transactions",
    "silver_db.int_finance__valid_payments",
    "silver_db.int_ksk__contract_performance",
    "silver_db.int_pharmacy__medication_transactions",
    "silver_db.int_pharmacy__prescription_lines",
    "gold_db.mart_clinical__daily_admissions",
    "gold_db.mart_clinical__exam_patient_list",
    "gold_db.mart_clinical__exam_service_kpi_overview",
    "gold_db.mart_clinical__monthly_cls_by_room",
    "gold_db.mart_clinical__monthly_cls_completion_rate",
    "gold_db.mart_clinical__monthly_cls_stats",
    "gold_db.mart_clinical__monthly_dept_scorecard",
    "gold_db.mart_clinical__monthly_doctor_productivity",
    "gold_db.mart_clinical__monthly_patient_category",
    "gold_db.mart_clinical__monthly_patient_traffic",
    "gold_db.mart_clinical__monthly_re_exam_rate",
    "gold_db.mart_clinical__monthly_specialty_visits",
    "gold_db.mart_clinical__monthly_visit_stats",
    "gold_db.mart_clinical__monthly_visit_types",
    "gold_db.mart_clinical__patient_registry_daily",
    "gold_db.mart_clinical__service_group_distribution",
    "gold_db.mart_clinical__today_treatment_list",
    "gold_db.mart_common__date_window_filter",
    "gold_db.mart_core__monthly_active_staff",
    "gold_db.mart_core__monthly_executive_summary",
    "gold_db.mart_core__organization_kpi_overview",
    "gold_db.mart_core__staff_by_specialty",
    "gold_db.mart_core__staff_by_title_group",
    "gold_db.mart_executive__kpi_card_annual_revenue",
    "gold_db.mart_executive__kpi_card_annual_visits",
    "gold_db.mart_executive__ksk_contract_kpi_overview",
    "gold_db.mart_executive__ksk_contract_list",
    "gold_db.mart_executive__monthly_revenue_vs_target",
    "gold_db.mart_executive__monthly_traffic_revenue",
    "gold_db.mart_finance__kpi_overview",
    "gold_db.mart_finance__monthly_dept_revenue",
    "gold_db.mart_finance__monthly_insurance_ratio",
    "gold_db.mart_finance__revenue_by_month",
    "gold_db.mart_finance__revenue_by_service_type",
    "gold_db.mart_finance__revenue_composition_monthly",
    "gold_db.mart_operations__daily_cls_status",
    "gold_db.mart_operations__monthly_bpmn_stages",
    "gold_db.mart_pharmacy__kpi_overview",
    "gold_db.mart_pharmacy__monthly_prescriptions",
    "gold_db.mart_pharmacy__waiting_prescriptions",
]

DBT_BATCH_SELECTOR = " ".join(
    table_name.split(".", 1)[1]
    for table_name in DBT_ICEBERG_TABLES
)

# Khoi tao DAG
with DAG(
    'hospital_lakehouse_dbt_pipeline',
    default_args=default_args,
    description='Tu dong hoa dbt chuyen doi Raw Data sang Dim/Fact cho benh vien',
    schedule='*/30 * * * *',  # Lap lich chay moi 30 phut mot lan
    start_date=datetime(2026, 5, 7),  # Ngay bat dau
    catchup=False,  # Khong chay bu cac ngay trong qua khu
    max_active_runs=1,  # Dam bao chi co 1 DAG run hoat dong tai moi thoi diem
    tags=['dbt', 'hospital_lakehouse'],
) as dag:

    # Khai bao Task chay dbt
    # Dung duong dan tuyet doi cua dbt trong venv de tranh loi moi truong cua Airflow
    run_dbt_marts = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} '
            f'--select {DBT_BATCH_SELECTOR}'
        ),
    )
