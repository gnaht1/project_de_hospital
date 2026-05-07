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

# Cau hinh Spark/Iceberg maintenance
SPARK_SQL_PATH = "/opt/spark/bin/spark-sql"
JAVA_HOME_PATH = "/usr/lib/jvm/java-11-openjdk-amd64"

# Cac bang Iceberg duoc dbt ghi ra Silver/Gold can giu lai 10 snapshot moi nhat.
# Neu them model dbt dang table/incremental moi, bo sung ten bang vao danh sach nay.
DBT_ICEBERG_TABLES = [
    "db.int_clinical__cls_by_room",
    "db.int_clinical__cls_services_classified",
    "db.int_clinical__doctor_productivity",
    "db.int_clinical__enriched_episodes",
    "db.int_clinical__exam_patient_queue",
    "db.int_clinical__exam_visit_status",
    "db.int_clinical__patient_visits_classified",
    "db.int_clinical__services_enriched",
    "db.int_clinical__specialty_visits",
    "db.int_clinical__visit_types_classified",
    "db.int_core__staff_classified",
    "db.int_core__staff_specialty_assignments",
    "db.int_core__staff_title_groups",
    "db.int_finance__monthly_revenue_actual",
    "db.int_finance__paid_service_transactions",
    "db.int_finance__receipt_transactions",
    "db.int_finance__revenue_by_department",
    "db.int_finance__revenue_transactions",
    "db.int_finance__valid_payments",
    "db.int_ksk__contract_performance",
    "db.int_pharmacy__medication_transactions",
    "db.int_pharmacy__prescription_lines",
    "db.mart_clinical__daily_admissions",
    "db.mart_clinical__exam_patient_list",
    "db.mart_clinical__exam_service_kpi_overview",
    "db.mart_clinical__monthly_cls_by_room",
    "db.mart_clinical__monthly_cls_completion_rate",
    "db.mart_clinical__monthly_cls_stats",
    "db.mart_clinical__monthly_dept_scorecard",
    "db.mart_clinical__monthly_doctor_productivity",
    "db.mart_clinical__monthly_patient_category",
    "db.mart_clinical__monthly_patient_traffic",
    "db.mart_clinical__monthly_re_exam_rate",
    "db.mart_clinical__monthly_specialty_visits",
    "db.mart_clinical__monthly_visit_stats",
    "db.mart_clinical__monthly_visit_types",
    "db.mart_clinical__nrt_specialty_density_hourly",
    "db.mart_clinical__patient_registry_daily",
    "db.mart_clinical__service_group_distribution",
    "db.mart_clinical__today_treatment_list",
    "db.mart_common__date_window_filter",
    "db.mart_core__dm_benh_nhan_snapshot",
    "db.mart_core__monthly_active_staff",
    "db.mart_core__monthly_executive_summary",
    "db.mart_core__organization_kpi_overview",
    "db.mart_core__staff_by_specialty",
    "db.mart_core__staff_by_title_group",
    "db.mart_executive__kpi_card_annual_revenue",
    "db.mart_executive__kpi_card_annual_visits",
    "db.mart_executive__ksk_contract_kpi_overview",
    "db.mart_executive__ksk_contract_list",
    "db.mart_executive__monthly_revenue_vs_target",
    "db.mart_executive__monthly_traffic_revenue",
    "db.mart_finance__kpi_overview",
    "db.mart_finance__monthly_dept_revenue",
    "db.mart_finance__monthly_insurance_ratio",
    "db.mart_finance__nrt_revenue_receipts",
    "db.mart_finance__nrt_revenue_today_vs_yesterday",
    "db.mart_finance__revenue_by_month",
    "db.mart_finance__revenue_by_service_type",
    "db.mart_finance__revenue_composition_monthly",
    "db.mart_operations__daily_cls_status",
    "db.mart_operations__monthly_bpmn_stages",
    "db.mart_pharmacy__kpi_overview",
    "db.mart_pharmacy__monthly_prescriptions",
    "db.mart_pharmacy__waiting_prescriptions",
]

expire_snapshot_sql = "\n".join(
    f"CALL his_catalog.system.expire_snapshots(table => '{table}', retain_last => 10);"
    for table in DBT_ICEBERG_TABLES
)

expire_snapshots_cmd = f"""
export JAVA_HOME={JAVA_HOME_PATH}
{SPARK_SQL_PATH} \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.his_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.his_catalog.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
  --conf spark.sql.catalog.his_catalog.uri=jdbc:postgresql://172.30.2.170:5432/iceberg_catalog \
  --conf spark.sql.catalog.his_catalog.jdbc.user=data_admin \
  --conf spark.sql.catalog.his_catalog.jdbc.password=12345678 \
  --conf spark.sql.catalog.his_catalog.warehouse=s3a://hospital-datalake/iceberg_warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://172.30.2.170:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=12345678 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/tmp/dbt_expire_snapshots_derby;create=true" \
  -e "{expire_snapshot_sql}"
"""

# Khoi tao DAG
with DAG(
    'hospital_lakehouse_dbt_pipeline',
    default_args=default_args,
    description='Tu dong hoa dbt chuyen doi Raw Data sang Dim/Fact cho benh vien',
    schedule='*/30 * * * *',  # Lap lich chay moi 30 phut mot lan
    start_date=datetime(2026, 5, 8),  # Ngay bat dau
    catchup=False,  # Khong chay bu cac ngay trong qua khu
    max_active_runs=1,  # Dam bao chi co 1 DAG run hoat dong tai moi thoi diem
    tags=['dbt', 'hospital_lakehouse'],
) as dag:

    # Khai bao Task chay dbt
    # Dung duong dan tuyet doi cua dbt trong venv de tranh loi moi truong cua Airflow
    run_dbt_marts = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /root/hospital_de_project/hospital_dbt && /root/dbt_venv/bin/dbt run --profiles-dir /root/.dbt',
    )

    # Sau khi dbt run thanh cong, goi Spark expire_snapshots de don metadata cu tren MinIO
    # va chi giu lai 10 snapshot gan nhat cho moi bang Iceberg do dbt tao/cap nhat.
    expire_dbt_snapshots = BashOperator(
        task_id='expire_dbt_iceberg_snapshots',
        bash_command=expire_snapshots_cmd,
    )

    # Neu sau nay Thang muon them buoc test data, co the chen giua run_dbt_marts va expire_dbt_snapshots:
    # run_dbt_marts >> run_dbt_test >> expire_dbt_snapshots
    run_dbt_marts >> expire_dbt_snapshots

