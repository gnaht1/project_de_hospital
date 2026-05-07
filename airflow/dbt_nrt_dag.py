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

# Cau hinh Spark/Iceberg maintenance tren VPS Airflow/Spark.
# Project local chi la mot phan code, nen cac path/endpoint nay giu theo moi truong VPS dang chay DAG.
SPARK_SQL_PATH = "/opt/spark/bin/spark-sql"
JAVA_HOME_PATH = "/usr/lib/jvm/java-11-openjdk-amd64"

DBT_PROJECT_DIR = "/root/hospital_de_project/hospital_dbt"
DBT_BIN = "/root/dbt_venv/bin/dbt"
DBT_PROFILES_DIR = "/root/.dbt"

NRT_DBT_SELECTOR = (
    "+mart_core__dm_benh_nhan_snapshot "
    "+mart_finance__nrt_revenue_receipts "
    "+mart_finance__nrt_revenue_today_vs_yesterday "
    "+mart_clinical__nrt_specialty_density_hourly"
)

# Cac bang NRT/Gold duoc cap nhat moi 3 phut. Sau moi lan dbt run thanh cong,
# expire_snapshots se xoa metadata/snapshot cu tren MinIO va chi giu 10 phien ban gan nhat.
# Neu selector NRT them model moi, bo sung ten bang Iceberg vao danh sach nay.
NRT_ICEBERG_TABLES = [
    "db.mart_core__dm_benh_nhan_snapshot",
    "db.mart_finance__nrt_revenue_receipts",
    "db.mart_finance__nrt_revenue_today_vs_yesterday",
    "db.mart_clinical__nrt_specialty_density_hourly",
]

expire_nrt_snapshot_sql = "\n".join(
    f"CALL his_catalog.system.expire_snapshots(table => '{table}', retain_last => 10);"
    for table in NRT_ICEBERG_TABLES
)

expire_nrt_snapshots_cmd = f"""
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
  --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/tmp/dbt_nrt_expire_snapshots_derby;create=true" \
  -e "{expire_nrt_snapshot_sql}"
"""

with DAG(
    'hospital_lakehouse_dbt_nrt_pipeline',
    default_args=default_args,
    description='Chay dbt NRT moi 3 phut cho cac chart realtime',
    schedule='*/3 * * * *',
    max_active_runs=1,
    start_date=datetime(2026, 5, 8),
    catchup=False,
    tags=['dbt', 'hospital_lakehouse', 'nrt'],
) as dag:

    run_dbt_nrt_models = BashOperator(
        task_id='run_dbt_nrt_models',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} -s '
            f'{NRT_DBT_SELECTOR}'
        ),
    )

    expire_nrt_iceberg_snapshots = BashOperator(
        task_id='expire_nrt_iceberg_snapshots',
        bash_command=expire_nrt_snapshots_cmd,
    )

    run_dbt_nrt_models >> expire_nrt_iceberg_snapshots
