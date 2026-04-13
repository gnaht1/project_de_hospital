import os
from config import (
    MINIO_URL, ACCESS_KEY, SECRET_KEY,
    BUCKET_NAME, CATALOG_NAME, DATABASE_NAME,
    POSTGRES_IP, POSTGRES_USER, POSTGRES_PASSWORD
)
from pyspark.sql import SparkSession

def main():
    print(">>> INIT SPARK SESSION FOR SCHEMA EVOLUTION...")

    # Initialize SparkSession with exact same configurations as main_job.py
    spark = SparkSession.builder \
        .appName("Iceberg_Schema_Updater") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.his_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.his_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.his_catalog.uri", f"jdbc:postgresql://{POSTGRES_IP}:5432/iceberg_catalog") \
        .config("spark.sql.catalog.his_catalog.jdbc.user", POSTGRES_USER) \
        .config("spark.sql.catalog.his_catalog.jdbc.password", POSTGRES_PASSWORD) \
        .config("spark.sql.catalog.his_catalog.warehouse", f"s3a://{BUCKET_NAME}/iceberg_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # INSERT YOUR ALTER TABLE QUERIES BELOW
    # ==========================================
    
    spark.sql("""
        drop TABLE if exists his_catalog.db.dm_benh_nhan_iceberg;
        
    """)

    print(">>> SCHEMA EVOLUTION COMPLETED SUCCESSFULLY!")
    
    # Stop session to release resources
    spark.stop()

if __name__ == "__main__":
    main()