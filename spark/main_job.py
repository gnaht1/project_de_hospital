import os
from config import (
    KAFKA_SERVER, MINIO_URL, ACCESS_KEY, SECRET_KEY,
    BUCKET_NAME, CATALOG_NAME, DATABASE_NAME,
    POSTGRES_IP, POSTGRES_USER, POSTGRES_PASSWORD
)
import schemas
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, row_number
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, BooleanType, DoubleType, FloatType, LongType
)
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def create_iceberg_table_if_not_exists(spark, full_table_name, schema):
    """
    Create Iceberg table with optimized properties for CDC and auto-maintenance.
    """
    try:
        spark.read.table(full_table_name)
        print(f"  [OK] Table {full_table_name} exists.")
    except AnalysisException:
        print(f"  [NEW] Creating table {full_table_name} with optimizations...")
        try:
            def spark_type_to_sql(spark_type):
                if isinstance(spark_type, IntegerType): return "INT"
                elif isinstance(spark_type, StringType): return "STRING"
                elif isinstance(spark_type, BooleanType): return "BOOLEAN"
                elif isinstance(spark_type, TimestampType): return "TIMESTAMP"
                elif isinstance(spark_type, DoubleType): return "DOUBLE"
                elif isinstance(spark_type, FloatType): return "FLOAT"
                elif isinstance(spark_type, LongType): return "LONG"
                else: return "STRING"
            
            fields_ddl = ", ".join([
                f"`{field.name}` {spark_type_to_sql(field.dataType)}"
                for field in schema.fields
            ])
            
            # Optimization: format-version 2 is required for Row-level deletes/upserts
            # Auto-cleanup metadata to prevent PostgreSQL bloat
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {fields_ddl}
                )
                USING iceberg
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.upsert.enabled' = 'true',
                    'write.metadata.delete-after-commit.enabled' = 'true',
                    'write.metadata.previous-versions-max' = '10',
                    'write.distribution-mode' = 'hash'
                )
            """
            spark.sql(create_table_sql)
            print("  -> Created successfully with TBLPROPERTIES!")
        except Exception as e:
            print(f"  -> Error creating table: {e}")

def start_stream_for_topic(spark, topic, conf):
    """
    Initialize structured streaming for a Kafka topic with Upsert and Delete support.
    """
    table_name = conf["table_name"]
    raw_schema = conf["schema"]
    primary_keys = conf["primary_keys"]
    
    envelope_schema = schemas.get_debezium_envelope(raw_schema)
    full_table_name = f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}"
    
    print(f"\n>>> [INIT] Stream: {topic} -> {full_table_name}")

    target_schema = StructType(
        raw_schema.fields + [
            StructField("op", StringType(), True),
            StructField("ts_ms", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ]
    )
    
    create_iceberg_table_if_not_exists(spark, full_table_name, target_schema)

    # Read from Kafka with optimized trigger offsets
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .load()

    # Parse JSON & Flatten (Included 'op' for delete handling)
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), envelope_schema).alias("data")) \
        .select("data.after.*", "data.op", "data.ts_ms") 
    
    df_parsed = df_parsed.withColumn("ingestion_timestamp", current_timestamp())
    
    # Fill missing columns with NULL to maintain schema consistency
    existing_columns = set(df_parsed.columns)
    target_columns = [field.name for field in target_schema.fields]
    for col_name in target_columns:
        if col_name not in existing_columns:
            df_parsed = df_parsed.withColumn(col_name, lit(None).cast(StringType()))
            
    df_parsed = df_parsed.select(*target_columns)

    def upsert_to_iceberg(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        # 1. Deduplicate within the micro-batch to handle multiple updates to same key
        window_spec = Window.partitionBy(*primary_keys).orderBy(col("ts_ms").desc())
        dedup_df = batch_df.withColumn("_row_num", row_number().over(window_spec)) \
                           .filter("_row_num = 1") \
                           .drop("_row_num")
        
        temp_view_name = f"temp_updates_{table_name.replace('.', '_')}_{batch_id}"
        dedup_df.createOrReplaceTempView(temp_view_name)
        
        # 2. Build MERGE INTO SQL with DELETE support (op = 'd')
        join_cond = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
        cols = [c for c in dedup_df.columns if c != 'op'] # Exclude 'op' from update set if preferred
        update_set = ", ".join([f"t.{c} = s.{c}" for c in dedup_df.columns])
        insert_cols = ", ".join([f"{c}" for c in dedup_df.columns])
        insert_vals = ", ".join([f"s.{c}" for c in dedup_df.columns])
        
        # Support Hard Deletes from Debezium (op='d')
        merge_sql = f"""
            MERGE INTO {full_table_name} t
            USING {temp_view_name} s
            ON {join_cond}
            WHEN MATCHED AND s.op = 'd' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED AND s.op != 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        try:
            batch_df.sparkSession.sql(merge_sql)
            # Log without extra count() scan to save time
            print(f"  [DONE] Batch {batch_id} | Table: {table_name} | Upsert/Delete processed.")
        except Exception as e:
            print(f"  [ERROR] Batch {batch_id} | Table: {table_name} | Error: {e}")
        finally:
            batch_df.sparkSession.catalog.dropTempView(temp_view_name)

    # Start streaming with 5-minute trigger to reduce commit frequency and help metadata compaction
    query = df_parsed.writeStream \
        .foreachBatch(upsert_to_iceberg) \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/{table_name}") \
        .start()
    
    return query

def main():
    print(">>> STARTING OPTIMIZED HOSPITAL LAKEHOUSE STREAMING...")

    # Spark Engine Tuning for Streaming
    spark = SparkSession.builder \
        .appName("Hospital_CDC_Optimized") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.his_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.his_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.his_catalog.uri", f"jdbc:postgresql://{POSTGRES_IP}:5432/iceberg_catalog") \
        .config("spark.sql.catalog.his_catalog.jdbc.user", POSTGRES_USER) \
        .config("spark.sql.catalog.his_catalog.jdbc.password", POSTGRES_PASSWORD) \
        .config("spark.sql.catalog.his_catalog.warehouse", f"s3a://{BUCKET_NAME}/iceberg_warehouse") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # DB Initializations
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.default")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}")

    active_streams = []
    for topic, conf in schemas.TABLE_CONFIGS.items():
        try:
            stream = start_stream_for_topic(spark, topic, conf)
            active_streams.append(stream)
        except Exception as e:
            print(f"!!! ERROR INIT TOPIC {topic}: {e}")

    print(f"\n>>> ACTIVATED {len(active_streams)} STREAM(S). MONITORING...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
