import os
from config import (
    KAFKA_SERVER, MINIO_URL, ACCESS_KEY, SECRET_KEY,
    BUCKET_NAME, CATALOG_NAME, DATABASE_NAME
)
import schemas
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType, DoubleType, FloatType, LongType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def create_iceberg_table_if_not_exists(spark, full_table_name, schema):
    """
    Create Iceberg table if it does not exist using Spark SQL DDL.
    """
    try:
        spark.read.table(full_table_name)
        print(f"  [OK] Table {full_table_name} exists.")
    except AnalysisException:
        print(f"  [NEW] Creating table {full_table_name}...")
        try:
            def spark_type_to_sql(spark_type):
                # Convert PySpark DataType to SQL Type String
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
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {fields_ddl}
                )
                USING iceberg
            """
            spark.sql(create_table_sql)
            print("  -> Created successfully!")
        except Exception as e:
            print(f"  -> Error creating table: {e}")

def start_stream_for_topic(spark, topic, conf):
    """
    Initialize structured streaming for a Kafka topic and process micro-batches.
    """
    table_name = conf["table_name"]
    raw_schema = conf["schema"]
    primary_keys = conf["primary_keys"]
    
    envelope_schema = schemas.get_debezium_envelope(raw_schema)
    full_table_name = f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}"
    
    print(f"\n>>> [INIT] Stream: {topic} -> {full_table_name}")

    # Define target schema including CDC metadata
    target_schema = StructType(
        raw_schema.fields + [
            StructField("op", StringType(), True),
            StructField("ts_ms", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ]
    )
    
    create_iceberg_table_if_not_exists(spark, full_table_name, target_schema)

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON & Flatten
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), envelope_schema).alias("data")) \
        .select("data.after.*", "data.op", "data.ts_ms") \
        .filter("op != 'd'") # Ignore hard deletes for now
    
    df_parsed = df_parsed.withColumn("ingestion_timestamp", current_timestamp())
    
    # Ensure all target columns exist to prevent schema mismatch
    existing_columns = set(df_parsed.columns)
    target_columns = [field.name for field in target_schema.fields]
    
    for col_name in target_columns:
        if col_name not in existing_columns:
            df_parsed = df_parsed.withColumn(col_name, lit(None).cast(StringType()))
            
    df_parsed = df_parsed.select(*target_columns)

    # Define upsert logic using MERGE INTO
    def upsert_to_iceberg(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        # 1. Deduplicate in the current batch (keep latest ts_ms)
        window_spec = Window.partitionBy(*primary_keys).orderBy(col("ts_ms").desc())
        dedup_df = batch_df.withColumn("_row_num", row_number().over(window_spec)) \
                           .filter("_row_num = 1") \
                           .drop("_row_num")
        
        # 2. Register temporary view for the micro-batch
        temp_view_name = f"temp_updates_{table_name}_{batch_id}"
        dedup_df.createOrReplaceTempView(temp_view_name)
        
        # 3. Build dynamic MERGE INTO SQL
        join_cond = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
        cols = dedup_df.columns
        update_set = ", ".join([f"t.{c} = s.{c}" for c in cols])
        insert_cols = ", ".join([f"{c}" for c in cols])
        insert_vals = ", ".join([f"s.{c}" for c in cols])
        
        merge_sql = f"""
            MERGE INTO {full_table_name} t
            USING {temp_view_name} s
            ON {join_cond}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        # 4. Execute MERGE and clean up
        try:
            batch_df.sparkSession.sql(merge_sql)
            print(f"  [DONE] Batch {batch_id} | Table: {table_name} | Upserted {dedup_df.count()} rows.")
        except Exception as e:
            print(f"  [ERROR] Batch {batch_id} | Table: {table_name} | Error: {e}")
        finally:
            batch_df.sparkSession.catalog.dropTempView(temp_view_name)

    # Start streaming query
    query = df_parsed.writeStream \
        .foreachBatch(upsert_to_iceberg) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/{table_name}") \
        .start()
    
    return query

def main():
    print(">>> INIT LINUX DISTRIBUTED MULTI-TABLE STREAMING...")

    # Init Spark without Windows workarounds
    spark = SparkSession.builder \
        .appName("Hospital_CDC_Master") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", f"s3a://{BUCKET_NAME}/iceberg_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}")

    active_streams = []

    # Start all streams defined in schemas.py
    for topic, conf in schemas.TABLE_CONFIGS.items():
        try:
            stream = start_stream_for_topic(spark, topic, conf)
            active_streams.append(stream)
        except Exception as e:
            print(f"!!! ERROR INIT TOPIC {topic}: {e}")

    print(f"\n>>> ACTIVATED {len(active_streams)} STREAM(S). RUNNING...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()