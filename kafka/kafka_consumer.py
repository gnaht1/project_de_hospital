import os
import sys
from pyspark.sql import SparkSession

# --- CẤU HÌNH MÔI TRƯỜNG CHO WINDOWS ---
# Trỏ Python worker về đúng file python đang chạy hiện tại để tránh lỗi
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- CẤU HÌNH PACKAGE CHO SPARK 4.1.1 ---
# Spark 4.x mặc định dùng Scala 2.13
# Format: org.apache.spark:spark-sql-kafka-0-10_<SCALA_VERSION>:<SPARK_VERSION>
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

print(f"--- Khoi tao Spark voi package: {KAFKA_PACKAGE} ---")

spark = SparkSession.builder \
    .appName("DebeziumKafkaConsumer") \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Giảm bớt log rác
spark.sparkContext.setLogLevel("WARN")

# --- KAFKA CONFIG ---
# Thay IP VPS của bạn vào đây
KAFKA_BOOTSTRAP_SERVERS = "172.30.2.37:9092" 
# Thay tên topic Debezium của bạn (ví dụ: dbserver1.public.users)
KAFKA_TOPIC = "his.core_his_prod.dm_nguon_nb" 

print(f"--- Dang ket noi toi Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC} ---")

try:
    # 1. Read Stream
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Process Data (Chuyển binary sang string để đọc được)
    df_string = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # 3. Output to Console
    query = df_string.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("--- Stream da bat dau. Cho du lieu tu Kafka... (Ctrl+C de dung) ---")
    query.awaitTermination()

except Exception as e:
    print("\n[LOI ROI]:", e)
    print("\nLuu y: Kiem tra lai xem da cai winutils.exe va set JAVA_HOME chua?")