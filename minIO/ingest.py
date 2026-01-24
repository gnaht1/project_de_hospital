import json
import io
import time
import re
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
from collections import defaultdict

# --- CONFIGURATION ---
KAFKA_SERVER = '172.30.2.37:9092' 
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = '12345678'
MINIO_BUCKET = 'hospital-datalake'

TOPIC_PATTERN = '^his\.core_his_prod\..*$' 

BATCH_SIZE = 50 
FLUSH_INTERVAL_SECONDS = 30 # (Mới) Thời gian chờ tối đa

def connect_minio():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

# (Mới) Hàm upload dùng chung để tránh viết lặp lại code
def upload_batch(minio_client, topic, data_buffer, reason="BATCH"):
    if not data_buffer:
        return

    now = datetime.now()
    path_prefix = f"{topic}/{now.year}/{now.month:02d}/{now.day:02d}"
    timestamp_str = now.strftime("%H%M%S%f")
    # Thêm reason vào tên file để dễ debug (VD: data_batch_... hoặc data_time_...)
    filename = f"{path_prefix}/data_{reason.lower()}_{timestamp_str}.json"
    
    try:
        data_bytes = json.dumps(data_buffer).encode('utf-8')
        minio_client.put_object(
            MINIO_BUCKET,
            filename,
            io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type='application/json'
        )
        print(f"[{reason}-UPLOAD] {topic}: {len(data_buffer)} rows -> {filename}")
    except Exception as ex:
        print(f"[ERROR] Failed to upload {topic}: {ex}")

def main():
    print(f"Starting Multi-Topic ETL Worker (Time & Size based)...")
    
    # 1. Setup MinIO
    minio_client = connect_minio()
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
    
    # 2. Setup Kafka Consumer
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='minio-multi-topic-group-v2', # Đổi version group để tránh offset cũ
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # (QUAN TRỌNG) Nếu 1s không có tin mới, vòng lặp consumer sẽ nhả ra để check thời gian
                consumer_timeout_ms=1000 
            )
            consumer.subscribe(pattern=TOPIC_PATTERN)
            print(f"Connected to Kafka. Listening to pattern: '{TOPIC_PATTERN}'")
            break
        except Exception as e:
            print(f"Waiting for Kafka... Error: {e}")
            time.sleep(5)

    topic_buffers = defaultdict(list)
    last_flush_time = time.time() # Mốc thời gian lần cuối check
    
    print("Worker is running...")
    
    try:
        while True:
            # Vòng lặp này sẽ chạy tối đa 1s nếu ko có tin (do config consumer_timeout_ms)
            # Sau đó nó thoát ra để xuống đoạn check thời gian bên dưới, rồi lại quay lại
            for message in consumer:
                topic = message.topic
                if topic.startswith("__"): continue
                
                topic_buffers[topic].append(message.value)
                
                # CHECK 1: Đủ số lượng (BATCH_SIZE)
                if len(topic_buffers[topic]) >= BATCH_SIZE:
                    upload_batch(minio_client, topic, topic_buffers[topic], reason="SIZE")
                    topic_buffers[topic] = [] # Clear buffer
            
            # CHECK 2: Đủ thời gian (FLUSH_INTERVAL)
            current_time = time.time()
            if current_time - last_flush_time > FLUSH_INTERVAL_SECONDS:
                # Duyệt qua các topic đang có dữ liệu tồn đọng
                for topic, buffer in topic_buffers.items():
                    if len(buffer) > 0:
                        upload_batch(minio_client, topic, buffer, reason="TIME")
                        topic_buffers[topic] = [] # Clear buffer
                
                # Reset đồng hồ
                last_flush_time = current_time

    except KeyboardInterrupt:
        print("\nStopping worker... Flushing remaining data...")
        # CHECK 3: Khi tắt chương trình
        for topic, buffer in topic_buffers.items():
            if len(buffer) > 0:
                upload_batch(minio_client, topic, buffer, reason="SHUTDOWN")
        
        print("Worker stopped safely.")

if __name__ == "__main__":
    main()