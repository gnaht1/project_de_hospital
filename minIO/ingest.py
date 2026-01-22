import json
import io
import time
import re
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
from collections import defaultdict

# --- CONFIGURATION ---
KAFKA_SERVER = '172.30.2.37:9092' # Thay IP VPS Kafka của bạn
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = '12345678' # Pass bạn vừa đổi
MINIO_BUCKET = 'hospital-datalake'

# Regex để lấy tất cả topic. 
# '^.*$' nghĩa là lấy hết. 
# Nếu muốn chỉ lấy topic bắt đầu bằng 'hospital', dùng: '^hospital.*$'
# TOPIC_PATTERN = '^.*$' 
TOPIC_PATTERN = '^his\.core_his_prod\..*$' 

# Số lượng message mỗi topic cần gom đủ trước khi upload
BATCH_SIZE = 50 

def connect_minio():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def main():
    print(f"Starting Multi-Topic ETL Worker...")
    
    # 1. Setup MinIO
    minio_client = connect_minio()
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
    
    # 2. Setup Kafka Consumer with Pattern Subscription
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='minio-multi-topic-group-v1',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            # Subscribe using Regex Pattern
            consumer.subscribe(pattern=TOPIC_PATTERN)
            print(f"Connected to Kafka. Listening to pattern: '{TOPIC_PATTERN}'")
            break
        except Exception as e:
            print(f"Waiting for Kafka... Error: {e}")
            time.sleep(5)

    # 3. Buffer Dictionary: { 'topic_A': [msg1, msg2], 'topic_B': [msg1] }
    topic_buffers = defaultdict(list)
    
    print("Worker is running...")
    
    try:
        for message in consumer:
            topic = message.topic
            
            # Skip internal Kafka topics (like __consumer_offsets)
            if topic.startswith("__"):
                continue
                
            # Add message to the specific buffer for this topic
            topic_buffers[topic].append(message.value)
            
            # Check if THIS specific topic has enough data to upload
            if len(topic_buffers[topic]) >= BATCH_SIZE:
                current_buffer = topic_buffers[topic]
                
                # --- Upload Logic ---
                # Structure: topic_name/YYYY/MM/DD/timestamp.json
                now = datetime.now()
                path_prefix = f"{topic}/{now.year}/{now.month:02d}/{now.day:02d}"
                timestamp_str = now.strftime("%H%M%S%f")
                filename = f"{path_prefix}/data_{timestamp_str}.json"
                
                try:
                    data_bytes = json.dumps(current_buffer).encode('utf-8')
                    minio_client.put_object(
                        MINIO_BUCKET,
                        filename,
                        io.BytesIO(data_bytes),
                        length=len(data_bytes),
                        content_type='application/json'
                    )
                    
                    print(f"[UPLOAD] {topic}: {len(current_buffer)} rows -> {filename}")
                    
                    # Clear only this topic's buffer
                    topic_buffers[topic] = []
                    
                except Exception as ex:
                    print(f"[ERROR] Failed to upload {topic}: {ex}")

    except KeyboardInterrupt:
        print("Stopping worker...")
        # Optional: Flush remaining data in buffers before exit
        # Duyệt qua tất cả các topic đang có dữ liệu chờ
        for topic, buffer in topic_buffers.items():
            if len(buffer) > 0:
                # Logic upload tương tự như trên
                now = datetime.now()
                path_prefix = f"{topic}/{now.year}/{now.month:02d}/{now.day:02d}"
                timestamp_str = now.strftime("%H%M%S%f")
                filename = f"{path_prefix}/data_flush_{timestamp_str}.json" # Thêm chữ flush để dễ nhận biết
                
                try:
                    data_bytes = json.dumps(buffer).encode('utf-8')
                    minio_client.put_object(
                        MINIO_BUCKET,
                        filename,
                        io.BytesIO(data_bytes),
                        length=len(data_bytes),
                        content_type='application/json'
                    )
                    print(f"[FLUSH] {topic}: Saved remaining {len(buffer)} rows.")
                except Exception as ex:
                    print(f"[ERROR] Failed to flush {topic}: {ex}")
        
        print("Worker stopped safely.")

if __name__ == "__main__":
    main()