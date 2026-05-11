# Data Lakehouse Platform Bệnh Viện

Nạp dữ liệu CDC thời gian thực, lưu trữ lakehouse dựa trên Iceberg, biến đổi gần thời gian thực với dbt, và dashboard BI được phục vụ thông qua Spark Thrift Server và Apache Superset.

## Mục Lục

- [1. Bối cảnh](#1-bối-cảnh)
- [2. Triển khai](#2-triển-khai)
  - [Mô tả các bước triển khai](#mô-tả-các-bước-triển-khai)
    - [Tổng quan Input/Output](#tổng-quan-inputoutput)
    - [2.1 Tổng quan dự án](#21-tổng-quan-dự-án)
    - [2.2 Tổng quan kiến trúc](#22-tổng-quan-kiến-trúc)
    - [2.3 Luồng dữ liệu đầu-cuối](#23-luồng-dữ-liệu-đầu-cuối)
    - [2.4 Lớp ingest dữ liệu](#24-lớp-ingest-dữ-liệu)
      - [2.4.1 Core HIS là hệ thống nguồn](#241-core-his-là-hệ-thống-nguồn)
      - [2.4.2 Debezium CDC ghi nhận thay đổi dữ liệu](#242-debezium-cdc-ghi-nhận-thay-đổi-dữ-liệu)
      - [2.4.3 Kafka nhận các sự kiện CDC](#243-kafka-nhận-các-sự-kiện-cdc)
      - [2.4.4 Spark Structured Streaming đọc dữ liệu từ Kafka](#244-spark-structured-streaming-đọc-dữ-liệu-từ-kafka)
    - [2.5 Lớp lưu trữ và lakehouse](#25-lớp-lưu-trữ-và-lakehouse)
      - [2.5.1 Spark ghi vào lớp lưu trữ Iceberg](#251-spark-ghi-vào-lớp-lưu-trữ-iceberg)
      - [2.5.2 Bronze Silver và Gold định nghĩa các tầng lưu trữ](#252-bronze-silver-và-gold-định-nghĩa-các-tầng-lưu-trữ)
      - [2.5.3 Vì sao chọn Apache Iceberg](#253-vì-sao-chọn-apache-iceberg)
      - [2.5.4 MinIO lưu trữ các tệp dữ liệu vật lý](#254-minio-lưu-trữ-các-tệp-dữ-liệu-vật-lý)
      - [2.5.5 PostgreSQL quản lý metadata Iceberg](#255-postgresql-quản-lý-metadata-iceberg)
    - [2.6 Biến đổi dữ liệu với dbt](#26-biến-đổi-dữ-liệu-với-dbt)
    - [2.7 Dashboard và các bài toán phân tích](#27-dashboard-và-các-bài-toán-phân-tích)
    - [2.8 Lớp query và BI](#28-lớp-query-và-bi)
      - [2.8.1 Spark Thrift Server cung cấp lớp query](#281-spark-thrift-server-cung-cấp-lớp-query)
      - [2.8.2 Superset cung cấp lớp trực quan hóa](#282-superset-cung-cấp-lớp-trực-quan-hóa)
    - [2.9 Điều phối và vận hành bảo trì](#29-điều-phối-và-vận-hành-bảo-trì)
    - [2.10 Ghi chú triển khai](#210-ghi-chú-triển-khai)
- [3. Kỹ năng và thành tựu sau khi hoàn thành dự án](#3-kỹ-năng-và-thành-tựu-sau-khi-hoàn-thành-dự-án)
  - [3.1 Kỹ năng kỹ thuật](#31-kỹ-năng-kỹ-thuật)
  - [3.2 Kỹ năng công cụ](#32-kỹ-năng-công-cụ)
  - [3.3 Kiến thức domain](#33-kiến-thức-domain)
  - [3.4 Tài liệu hóa quy trình nghiệp vụ và dữ liệu](#34-tài-liệu-hóa-quy-trình-nghiệp-vụ-và-dữ-liệu)
- [4. Định hướng phát triển](#4-định-hướng-phát-triển)
- [Tổng kết](#tổng-kết)

## 1. Bối cảnh

Dự án này được thiết kế cho môi trường phòng khám hoặc bệnh viện, nơi các hệ thống vận hành phát sinh dữ liệu giao dịch liên tục từ đăng ký bệnh nhân, nhập viện, khám bệnh, thanh toán, xét nghiệm và các quy trình chăm sóc liên quan khác.

Trong bối cảnh y tế như vậy, dữ liệu thường được lưu trữ trước tiên trong các ứng dụng vận hành cốt lõi như `Hospital Information System (HIS)`. Hệ thống này được tối ưu cho vận hành hằng ngày, phân tích lịch sử, báo cáo liên phòng ban, và giám sát gần thời gian thực.

Platform trong repository này cung cấp một cách để đưa dữ liệu vận hành đó vào một lakehouse tập trung, giúp phòng khám hỗ trợ:

- giám sát gần thời gian thực các hoạt động bệnh nhân và dịch vụ
- báo cáo quản trị xuyên suốt giữa các phòng ban và theo các mốc thời gian
- phân tích lịch sử đáng tin cậy cho kiểm toán và đánh giá vận hành
- các bài toán khoa học dữ liệu, machine learning, hoặc AI trong tương lai trên dữ liệu y tế đã được chuẩn hóa

Mục tiêu tổng thể là cung cấp cho tổ chức một nền tảng phân tích có khả năng mở rộng mà không làm gián đoạn các hệ thống nguồn mà nhân viên đang dùng cho công việc lâm sàng và hành chính hằng ngày.

## 2. Triển khai
<p align="center">
  <img src="./readme_pic/de_flowchart.jpg" alt="Flow Chart"/>
  <b>Figure 1:</b> Flow chart <br>
</p>

### Mô tả các steps triển khai

#### Tổng quan Input/Output

- `Input`: dữ liệu giao dịch từ `HIS` cốt lõi của phòng khám, bao gồm các bản ghi vận hành như đăng ký, lượt khám, dịch vụ, thanh toán và các hoạt động y tế hằng ngày khác.
  - Link **mô tả dữ liệu**: [Link](https://docs.google.com/spreadsheets/d/1eGdW56kQfhWlkBmLUB7z0Jgh5vEu-bKBL0QVoc3e2bI/edit?usp=sharing)
- `Processing`: ghi nhận CDC với `Debezium`, streaming sự kiện qua `Kafka`, ingest với `Spark Structured Streaming`, lưu trữ trong `Iceberg` trên `MinIO`, biến đổi với `dbt`, và điều phối bằng `Airflow` và `Crontab`.
- `Output`: các dataset `Bronze`, `Silver`, `Gold` đã được chuẩn hóa, các bảng phân tích có thể truy vấn bằng SQL thông qua `Spark Thrift Server`, và dashboard phân quyền theo vai trò trong `Superset` cho từng nhóm người dùng.

Quá trình triển khai đi theo flowchart như một pipeline theo từng lớp, bắt đầu từ hệ thống vận hành của phòng khám, đi qua ingest streaming và lưu trữ lakehouse, sau đó kết thúc ở các bước biến đổi, phục vụ SQL, trực quan hóa và bảo trì.

#### 2.1 Tổng quan dự án

Dự án này là một nền tảng data engineering cho bệnh viện được xây dựng trên kiến trúc lakehouse. Mục tiêu của nó là đưa dữ liệu từ các hệ thống vận hành của bệnh viện vào một môi trường sẵn sàng cho phân tích, hỗ trợ dashboard vận hành gần thời gian thực, mô hình dimension và fact cho báo cáo, theo dõi lịch sử đáng tin cậy thông qua Iceberg snapshots, và mở rộng cho các bài toán machine learning hoặc AI trong tương lai.

Pipeline thực tế là:

`Core HIS -> Debezium -> Kafka -> Spark Structured Streaming -> Iceberg on MinIO -> dbt -> Spark Thrift Server -> Superset`

#### 2.2 Tổng quan kiến trúc

Hệ thống tách riêng trách nhiệm giữa các thành phần ingest, lưu trữ, biến đổi, điều phối và trực quan hóa:

- `Debezium` ghi nhận các sự kiện CDC từ cơ sở dữ liệu nguồn của bệnh viện.
- `Kafka` vận chuyển các thay đổi bảng dữ liệu dưới dạng sự kiện streaming.
- `Spark Structured Streaming` đọc Kafka liên tục và ghi dữ liệu raw vào các bảng Iceberg.
- `MinIO` lưu trữ các tệp dữ liệu vật lý.
- `Apache Iceberg` cung cấp table format, snapshots và hành vi ACID.
- `PostgreSQL` đóng vai trò JDBC catalog cho Iceberg và chỉ lưu metadata.
- `dbt` biến đổi các bảng raw thành các model staging, dimension và fact đã được chuẩn hóa.
- `Airflow` và `Crontab` lên lịch cho công việc transform và bảo trì.
- `Spark Thrift Server` cung cấp quyền truy cập vào hệ thống lakehouse thông qua SQL.
- `Superset` cung cấp dashboard và khả năng phân tích cho người dùng cuối.

#### 2.3 End-to-End Data Flow

1. Các thay đổi trong hệ thống cốt lõi của bệnh viện được ghi nhận thông qua CDC.
2. **Debezium** đẩy những thay đổi đó vào các topic Kafka.
3. **Spark Structured Streaming** chạy liên tục và đọc dữ liệu từ Kafka.
4. **Spark** phân tích CDC envelope, giữ lại các cột kỹ thuật như `op` và `ts_ms`, sau đó merge bản ghi vào các bảng raw Iceberg trên MinIO.
5. **PostgreSQL** theo dõi metadata Iceberg như vị trí bảng, manifest và snapshots.
6. **dbt** xây dựng các tầng staging và mart đã được chuẩn hóa, bao gồm bảng dimension và fact.
7. **Spark Thrift Server** cung cấp dữ liệu đã được xử lý (curated data) để truy cập bằng SQL.
8. **Superset** truy vấn các bảng đã chuẩn hóa để phục vụ dashboard và báo cáo.
9. **Airflow** và **Crontab** lên lịch refresh, điều phối và bảo trì pipeline.

#### 2.4 Lớp ingest dữ liệu

##### 2.4.1 Core HIS là Source System

Quy trình bắt đầu từ `HIS` cốt lõi của phòng khám, nơi lưu trữ dữ liệu giao dịch được tạo ra từ các hoạt động hằng ngày như đăng ký, lượt khám, dịch vụ, thanh toán và vận hành lâm sàng.

##### 2.4.2 Debezium CDC ghi nhận thay đổi dữ liệu

`Debezium` lắng nghe cơ sở dữ liệu nguồn và ghi nhận insert, update và delete thành các sự kiện thay đổi, cho phép dữ liệu di chuyển liên tục mà không cần trích xuất lại toàn bộ bảng nhiều lần.

##### 2.4.3 Kafka nhận các sự kiện CDC

Các thay đổi đã được ghi nhận sẽ được đẩy vào các topic `Kafka`, đóng vai trò là lớp streaming sự kiện giữa hệ thống nguồn và xử lý downstream.

##### 2.4.4 Spark Structured Streaming đọc dữ liệu từ Kafka

Job streaming chạy liên tục, consume các sự kiện CDC từ Kafka, phân tích cấu trúc message, giữ lại các trường CDC kỹ thuật, và chuẩn bị bản ghi cho lớp raw trong lakehouse.

Spark là công cụ streaming được lựa chọn cho dự án này. Theo quyết định của dự án, Spark Structured Streaming đã đáp ứng được yêu cầu streaming, vì vậy Flink không cần thiết trừ khi trở thành một yêu cầu bắt buộc từ bên ngoài.

#### 2.5 Lớp lưu trữ và lakehouse

##### 2.5.1 Spark ghi vào lớp lưu trữ Iceberg

Sau khi đọc sự kiện, Spark ghi dữ liệu vào lớp bảng `Iceberg`, được tổ chức thành các tầng `Bronze`, `Silver` và `Gold`.

##### 2.5.2 Bronze, Silver và Gold định nghĩa các tầng lưu trữ dữ liệu

- `Bronze`: lưu dữ liệu CDC raw vừa ingest.
- `Silver`: lưu dữ liệu đã được làm sạch, chuẩn hóa và tinh chỉnh một phần.
- `Gold`: lưu các dataset sẵn sàng cho nghiệp vụ, phục vụ phân tích, báo cáo và dashboard.

<p align="center">
  <img src="./readme_pic/iceberg.png" alt="iceberg"/>
  <b>Figure 2:</b> Parquet files in Iceberg format<br>
</p>

##### 2.5.3 Vì sao chọn Apache Iceberg?

Apache Iceberg là lựa chọn thiết kế trung tâm vì các tệp CSV, JSON hoặc text thông thường không mở rộng tốt cho bài toán phân tích dựa trên CDC.

Các lợi ích chính gồm:

- `ACID transactions`: cập nhật, xóa và merge an toàn hơn
- `Time travel`: truy vấn các snapshot trước đó bằng timestamp hoặc snapshot ID
- `Schema evolution`: thêm hoặc sửa cột mà không cần viết lại toàn bộ dữ liệu lịch sử
- `Metadata pruning`: Spark có thể bỏ qua các tệp không liên quan thay vì quét toàn bộ lake
- `Small-file management`: hỗ trợ compaction để khôi phục hiệu năng

Trong pipeline CDC, sự kiện insert tạo ra bản ghi, sự kiện update tạo ra snapshot mới hơn, và sự kiện delete được xử lý thông qua merge logic. Mặc định, truy vấn thông thường đọc snapshot mới nhất trong khi các phiên bản cũ vẫn còn tồn tại cho đến khi chính sách retention xóa bỏ.

##### 2.5.4 MinIO lưu trữ các tệp dữ liệu vật lý

Lớp lưu trữ được hỗ trợ bởi object storage `MinIO`, nơi các tệp `Parquet` bên dưới của các bảng Iceberg được lưu giữ.

##### 2.5.5 PostgreSQL quản lý metadata Iceberg

Một `PostgreSQL Catalog` riêng biệt lưu metadata cho các bảng Iceberg, bao gồm định nghĩa bảng, manifest và tham chiếu snapshot, trong khi dữ liệu nghiệp vụ thực tế vẫn nằm trong object storage.

Lớp metadata này rất quan trọng cho:

- lập kế hoạch truy vấn nhanh
- theo dõi snapshot
- time travel
- đọc và ghi đồng thời an toàn

#### 2.6 Biến đổi dữ liệu với dbt

`dbt` hoạt động trên các tầng Bronze, Silver và Gold để thực hiện logic biến đổi và tổng hợp, biến dữ liệu streaming raw thành các model sẵn sàng cho phân tích.

Hướng mô hình hóa bao gồm:

- các model `staging` để làm sạch và chuẩn hóa bảng nguồn raw
- các model `marts` để tạo bảng dimension và fact cho báo cáo
- các model vật lý được hỗ trợ bởi Iceberg thay vì logic truy vấn tạm thời

<p align="center">
  <img src="./readme_pic/dbt_example.png" alt="dbt_example"/>
  <b>Figure 3:</b> Dbt model example<br>
</p>

Đối với các use case gần thời gian thực, dbt sử dụng incremental model với merge logic để chỉ xử lý các bản ghi mới đến hoặc vừa thay đổi.

#### 2.7 Dashboard và các bài toán phân tích

Nền tảng này được thiết kế để hỗ trợ nhiều loại dashboard, mỗi loại có nhịp refresh và mục đích nghiệp vụ khác nhau.

Các nhóm dashboard bao gồm:

- `Near real-time operational dashboards`: giám sát các hoạt động thay đổi nhanh như nhập viện, luồng bệnh nhân, hoặc sự kiện dịch vụ.
- `Periodic management dashboards`: hỗ trợ báo cáo theo tuần, tháng, quý và năm.
- `Department or service dashboards`: phân tách hoạt động theo đơn vị y khoa, nhóm dịch vụ, hoặc các chiều liên quan đến nhân sự.
- `Executive summary dashboards`: trình bày KPI tổng quan cho ban lãnh đạo bệnh viện.

Đối với luồng dashboard gần thời gian thực:

1. `Spark Structured Streaming` tiếp tục ingest dữ liệu CDC liên tục vào tầng Bronze.
2. `dbt` xây dựng tầng Gold bằng các bảng incremental vật lý.
3. `Airflow` chạy một workflow NRT chuyên biệt mỗi 3 phút.
4. `Superset` đọc trực tiếp từ các bảng Gold đã được làm sạch để phục vụ dashboard.

<!-- Important NRT implementation details include:

- keep `op` and `ts_ms` for CDC-aware downstream logic
- use a `1 minute` Spark trigger to keep raw data fresh
- use `materialized='incremental'` with `incremental_strategy='merge'`
- filter only records with `ts_ms` greater than the latest timestamp already present
- deduplicate with `ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC)`
- exclude deleted records with `op != 'd'` when building serving tables -->

Các dashboard batch và dashboard đã chuẩn hóa có thể sử dụng chu kỳ refresh dài hơn và cache windows lớn hơn để giảm tải cho Spark Thrift Server.

#### 2.8 Lớp query và BI

##### 2.8.1 Spark Thrift Server cung cấp lớp query

Khi các bảng đã chuẩn hóa sẵn sàng, `Spark SQL Thrift Server` cung cấp một query engine dựa trên SQL để các công cụ BI có thể truy cập dữ liệu theo cách tiêu chuẩn.

##### 2.8.2 Superset cung cấp lớp trực quan hóa

`Superset` kết nối tới query engine và cung cấp dashboard, chart và các màn hình báo cáo cho người dùng cuối.

Trong dự án này, quyền truy cập dashboard được kiểm soát theo vai trò người dùng để mỗi nhóm chỉ nhìn thấy các dashboard phù hợp với trách nhiệm của mình, như `Ban giám đốc`, `Trưởng khoa`, `Nhân sự`, `Kế toán`, và `Dược`, v.v.

<p align="center">
  <img src="./readme_pic/superset_visual.png" alt="superset_visual"/>
  <b>Figure 4:</b> Superset dashboard example<br>
</p>

Thiết kế này đặc biệt hữu ích cho các tình huống báo cáo như xu hướng nhập viện, mức độ sử dụng dịch vụ, giám sát hoạt động bệnh nhân, và báo cáo quản trị theo ngày, tuần, tháng, quý hoặc năm.

#### 2.9 Điều phối và vận hành bảo trì

`Airflow` và `Crontab` chịu trách nhiệm lên lịch các job biến đổi, chu kỳ refresh và các tác vụ vận hành bảo trì để giữ cho nền tảng chạy ổn định.

Các cơ chế bảo vệ trong điều phối bao gồm:

- `schedule_interval='*/3 * * * *'`
- `max_active_runs=1`
- `catchup=False`

Điều này ngăn các lần chạy chồng lên nhau, tránh replay storm sau khi downtime, và giữ cho server ổn định.

Bảo trì cũng rất cần thiết vì ingest gần thời gian thực tạo ra rất nhiều small files. Các tác vụ quan trọng bao gồm:

- `rewrite_data_files` để compact small files thành các tệp lớn hơn
- `rewrite_manifests` để giảm overhead metadata
- `expire_snapshots` để xóa lịch sử cũ và kiểm soát tăng trưởng lưu trữ
- các xử lý vận hành như khắc phục lỗi đầy inode khi cần

<p align="center">
  <img src="./readme_pic/air_flow.png" alt="DAG airflow"/>
  <b>Figure 5:</b> DAG airflow <br>
</p>

#### 2.10 Ghi chú triển khai

Dự án được thiết kế để chạy trên hạ tầng Linux VPS với tài nguyên vừa phải.

Các mẫu hình hạ tầng bao gồm:

- `MinIO` cài đặt native để giảm memory overhead so với Docker
- các Spark job chạy dài hạn được quản lý bằng `tmux`, `nohup`, hoặc `systemd`
- `Airflow` được triển khai với swap khi cần trên máy có ít bộ nhớ
- các service `Airflow` được quản lý bằng `systemd` để tự động khởi động và restart
- `Superset` được triển khai riêng và kết nối tới Spark Thrift Server

Các mối quan tâm khi vận hành bao gồm Spark streaming liên tục tạo ra nhiều small files, metadata Iceberg tăng nhanh nếu snapshots không bao giờ được expire, cần chỉnh timeout truy vấn trong Superset, và hiệu năng PostgreSQL catalog ảnh hưởng đến lập kế hoạch truy vấn.

<!-- #### 2.11 Current Progress

Based on the project notes, the implementation status is:

1. `Core HIS -> Debezium -> Kafka`: completed
2. `Kafka -> Spark Structured Streaming -> Iceberg raw data on MinIO`: completed
3. `Airflow` orchestration for dbt: implemented as the next operational layer
4. `Near real-time dashboard flow`: defined and optimized around a 3-minute cycle
5. `Superset` dashboards on top of curated data: final serving layer -->

## 3. Kỹ năng đạt được sau khi hoàn thành project

Dự án này không chỉ mang lại một nền tảng phân tích y tế đang hoạt động, mà còn giúp hình thành một bộ kỹ năng và thành tựu thực tế rõ ràng thông qua quá trình thiết kế, triển khai, vận hành và phát triển dashboard.

### 3.1 Techical skills

Hoàn thành dự án này giúp củng cố các kỹ năng cốt lõi trong data engineering hiện đại, đặc biệt là:

- thiết kế kiến trúc lakehouse dựa trên CDC từ đầu đến cuối
- xây dựng pipeline streaming với `Debezium`, `Kafka` và `Spark Structured Streaming`
- mô hình hóa nền tảng dữ liệu theo kiến trúc `Bronze`, `Silver` và `Gold`
- làm việc với `Apache Iceberg` cho bảng ACID, quản lý snapshot, schema evolution và time travel
- triển khai logic biến đổi gần thời gian thực với xử lý incremental và merge strategies
- mở dữ liệu sẵn sàng cho phân tích thông qua lớp phục vụ SQL cho BI

### 3.2 Tool skills

Dự án cũng phát triển khả năng thực hành với các công cụ chính được sử dụng trong toàn bộ nền tảng, bao gồm:

- cấu hình connector `Debezium` cho ingest CDC
- quản lý Kafka topics và hiểu được cơ chế di chuyển dữ liệu theo sự kiện
- phát triển Spark jobs cho ingest liên tục và ghi bảng
- sử dụng `MinIO` làm object storage cho các tệp dữ liệu lakehouse
- bảo trì `PostgreSQL` Iceberg catalog cho quản lý metadata
- xây dựng các model biến đổi trong `dbt`
- lên lịch workflow và công việc bảo trì bằng `Airflow` và `Crontab`
- phục vụ dữ liệu qua `Spark Thrift Server`
- tạo dashboard và phân quyền theo vai trò trong `Superset`
- vận hành nền tảng trên môi trường Linux với các công cụ như `systemd`, `tmux` và `nohup`

### 3.3 Kiến thức domain

Bên cạnh phần kỹ thuật, dự án này còn giúp xây dựng kiến thức domain trong phân tích y tế và báo cáo vận hành, như:

- hiểu cách các hệ thống nguồn của bệnh viện hoặc phòng khám tạo ra dữ liệu giao dịch
- nhận diện các object y tế quan trọng như bệnh nhân, lượt khám, dịch vụ, hóa đơn, khoa phòng và đơn vị dịch vụ
- hiểu sự khác nhau giữa ghi nhận dữ liệu vận hành và dữ liệu đã sẵn sàng cho báo cáo phân tích
- thiết kế dashboard cho các vai trò tổ chức khác nhau như `Ban giám đốc`, `Trưởng khoa`, `Nhân sự`, `Kế toán`, và `Dược`
- cân bằng nhu cầu giám sát gần thời gian thực với nhu cầu báo cáo quản trị định kỳ
- nhận ra các ràng buộc vận hành trong môi trường y tế, nơi hệ thống nguồn phải luôn ổn định trong khi workload phân tích vẫn chạy song song

### 3.4 Tài liệu hóa quy trình nghiệp vụ và dữ liệu

Bên cạnh việc triển khai nền tảng dữ liệu, dự án cũng bao gồm các tài liệu phân tích và tài liệu hóa bổ trợ.

Những tài liệu này giúp kết nối pipeline kỹ thuật với bối cảnh nghiệp vụ thực tế:

- File `.bpmn` mô tả quy trình vận hành và quy trình nghiệp vụ trong môi trường bệnh viện hoặc phòng khám

<p align="center">
  <img src="./readme_pic/bpmn.png" alt="bpmn"
  <b>Figure 6:</b> Business process diagram example <br>
</p>

- File `.dbml` mô tả cấu trúc cơ sở dữ liệu, quan hệ giữa các bảng, và các thực thể nghiệp vụ được sử dụng trong hệ thống nguồn

Tài liệu này quan trọng vì nó giúp giải thích cách các hoạt động vận hành được chuyển thành dữ liệu nguồn, cách các bảng liên kết với quy trình nghiệp vụ thực tế, và cách các model phân tích downstream nên được thiết kế.

## 4. Định hướng phát triển

Các bước tiếp theo tiềm năng đã được đề cập trong ghi chú lập kế hoạch của dự án bao gồm:

- dashboard machine learning xây dựng trên dữ liệu đã chuẩn hóa có thể đọc bằng Spark
- workflow AI hoặc chatbot để tra cứu lịch sử điều trị và hỗ trợ ra quyết định
- dashboard quản trị bệnh viện phong phú hơn cho báo cáo điều hành

## Tổng kết

Dự án này cho thấy một kiến trúc lakehouse thực tế cho bệnh viện, kết hợp ingest streaming, lưu trữ bảng giao dịch, biến đổi có lên lịch, và dashboard gần thời gian thực trên hạ tầng hạn chế.

Những quyết định kỹ thuật quan trọng nhất trong dự án bao gồm:

- giữ Spark Structured Streaming là ingest engine cốt lõi
- sử dụng Iceberg trên MinIO thay vì tệp raw thông thường
- sử dụng PostgreSQL làm metadata catalog thay vì kho dữ liệu nghiệp vụ
- sử dụng dbt kết hợp Airflow để cầu nối dữ liệu CDC raw thành các model NRT và báo cáo đã chuẩn hóa
