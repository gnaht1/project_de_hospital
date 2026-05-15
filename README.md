# Hospital Data Lakehouse Platform

Vietnamese version: [Click here](./readme_vn.md).

<!-- This platform builds a hospital data pipeline based on a lakehouse architecture, moving transactional HIS data into a near real-time analytics environment. The design focuses on four core qualities: **accuracy**, **speed**, **stability**, and **security**. -->

Main pipeline:

`Core HIS -> Debezium -> Kafka -> Spark Structured Streaming -> Apache Iceberg on MinIO -> dbt -> Spark Thrift Server -> Apache Superset`

## Table of Contents

- [1. Goals](#1-goals)
- [2. Four Engineering Values](#2-four-engineering-values)
  - [2.1 Accuracy](#21-accuracy)
  - [2.2 Speed](#22-speed)
  - [2.3 Stability](#23-stability)
  - [2.4 Security](#24-security)
- [3. Architecture Overview](#3-architecture-overview)
- [4. End-to-End Data Flow](#4-end-to-end-data-flow)
- [5. Layered Design](#5-layered-design)
  - [5.1 Input, Processing, and Output](#51-input-processing-and-output)
  - [5.2 Bronze, Silver, and Gold](#52-bronze-silver-and-gold)
  - [5.3 Why Apache Iceberg](#53-why-apache-iceberg)
  - [5.4 Role of the PostgreSQL Catalog](#54-role-of-the-postgresql-catalog)
  - [5.5 dbt Transformation](#55-dbt-transformation)
- [6. Dashboards and Analytics](#6-dashboards-and-analytics)
- [7. Operations and Maintenance](#7-operations-and-maintenance)
  - [7.1 Airflow](#71-airflow)
  - [7.2 Iceberg and MinIO Maintenance](#72-iceberg-and-minio-maintenance)
  - [7.3 VPS Deployment](#73-vps-deployment)
- [8. Business Documentation](#8-business-documentation)
- [9. Skills Gained](#9-skills-gained)
  - [9.1 Technical Skills](#91-technical-skills)
  - [9.2 Tool Skills](#92-tool-skills)
  - [9.3 Domain Knowledge](#93-domain-knowledge)
- [10. Future Directions](#10-future-directions)
- [Summary](#summary)

## 1. Goals

Hospital data is generated continuously from patient registration, consultations, service orders, clinical services, billing, medication dispensing, and other operational activities. HIS systems are usually optimized for daily transactions, not cross-department reporting, historical analytics, or near real-time dashboards.

This project creates a centralized analytics platform to:

- Monitor hospital operations in near real time.
- Standardize source data into trusted analytical tables.
- Serve dashboards for executives, departments, accounting, HR, and pharmacy teams.
- Preserve data-change history through snapshots for validation, reconciliation, and future analysis.
- Provide a foundation for future machine learning, AI, or business chatbot workflows.

## 2. Four Engineering Values

### 2.1 Accuracy

The system prioritizes data correctness from the moment records are generated in HIS until they appear on dashboards.

- `Debezium` captures changes through CDC, including inserts, updates, and deletes, instead of relying on manual batch exports.
- `Spark Structured Streaming` preserves technical fields such as `op` and `ts_ms` so downstream logic can identify create, update, and delete events.
- `MERGE INTO` on `Apache Iceberg` updates each record to the correct latest state by primary key.
- Micro-batch deduplication keeps the latest record by `ts_ms`, reducing error when the same key changes multiple times in a short window.
- `dbt` separates staging, intermediate, and mart layers so transformations are controlled, readable, testable, and extensible.
- Iceberg snapshots allow previous versions to be queried for reconciliation or recovery after transformation errors.

### 2.2 Speed

The system is designed to move new HIS data into the analytics layer with low latency while keeping resource usage controlled.

- CDC continuously pushes changes into `Kafka`, avoiding full-table export cycles.
- Spark Streaming reads Kafka 24/7 and writes to Bronze in micro-batches.
- Ingestion triggers are designed around short cycles, for example 1 minute for the raw layer.
- Near real-time dashboards use physical Gold tables updated incrementally by dbt.
- Airflow has a dedicated NRT DAG that runs every 3 minutes with `schedule_interval='*/3 * * * *'`.
- Superset reads directly from cleaned Gold tables through Spark Thrift Server and can use 1-5 minute auto refresh for operational dashboards.

### 2.3 Stability

The pipeline is clearly separated into layers so each component has a specific responsibility, reducing operational risk on modest VPS infrastructure.

- Spark Streaming only handles continuous ingestion from Kafka into Iceberg and is not restarted by Airflow schedules.
- Airflow only orchestrates dbt and maintenance tasks, making workflows easier to observe and retry.
- `max_active_runs=1` prevents overlapping dbt runs.
- `catchup=False` prevents bulk replay after downtime, reducing the risk of RAM exhaustion or server congestion.
- Iceberg supports ACID transactions so dashboards do not read partially committed data.
- `rewrite_data_files`, `rewrite_manifests`, and `expire_snapshots` control small files, metadata growth, and storage usage.
- Long-running services such as Spark jobs, Spark Thrift Server, Airflow, and Superset can be managed with `systemd`, `tmux`, or `nohup` depending on the environment.

### 2.4 Security

The security design focuses on data responsibility separation, access control, and reducing exposure of healthcare data.

- Business data is stored in MinIO as Parquet/Iceberg files; the PostgreSQL catalog stores metadata only, not original healthcare data.
- Superset dashboards are role-based, for example executives, department heads, accounting, HR, and pharmacy.
- End users access data through dashboards or the SQL serving layer, not directly through the operational HIS database.
- MinIO keys, PostgreSQL credentials, and service accounts are separated from model logic so they can be managed through environment variables or dedicated configuration files.
- The lakehouse architecture separates analytical workloads from the source database, reducing the risk of heavy dashboards or queries affecting hospital operations.
- Iceberg snapshots and the metadata catalog support change tracking and data auditability.

## 3. Architecture Overview

<p align="center">
  <img src="./readme_pic/de_flowchart.jpg" alt="Flow Chart"/>
  <br>
  <b>Figure 1:</b> Flow chart
</p>

Main system components:

- `Core HIS`: source system that generates hospital data.
- `Debezium`: captures CDC from the source database.
- `Kafka`: transports data-change events.
- `Spark Structured Streaming`: reads Kafka and writes raw data to Iceberg.
- `MinIO`: object storage for physical Parquet files.
- `Apache Iceberg`: table format supporting ACID, snapshots, merge, delete, and schema evolution.
- `PostgreSQL JDBC Catalog`: stores Iceberg metadata such as tables, manifests, snapshots, and file locations.
- `dbt`: transforms raw data into staging, intermediate, and mart models.
- `Airflow` and `Crontab`: orchestrate transformation and maintenance jobs.
- `Spark Thrift Server`: exposes SQL access for BI tools.
- `Superset`: provides dashboards, charts, and user authorization.

## 4. End-to-End Data Flow

1. Data is generated in Core HIS when registration, visits, services, billing, or operational actions happen.
2. Debezium reads changes from the source database and publishes CDC events to Kafka topics.
3. Spark Structured Streaming consumes Kafka, parses CDC envelopes, and standardizes technical columns.
4. Spark writes data into Iceberg tables on MinIO using upsert/delete logic by primary key.
5. The PostgreSQL catalog records Iceberg table metadata, snapshots, and related files.
6. dbt reads raw data through Spark and builds staging, intermediate, and mart models.
7. Airflow orchestrates dbt on either batch or near real-time schedules.
8. Spark Thrift Server exposes a SQL layer so Superset can query curated data.
9. Superset displays dashboards according to user roles.

## 5. Layered Design

### 5.1 Input, Processing, and Output

- `Input`: transactional HIS data, including patients, visits, services, billing, departments, staff, medication, and related business operations.
- `Processing`: Debezium CDC, Kafka streaming, Spark Structured Streaming, Iceberg table format, dbt transformation, and Airflow orchestration.
- `Output`: Bronze, Silver, and Gold tables; business marts; Superset dashboards; SQL access through Spark Thrift Server.

Data description document: [Google Sheet](https://docs.google.com/spreadsheets/d/1eGdW56kQfhWlkBmLUB7z0Jgh5vEu-bKBL0QVoc3e2bI/edit?usp=sharing).

### 5.2 Bronze, Silver, and Gold

- `Bronze`: stores raw CDC data from Kafka and preserves technical fields such as `op`, `ts_ms`, and ingestion timestamp.
- `Silver`: cleans data, standardizes data types, normalizes keys, and removes basic noise.
- `Gold`: analytics-ready tables for dashboards, KPIs, and business reporting.

<p align="center">
  <img src="./readme_pic/iceberg.png" alt="Iceberg files"/>
  <br>
  <b>Figure 2:</b> Parquet files in Iceberg format
</p>

### 5.3 Why Apache Iceberg

Iceberg is central to the design because CDC pipelines need more than writing loose CSV, JSON, or Parquet files.

- `ACID transactions`: safe reads and writes, preventing dashboards from reading uncommitted states.
- `MERGE INTO`: supports CDC upserts and deletes.
- `Time travel`: query data by snapshot or timestamp.
- `Schema evolution`: controlled schema changes as source systems evolve.
- `Metadata pruning`: Spark can skip irrelevant files, improving query speed.
- `Compaction`: merges small files to improve dashboard performance and reduce metadata overhead.

### 5.4 Role of the PostgreSQL Catalog

PostgreSQL is not used as the business data warehouse in this project. It acts as the metadata catalog for Iceberg.

PostgreSQL stores:

- Table names and schemas.
- Table locations in MinIO.
- Current and historical snapshots.
- Manifests and metadata used for query planning.

This separation keeps physical healthcare data in MinIO while table-management metadata lives in PostgreSQL. Spark, dbt, and Superset through Spark Thrift Server use the catalog to locate the exact files they need instead of scanning the whole object store.

### 5.5 dbt Transformation

dbt turns raw data into business-meaningful models.

- `staging`: standardizes source tables and gives columns clearer names.
- `intermediate`: implements reusable business logic.
- `marts`: creates dashboard-serving tables by domain, such as clinical, finance, pharmacy, executive, operations, and core.
- `incremental model`: processes only new or changed data, making it suitable for NRT dashboards.
- `merge strategy`: updates physical Gold tables instead of depending on heavy and slow views.

<p align="center">
  <img src="./readme_pic/dbt_example.png" alt="dbt model example"/>
  <br>
  <b>Figure 3:</b> dbt model example
</p>

## 6. Dashboards and Analytics

Superset is the final presentation layer for business users. Dashboards are designed around multiple use cases:

- `Near real-time dashboard`: monitors fast-changing activity such as visits, waiting patients, daily revenue, or hourly specialty density.
- `Management dashboard`: supports weekly, monthly, quarterly, and yearly reporting.
- `Department dashboard`: analyzes performance by department, room, service group, or staff dimension.
- `Executive dashboard`: presents high-level KPIs for leadership.
- `Pharmacy dashboard`: tracks prescriptions, dispensing, and pharmacy activity.
- `Finance dashboard`: analyzes revenue, payments, service composition, and time-based comparison.

<p align="center">
  <img src="./readme_pic/superset_visual.png" alt="Superset dashboard"/>
  <br>
  <b>Figure 4:</b> Superset dashboard example
</p>

NRT dashboard flow:

1. Spark Streaming continuously writes CDC data into Bronze.
2. dbt incremental models update Gold on a short cycle.
3. Airflow runs the NRT DAG every 3 minutes.
4. Superset reads standardized Gold tables and refreshes as needed.

## 7. Operations and Maintenance

### 7.1 Airflow

Airflow orchestrates jobs with clear starts and ends, especially dbt and maintenance tasks.

<p align="center">
  <img src="./readme_pic/air_flow.png" alt="Airflow DAG"/>
  <br>
  <b>Figure 5:</b> Airflow DAG
</p>

Important operational settings:

- `schedule_interval='*/3 * * * *'` for the near real-time workflow.
- `max_active_runs=1` to prevent overlapping runs.
- `catchup=False` to avoid bulk replay after downtime.
- Separate NRT DAGs from batch DAGs to reduce cross-impact.

### 7.2 Iceberg and MinIO Maintenance

Continuous streaming writes can create many small files and metadata entries, so the system requires scheduled maintenance:

- `rewrite_data_files`: compact small files into larger files.
- `rewrite_manifests`: reduce metadata overhead during query planning.
- `expire_snapshots`: remove old snapshots to control storage usage.
- Monitor inode usage, disk capacity, and metadata size.
- Tune Superset timeouts, cache windows, and refresh intervals to avoid unnecessary load.

### 7.3 VPS Deployment

The project is designed to run on modest Linux VPS infrastructure.

- MinIO can be installed natively to reduce overhead compared with Docker.
- Spark Streaming can run long term with `tmux`, `nohup`, or `systemd`.
- Spark Thrift Server runs as a separate service for SQL serving.
- Airflow can use `systemd` to auto-restart the scheduler and webserver.
- Superset should use a dedicated metadata database such as PostgreSQL instead of SQLite for stable operation.

## 8. Business Documentation

The project includes not only the technical pipeline but also business documentation:

- `.bpmn` files describe workflows such as reception, consultation, clinical service orders, billing, medication dispensing, and corporate health checks.
- `.dbml` files describe the database structure and relationships between entities.
- dbt models reflect hospital domains such as clinical, finance, pharmacy, operations, executive, and core.

<p align="center">
  <img src="./readme_pic/bpmn.png" alt="BPMN"/>
  <br>
  <b>Figure 6:</b> Business process diagram example
</p>

This documentation connects the technical data pipeline with real business workflows, avoiding dashboards that are built only from tables without understanding the process behind them.

## 9. Skills Gained

### 9.1 Technical Skills

- Designing an end-to-end lakehouse architecture for hospital data.
- Building CDC pipelines with Debezium, Kafka, and Spark Structured Streaming.
- Writing CDC data to Apache Iceberg with upsert/delete logic.
- Organizing data into Bronze, Silver, and Gold layers.
- Building dbt models for staging, intermediate, and mart layers.
- Serving analytics data through Spark Thrift Server and Superset.
- Optimizing operations around small files, metadata, and snapshot retention.

### 9.2 Tool Skills

- Debezium connectors and Kafka topics.
- Spark Structured Streaming and Spark SQL.
- MinIO object storage.
- Apache Iceberg and PostgreSQL JDBC Catalog.
- dbt models, incremental processing, and merge strategy.
- Airflow DAGs, schedules, and retry control.
- Superset dashboards, datasets, charts, and role-based access.
- Linux service operation with `systemd`, `tmux`, `nohup`, cron, and swap.

### 9.3 Domain Knowledge

- Understanding how hospital data is generated from operational workflows.
- Identifying core entities such as patients, visits, services, departments, staff, invoices, and prescriptions.
- Distinguishing operational transaction data from standardized analytical data.
- Designing dashboards by user role instead of creating one shared dashboard for everyone.
- Balancing fast refresh needs with the stability requirements of healthcare systems.

## 10. Future Directions

- Add dbt data tests for primary keys, nulls, relationships, and abnormal values.
- Further standardize dashboard authorization by real user groups.
- Expand monitoring for Kafka lag, Spark Streaming status, Airflow run status, and Superset query latency.
- Optimize Iceberg snapshot retention policies based on audit requirements.
- Develop machine learning dashboards or an AI assistant on top of standardized Gold data.

## Summary

This project is a practical hospital data lakehouse platform combining CDC, streaming, Iceberg table format, dbt transformation, Airflow orchestration, and Superset dashboards.

The design's core value is concentrated in four areas:

- **Accuracy**: CDC, primary-key merges, deduplication, dbt modeling, and Iceberg snapshots.
- **Speed**: streaming ingestion, incremental transformation, and 3-minute near real-time dashboards.
- **Stability**: clear layer separation, Airflow guardrails, ACID transactions, and scheduled maintenance.
- **Security**: separation between HIS and analytics, Superset role-based access, metadata isolation, and reduced direct access to source data.
