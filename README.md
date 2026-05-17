# Building a Data Model for Jarvis Clinic

Vietnamese version: [Link](./readme_vn.md).

<!-- This project builds a hospital data pipeline on a lakehouse architecture, moving transactional HIS data into a near real-time analytics environment. The design focuses on four core qualities: accuracy, speed, stability, and security. -->

## Table of Contents
- [1. Context](#1-context)
- [2. Architecture Overview](#2-architecture-overview)
  - [2.1. Main Pipeline](#21-main-pipeline)
  - [2.2. Step-by-Step Data Flow](#22-step-by-step-data-flow)
  - [2.3. Why Build the Architecture This Way?](#23-why-build-the-architecture-this-way)
    - [2.3.1 Accuracy](#231-accuracy)
    - [2.3.2 Speed](#232-speed)
    - [2.3.3 Stability](#233-stability)
    - [2.3.4 Security](#234-security)
- [3. Implementation Details of the Data Lakehouse Model](#3-implementation-details-of-the-data-lakehouse-model)
  - [3.1 Preparing Source Data and Business Documentation](#31-preparing-source-data-and-business-documentation)
  - [3.2 Synchronizing Core Data with Debezium and Kafka](#32-synchronizing-core-data-with-debezium-and-kafka)
  - [3.3 Ingesting Data into Bronze on MinIO and Iceberg](#33-ingesting-data-into-bronze-on-minio-and-iceberg)
  - [3.4 Building Bronze, Silver, and Gold Layers](#34-building-bronze-silver-and-gold-layers)
  - [3.5 Transforming Data with dbt](#35-transforming-data-with-dbt)
  - [3.6 Serving Dashboards with Spark Thrift Server and Superset](#36-serving-dashboards-with-spark-thrift-server-and-superset)
  - [3.7 Orchestration and Operations with Airflow](#37-orchestration-and-operations-with-airflow)
- [4. Skills Gained](#4-skills-gained)
  - [4.1 Technical Skills](#41-technical-skills)
  - [4.2 Tool Skills](#42-tool-skills)
  - [4.3 Domain Knowledge](#43-domain-knowledge)
- [5. Future Directions](#5-future-directions)
- [Summary](#summary)

## 1. Context

Jarvis Clinic needs to standardize and monitor data generated from its polyclinic management system, including reception, registration, consultations, paraclinical services, cashier and payment flows, pharmacy and dispensing, corporate health checks, patient management, department management, staff and specialty management, and operational reporting.

From a data engineering perspective, this project builds an enterprise data architecture to:

- Standardize source data into trustworthy analytical tables.
- Serve dashboards for executives, departments, accounting, HR, and pharmacy teams.
- Monitor hospital operations in near real time.
- Preserve change history through snapshots for validation, reconciliation, and further analysis.
- Provide lakehouse data for future data science, machine learning, AI, or business chatbot use cases.

## 2. Architecture Overview

<p align="center">
  <img src="./readme_pic/de_flowchart.jpg" alt="Flow Chart"/>
  <br>
  <b>Figure 1:</b> Flow chart
</p>

## 2.1. Main Pipeline

`Core HIS -> Debezium -> Kafka -> Spark Structured Streaming -> Apache Iceberg on MinIO -> dbt -> Spark Thrift Server -> Apache Superset`

Main components:

- `Core HIS`: the operational source system where hospital data is generated.
- `Debezium`: captures CDC events from the source database.
- `Kafka`: transports data change events.
- `Spark Structured Streaming`: reads Kafka and writes raw data into Iceberg.
- `MinIO`: object storage for physical Parquet files.
- `Apache Iceberg`: table format supporting ACID, snapshots, merge, delete, and schema evolution.
- `PostgreSQL JDBC Catalog`: stores Iceberg metadata such as tables, manifests, snapshots, and file locations.
- `dbt`: transforms raw data into staging, intermediate, and mart models.
- `Airflow` and `Crontab`: orchestrate transformation jobs and maintenance tasks.
- `Spark Thrift Server`: exposes SQL access for BI tools.
- `Superset`: provides dashboards, charts, and user-level access control.

## 2.2. Step-by-Step Data Flow

1. Data is generated in Core HIS when registration, visits, services, billing, or operational actions occur.
2. Debezium reads source-database changes and publishes CDC events into Kafka topics.
3. Spark Structured Streaming consumes Kafka, parses CDC envelopes, and standardizes technical columns.
4. Spark writes data into Iceberg tables on MinIO using upsert/delete logic by primary key.
5. The PostgreSQL catalog stores Iceberg table metadata, snapshots, and file references.
6. dbt reads raw data through Spark and builds staging, intermediate, and mart models.
7. Airflow orchestrates dbt on batch or near real-time schedules.
8. Spark Thrift Server exposes a SQL layer so Superset can query curated data.
9. Superset displays dashboards according to user roles.

## 2.3. Why Build the Architecture This Way?
- It aligns with the project objectives.
- It satisfies four core requirements:

### 2.3.1 Accuracy

The system prioritizes data accuracy from the moment records are created in HIS until they appear on dashboards.

- Core data from the clinic management system must be synchronized accurately and quickly into the analytics platform so that staging, Silver, Gold, and dashboards stay aligned with real operations.
- `Debezium` reads changes from core tables and publishes them into `Kafka` as CDC events, including inserts, updates, and deletes, so source-data changes are continuously recorded and synchronized instead of relying on manual batch exports.
- `Spark Structured Streaming` preserves technical fields such as `op` and `ts_ms` so downstream logic can distinguish create, update, and delete events.
- `MERGE INTO` on `Apache Iceberg` updates each record to its correct latest state by primary key.
- Micro-batch deduplication keeps the latest record by `ts_ms`, reducing errors when the same key changes multiple times in a short window.
- `dbt` separates staging, intermediate, and mart layers so transformations are controlled, readable, testable, and extensible.
- Iceberg snapshots allow previous versions to be queried for reconciliation or recovery after transformation issues.

### 2.3.2 Speed

The system is designed to move new HIS data into the analytics layer with low latency while keeping resource usage controlled.

- CDC continuously pushes changes into `Kafka`, avoiding full-table export cycles.
- Spark Streaming reads Kafka 24/7 and writes into Bronze in micro-batches.
- Near real-time dashboards use physical Gold tables updated incrementally by dbt.
- Airflow has a dedicated NRT DAG running every 3 minutes with `schedule_interval='*/3 * * * *'`.
- Superset reads directly from cleaned Gold tables through Spark Thrift Server and can auto-refresh every 1 to 5 minutes for operational dashboards.

### 2.3.3 Stability

The pipeline is clearly separated so each component has a specific responsibility, reducing operational risk on modest VPS infrastructure.

- Spark Streaming only handles continuous ingestion from Kafka into Iceberg and is not restarted by Airflow schedules.
- Airflow only orchestrates dbt and maintenance tasks, making workflows easier to observe and retry.
- `max_active_runs=1` prevents overlapping dbt runs.
- `catchup=False` prevents large replay batches after downtime, reducing the risk of RAM exhaustion or server congestion.
- Iceberg supports ACID transactions so dashboards do not read partially committed data.
- `rewrite_data_files`, `rewrite_manifests`, and `expire_snapshots` control small files, metadata growth, and storage usage.
- Long-running services such as Spark jobs, Spark Thrift Server, Airflow, and Superset can be managed with `systemd`, `tmux`, or `nohup`.

### 2.3.4 Security

The security design focuses on separating responsibilities, controlling access, and reducing exposure of healthcare data.

- Business data is stored in MinIO as Parquet/Iceberg files; the PostgreSQL catalog stores metadata only, not original healthcare data.
- Superset dashboards are role-based, for example executives, department heads, accounting, HR, and pharmacy.
- End users access data through dashboards or the SQL serving layer, not directly through the operational HIS database.
- MinIO keys, PostgreSQL credentials, and service accounts are separated from model logic so they can be managed through environment variables or dedicated configuration files.
- The lakehouse architecture separates analytical workloads from the source database, reducing the risk of heavy dashboards or queries affecting clinic operations.
- Iceberg snapshots and the metadata catalog support traceability and data auditability.

## 3. Implementation Details of the Data Lakehouse Model

### 3.1 Preparing Source Data and Business Documentation

The first step is preparing the `Core HIS` data foundation by organizing the clinic's core tables and identifying key entities such as patients, treatment episodes, services, receipts, medications, departments, staff, and corporate health-check contracts.

In parallel, the project documents business context through BPMN and DDL:

- `.bpmn` files describe workflows for reception, consultations, paraclinical services, billing, medication dispensing, and corporate health checks.
- DDL/DBML artifacts describe table structures, primary keys, foreign keys, and explanatory comments.

<p align="center">
  <img src="./readme_pic/bpmn.png" alt="BPMN"/>
  <br>
  <b>Figure 2:</b> Business process diagram example
</p>

These documents help downstream systems understand not only table structures, but also which operational step each table represents.

### 3.2 Synchronizing Core Data with Debezium and Kafka

Once the core data model is defined, the system uses `Debezium` and `Kafka Connect` to track changes in the source database. Debezium reads changes from core tables and publishes them into `Kafka` as CDC events, including inserts, updates, and deletes.

Kafka acts as the intermediate event log:

- It stores the stream of changes produced by Core DB.
- It decouples the source system from downstream processing jobs.
- It allows Spark to read events again through offsets and checkpoints when needed.
- It supports continuous synchronization instead of manual batch exports.

### 3.3 Ingesting Data into Bronze on MinIO and Iceberg

`Spark Structured Streaming` reads Kafka topics, parses CDC envelopes, and writes raw data into the Bronze layer on `MinIO` using `Apache Iceberg`.

The Bronze layer keeps data as close as possible to source state:

- Transactional HIS records.
- CDC technical fields such as `op` and `ts_ms`.
- The lakehouse ingestion timestamp.
- Checkpoints so Spark can continue safely after restart.

This design makes Bronze both the raw landing zone and a validation layer for reconciliation between source data, Kafka events, and transformed data.

<p align="center">
  <img src="./readme_pic/iceberg.png" alt="Iceberg files"/>
  <br>
  <b>Figure 3:</b> Parquet files in Iceberg format
</p>

### 3.4 Building Bronze, Silver, and Gold Layers

The project describes the data lakehouse from two viewpoints: the overall processing flow and the data layers inside the lakehouse.

- `Input`: transactional data generated from HIS, including patients, visits, services, billing, departments, staff, medication, and related business activity.
- `Processing`: data synchronization and processing components such as Debezium CDC, Kafka, Spark Structured Streaming, Apache Iceberg, dbt, and Airflow.
- `Output`: analytical tables, business marts, Superset dashboards, and SQL access through Spark Thrift Server.

Inside the lakehouse, data is organized into three layers to clearly separate raw data, intermediate processed data, and analytics-ready data:

- `Bronze`: stores raw CDC data from Kafka and preserves technical fields such as `op`, `ts_ms`, and ingestion time.
- `Silver`: cleans and enriches data from staging, filters invalid records, joins reference information, standardizes selected time/value fields, and creates intermediate business classifications.
- `Gold`: stores finalized analytical tables ready for dashboards, KPIs, and business reporting.

Apache Iceberg is central to the architecture because CDC data does not just need to be stored as files; it must be managed as tables with history, updates, deletes, version tracking, and optimized query access:

- `ACID transactions`: safe reads and writes, preventing dashboards from reading uncommitted states.
- `MERGE INTO`: supports CDC upserts and deletes.
- `Time travel`: queries data by snapshot or timestamp.
- `Schema evolution`: supports controlled schema changes as source systems evolve.
- `Metadata pruning`: Spark can skip irrelevant files and improve query speed.
- `Compaction`: merges small files to improve dashboard performance and reduce metadata overhead.

PostgreSQL in this project is not the business data warehouse. It acts as the Iceberg metadata catalog and stores:

- Table names and schemas.
- Table locations in MinIO.
- Current and historical snapshots.
- Manifests and query-planning metadata.

This separation keeps physical healthcare data in MinIO while table-management metadata stays in PostgreSQL. Spark, dbt, and Superset through Spark Thrift Server use the catalog to locate exact files instead of scanning the whole object store.

### 3.5 Transforming Data with dbt

`dbt` is configured to read data stored in MinIO/Iceberg through Spark, then turn raw data into business-meaningful models.

- `staging`: standardizes source tables and gives columns clearer names.
- `intermediate`: implements reusable business logic.
- `marts`: creates dashboard-serving tables by domain, such as clinical, finance, pharmacy, executive, operations, and core.
- `incremental model`: processes only new or changed data, making it suitable for NRT dashboards.
- `merge strategy`: updates physical Gold tables instead of depending on heavy, slow views.

<p align="center">
  <img src="./readme_pic/dbt_example.png" alt="dbt model example"/>
  <br>
  <b>Figure 4:</b> dbt model example
</p>

### 3.6 Serving Dashboards with Spark Thrift Server and Superset

Superset is the final presentation layer for business users. Dashboards are designed around several needs:

- `Near real-time dashboard`: monitors fast-changing activity such as visits, waiting patients, daily revenue, or hourly specialty density.
- `Management dashboard`: supports weekly, monthly, quarterly, and yearly reporting.
- `Department dashboard`: analyzes activity by department, room, service group, or staff dimension.
- `Executive dashboard`: presents high-level KPIs for leadership.
- `Pharmacy dashboard`: tracks prescriptions, dispensing, and pharmacy operations.
- `Finance dashboard`: analyzes revenue, payments, service composition, and time-based comparisons.

<p align="center">
  <img src="./readme_pic/superset_visual.png" alt="Superset dashboard"/>
  <br>
  <b>Figure 5:</b> Superset dashboard example
</p>

NRT dashboard flow:

1. Spark Streaming continuously writes CDC data into Bronze.
2. dbt incremental models update Gold on a short cycle.
3. Airflow runs the NRT DAG every 3 minutes.
4. Superset reads standardized Gold tables and refreshes as needed.

### 3.7 Orchestration and Operations with Airflow

Airflow orchestrates jobs with clear starts and ends, especially dbt and maintenance tasks.

<p align="center">
  <img src="./readme_pic/air_flow.png" alt="Airflow DAG"/>
  <br>
  <b>Figure 6:</b> Airflow DAG
</p>

Important operational settings:

- `schedule_interval='*/3 * * * *'` for the near real-time workflow.
- `max_active_runs=1` to prevent overlapping runs.
- `catchup=False` to avoid bulk replay after downtime.
- Separate NRT DAGs from batch DAGs to reduce cross-impact.

Continuous streaming writes can create many small files and metadata entries, so the system requires scheduled maintenance:

- `rewrite_data_files`: compact small files into larger files.
- `rewrite_manifests`: reduce metadata overhead during query planning.
- `expire_snapshots`: remove old snapshots to control storage usage.
- Monitor inode usage, disk capacity, and metadata size.
- Tune Superset timeouts, cache windows, and refresh intervals to avoid unnecessary load.

The project is designed to run on modest Linux VPS infrastructure.

- MinIO can be installed natively to reduce overhead compared with Docker.
- Spark Streaming can run long term with `tmux`, `nohup`, or `systemd`.
- Spark Thrift Server runs as a separate service for SQL serving.
- Airflow can use `systemd` to auto-restart the scheduler and webserver.
- Superset should use a dedicated metadata database such as PostgreSQL instead of SQLite for stable operation.

## 4. Skills Gained

### 4.1 Technical Skills

- Designing an end-to-end lakehouse architecture for hospital data.
- Building CDC pipelines with Debezium, Kafka, and Spark Structured Streaming.
- Writing CDC data into Apache Iceberg with upsert/delete logic.
- Organizing data into Bronze, Silver, and Gold layers.
- Building dbt models for staging, intermediate, and mart layers.
- Serving analytics data through Spark Thrift Server and Superset.
- Optimizing operations around small files, metadata, and snapshot retention.

### 4.2 Tool Skills

- Debezium connectors and Kafka topics.
- Spark Structured Streaming and Spark SQL.
- MinIO object storage.
- Apache Iceberg and PostgreSQL JDBC Catalog.
- dbt models, incremental processing, and merge strategy.
- Airflow DAGs, schedules, and retry control.
- Superset dashboards, datasets, charts, and role-based access.
- Linux service operation with `systemd`, `tmux`, `nohup`, cron, and swap.

### 4.3 Domain Knowledge

- Understanding how clinic and hospital data is generated from operational workflows.
- Identifying key entities such as patients, visits, services, departments, staff, invoices, and prescriptions.
- Distinguishing operational transaction data from standardized analytical data.
- Designing dashboards by user role instead of a single generic dashboard.
- Balancing fast refresh requirements with the stability constraints of healthcare systems.

## 5. Future Directions

- Add dbt data tests for primary keys, nulls, relationships, and abnormal values.
- Further standardize dashboard authorization by real user groups.
- Expand monitoring for Kafka lag, Spark Streaming status, Airflow run status, and Superset query latency.
- Optimize Iceberg snapshot retention policies based on audit requirements.
- Develop machine learning dashboards or an AI assistant on top of standardized Gold data.

## Summary

This project is a practical healthcare data lakehouse platform combining CDC, streaming, Iceberg table format, dbt transformation, Airflow orchestration, and Superset dashboards.

The design's main value is concentrated in four areas:

- **Accuracy**: CDC, primary-key merges, deduplication, dbt modeling, and Iceberg snapshots.
- **Speed**: streaming ingestion, incremental transformation, and 3-minute near real-time dashboards.
- **Stability**: clear layer separation, Airflow guardrails, ACID transactions, and scheduled maintenance.
- **Security**: separation between HIS and analytics, role-based Superset access, metadata isolation, and reduced direct access to source data.
