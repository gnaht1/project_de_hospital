# Hospital Data Lakehouse Platform

*You can access the Vietnamese version [here](./readme_vn.md)*.

Real-time CDC ingestion, Iceberg-based lakehouse storage, near real-time transformation with dbt, and BI dashboards served through Spark Thrift Server and Apache Superset.

## Table of Contents

- [1. Context](#1-context)
- [2. Implementation](#2-implementation)
  - [Description of Implementation Steps](#description-of-implementation-steps)
    - [Input/Output Overview](#inputoutput-overview)
    - [2.1 Project Overview](#21-project-overview)
    - [2.2 Architecture Overview](#22-architecture-overview)
    - [2.3 End-to-End Data Flow](#23-end-to-end-data-flow)
    - [2.4 Ingestion Layer](#24-ingestion-layer)
      - [2.4.1 Core HIS as the Source System](#241-core-his-as-the-source-system)
      - [2.4.2 Debezium CDC Captures Database Changes](#242-debezium-cdc-captures-database-changes)
      - [2.4.3 Kafka Receives CDC Events](#243-kafka-receives-cdc-events)
      - [2.4.4 Spark Structured Streaming Reads from Kafka](#244-spark-structured-streaming-reads-from-kafka)
    - [2.5 Storage and Lakehouse Layer](#25-storage-and-lakehouse-layer)
      - [2.5.1 Spark Writes into the Iceberg Storage Layer](#251-spark-writes-into-the-iceberg-storage-layer)
      - [2.5.2 Bronze, Silver, and Gold Define the Storage Stages](#252-bronze-silver-and-gold-define-the-storage-stages)
      - [2.5.3 Why Apache Iceberg](#253-why-apache-iceberg)
      - [2.5.4 MinIO Stores the Physical Data Files](#254-minio-stores-the-physical-data-files)
      - [2.5.5 PostgreSQL Manages Iceberg Metadata](#255-postgresql-manages-iceberg-metadata)
    - [2.6 Data Transformation with dbt](#26-data-transformation-with-dbt)
    - [2.7 Dashboards and Analytics Use Cases](#27-dashboards-and-analytics-use-cases)
    - [2.8 Query Serving and BI](#28-query-serving-and-bi)
      - [2.8.1 Spark Thrift Server Exposes the Query Layer](#281-spark-thrift-server-exposes-the-query-layer)
      - [2.8.2 Superset Delivers the Visualization Layer](#282-superset-delivers-the-visualization-layer)
    - [2.9 Orchestration and Maintenance](#29-orchestration-and-maintenance)
    - [2.10 Deployment Notes](#210-deployment-notes)
- [3. Skills and Achievements After Completing the Project](#3-skills-and-achievements-after-completing-the-project)
  - [3.1 Technical Skills](#31-technical-skills)
  - [3.2 Tool Skills](#32-tool-skills)
  - [3.3 Domain Knowledge](#33-domain-knowledge)
  - [3.4 Business Process and Data Documentation](#34-business-process-and-data-documentation)
- [4. Future Directions](#4-future-directions)
- [Summary](#summary)

## 1. Context

This project is designed for a clinic or hospital environment where operational systems generate continuous transactional data from patient registration, admissions, consultations, billing, laboratory services, and other care-related workflows.

In this kind of healthcare setting, data is usually stored first inside core operational applications such as a `Hospital Information System (HIS)`. This system are optimized for daily operations, historical analytics, cross-department reporting, or near real-time monitoring.

The platform in this repository provides a way to move that operational data into a centralized lakehouse so the clinic can support:

- near real-time monitoring of patient and service activity
- management reporting across departments and time periods
- trusted historical analysis for audits and operational review
- future data science, machine learning, or AI use cases on top of curated healthcare data

The overall goal is to give the organization a scalable analytics foundation without interrupting the source systems that staff use for daily clinical and administrative work.

## 2. Implementation
<p align="center">
  <img src="./readme_pic/de_flowchart.jpg" alt="Flow Chart"/>
  <b>Figure 1:</b> Flow chart <br>
</p>

### Description of Implementation Steps

#### Input/Output Overview

- `Input`: transactional data from the clinic's core `HIS`, including operational records such as registration, visits, services, billing, and other day-to-day healthcare activities.
  - Link **data discription** : [Link](https://docs.google.com/spreadsheets/d/1eGdW56kQfhWlkBmLUB7z0Jgh5vEu-bKBL0QVoc3e2bI/edit?usp=sharing)
- `Processing`: CDC capture with `Debezium`, event streaming through `Kafka`, ingestion with `Spark Structured Streaming`, storage in `Iceberg` on `MinIO`, transformation with `dbt`, and orchestration with `Airflow` and `Crontab`.
- `Output`: curated `Bronze`, `Silver`, and `Gold` datasets, SQL-accessible analytics tables through `Spark Thrift Server`, and role-based dashboards in `Superset` for different user groups.

The implementation follows the flowchart as a layered pipeline, starting from the operational clinic system, moving through streaming ingestion and lakehouse storage, and ending in transformation, SQL serving, visualization, and maintenance.

#### 2.1 Project Overview

This project is a hospital data engineering platform built with a lakehouse architecture. Its goal is to move data from operational hospital systems into an analytics-ready environment that supports near real-time operational dashboards, dimensional and fact modeling for reporting, reliable historical tracking through Iceberg snapshots, and future machine learning or AI use cases.

The practical pipeline is:

`Core HIS -> Debezium -> Kafka -> Spark Structured Streaming -> Iceberg on MinIO -> dbt -> Spark Thrift Server -> Superset`

#### 2.2 Architecture Overview

The system separates responsibilities across ingestion, storage, transformation, orchestration, and visualization:

- `Debezium` captures CDC events from the hospital source database.
- `Kafka` transports table changes as streaming events.
- `Spark Structured Streaming` consumes Kafka continuously and lands raw data into Iceberg tables.
- `MinIO` stores the physical data files.
- `Apache Iceberg` provides the table abstraction, snapshots, and ACID behavior.
- `PostgreSQL` acts as the Iceberg JDBC catalog and stores metadata only.
- `dbt` transforms raw tables into curated staging, dimension, and fact models.
- `Airflow` and `Crontab` schedule transformation and maintenance workloads.
- `Spark Thrift Server` exposes the lakehouse through SQL.
- `Superset` provides dashboards and analytics for end users.

#### 2.3 End-to-End Data Flow

1. Changes in the hospital core system are captured through CDC.
2. **Debezium** publishes those changes into Kafka topics.
3. **Spark Structured Streaming** runs continuously and reads from Kafka.
4. **Spark** parses CDC envelopes, keeps technical columns such as `op` and `ts_ms`, and merges records into raw Iceberg tables on MinIO.
5. **PostgreSQL** tracks Iceberg metadata such as table locations, manifests, and snapshots.
6. **dbt** builds curated staging and mart layers, including dimension and fact tables.
7. **Spark Thrift Server** exposes curated data for SQL access.
8. **Superset** queries the curated tables for dashboards and reporting.
9. **Airflow** and **Crontab** schedule refresh, orchestration, and maintenance tasks.

#### 2.4 Ingestion Layer

##### 2.4.1 Core HIS as the Source System

The process starts from the clinic's core `HIS`, which stores transactional data generated by daily activities such as registration, visits, services, billing, and clinical operations.

##### 2.4.2 Debezium CDC Captures Database Changes

`Debezium` listens to the source database and captures inserts, updates, and deletes as change events, allowing data to move continuously without repeated full-table extraction.

##### 2.4.3 Kafka Receives CDC Events

The captured changes are pushed into `Kafka` topics, which act as the event streaming layer between the source system and downstream processing.

##### 2.4.4 Spark Structured Streaming Reads from Kafka

The streaming job runs continuously, consumes CDC events from Kafka, parses the message structure, preserves technical CDC fields, and prepares records for the raw lakehouse layer.

Spark is the chosen streaming engine for this project. Based on the project decision, Spark Structured Streaming already fulfills the streaming requirement, so Flink is not required unless it becomes a mandatory external requirement.

#### 2.5 Storage and Lakehouse Layer

##### 2.5.1 Spark Writes into the Iceberg Storage Layer

After reading the events, Spark writes the data into the `Iceberg` table layer, which is organized into `Bronze`, `Silver`, and `Gold` zones.

##### 2.5.2 Bronze, Silver, and Gold Define the Storage Stages

- `Bronze`: stores raw ingested CDC data.
- `Silver`: stores cleaned, standardized, and partially refined data.
- `Gold`: stores curated business-ready datasets for analytics, reporting, and dashboard serving.

<p align="center">
  <img src="./readme_pic/iceberg.png" alt="iceberg"/>
  <b>Figure 2:</b> Parquet files in Iceberg format<br>
</p>

##### 2.5.3 Why Apache Iceberg?

Apache Iceberg is a central design choice because plain CSV, JSON, or text files do not scale well for CDC-driven analytics.

Key benefits include:

- `ACID transactions`: safer updates, deletes, and merges
- `Time travel`: query previous snapshots using timestamp or snapshot ID
- `Schema evolution`: add or modify columns without rewriting all historical data
- `Metadata pruning`: Spark can skip irrelevant files instead of scanning the entire lake
- `Small-file management`: supports compaction to recover performance

In the CDC pipeline, insert events create rows, update events generate newer snapshots, and delete events are handled through merge logic. By default, normal queries read the latest snapshot while older versions remain available until snapshot retention removes them.

##### 2.5.4 MinIO Stores the Physical Data Files

The storage layer is backed by `MinIO` object storage, where the underlying `Parquet` files for Iceberg tables are kept.

##### 2.5.5 PostgreSQL Manages Iceberg Metadata

A separate `PostgreSQL Catalog` stores metadata for the Iceberg tables, including table definitions, manifests, and snapshot references, while the actual business data remains in object storage.

This metadata layer is critical for:

- fast query planning
- snapshot tracking
- time travel
- safe concurrent reads and writes

#### 2.6 Data Transformation with dbt

`dbt` works on top of the Bronze, Silver, and Gold layers to perform transformation and aggregation logic, shaping raw streaming data into analytics-ready models.

The modeling approach includes:

- `staging` models to clean and standardize raw source tables
- `marts` models to produce dimension and fact tables for reporting
- physical Iceberg-backed models instead of temporary query-only logic

<p align="center">
  <img src="./readme_pic/dbt_example.png" alt="dbt_example"/>
  <b>Figure 3:</b> Dbt model example<br>
</p>

For near real-time use cases, dbt uses incremental models with merge logic so only newly arrived or changed records are processed.

#### 2.7 Dashboards and Analytics Use Cases

This platform is designed to support multiple dashboard types, each with a different refresh pattern and business purpose.

Dashboard categories include:

- `Near real-time operational dashboards`: monitor fast-changing activity such as admissions, patient flow, or service events.
- `Periodic management dashboards`: support weekly, monthly, quarterly, and yearly reporting.
- `Department or service dashboards`: break down activity by medical unit, service group, or staff-related dimensions.
- `Executive summary dashboards`: present high-level KPIs for hospital leadership.

For the near real-time dashboard flow:

1. `Spark Structured Streaming` keeps ingesting CDC data continuously into the Bronze layer.
2. `dbt` builds the Gold layer using physical incremental tables.
3. `Airflow` runs a dedicated NRT workflow every 3 minutes.
4. `Superset` reads directly from the cleaned Gold tables for dashboard consumption.

<!-- Important NRT implementation details include:

- keep `op` and `ts_ms` for CDC-aware downstream logic
- use a `1 minute` Spark trigger to keep raw data fresh
- use `materialized='incremental'` with `incremental_strategy='merge'`
- filter only records with `ts_ms` greater than the latest timestamp already present
- deduplicate with `ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC)`
- exclude deleted records with `op != 'd'` when building serving tables -->

Batch and curated dashboards can use longer refresh intervals and cache windows to reduce load on Spark Thrift Server.

#### 2.8 Query Serving and BI

##### 2.8.1 Spark Thrift Server Exposes the Query Layer

Once curated tables are available, `Spark SQL Thrift Server` provides a SQL-based query engine so BI tools can access the data in a standard way.

##### 2.8.2 Superset Delivers the Visualization Layer

`Superset` connects to the query engine and provides dashboards, charts, and reporting views for end users.

In this project, dashboard access is controlled by user roles so each group can only view the dashboards relevant to its responsibility, such as `Board of Directors`, `Head of Department Physicians`, `HR`, `Accounting`, and `Pharmacy Inventory Staff`, etc.

<p align="center">
  <img src="./readme_pic/superset_visual.png" alt="superset_visual"/>
  <b>Figure 4:</b> Superset dashboard example<br>
</p>

This design is especially useful for reporting scenarios such as admission trends, service utilization, patient activity monitoring, and periodic management reports by day, week, month, quarter, or year.

#### 2.9 Orchestration and Maintenance

`Airflow` and `Crontab` are responsible for scheduling transformation jobs, refresh cycles, and operational maintenance tasks that keep the platform running reliably.

Key orchestration safeguards include:

- `schedule_interval='*/3 * * * *'`
- `max_active_runs=1`
- `catchup=False`

This prevents overlapping runs, avoids replay storms after downtime, and keeps the server stable.

Maintenance is also required because near real-time ingestion creates many small files. Important tasks include:

- `rewrite_data_files` to compact small files into larger ones
- `rewrite_manifests` to reduce metadata overhead
- `expire_snapshots` to remove old history and control storage growth
- operational fixes such as resolving full inode errors when needed

<p align="center">
  <img src="./readme_pic/air_flow.png" alt="DAG airflow"/>
  <b>Figure 5:</b> DAG airflow <br>
</p>

#### 2.10 Deployment Notes

The project is designed to run on modest Linux VPS infrastructure.

Infrastructure patterns include:

- `MinIO` installed natively to reduce memory overhead compared with Docker
- long-running Spark jobs managed with `tmux`, `nohup`, or `systemd`
- `Airflow` deployed with swap enabled where necessary on low-memory machines
- `Airflow` services managed through `systemd` for auto-start and restart behavior
- `Superset` deployed separately and connected to Spark Thrift Server

Operational concerns include continuous Spark streaming generating many small files, fast-growing Iceberg metadata if snapshots are never expired, query timeout tuning in Superset, and PostgreSQL catalog performance affecting query planning.

<!-- #### 2.11 Current Progress

Based on the project notes, the implementation status is:

1. `Core HIS -> Debezium -> Kafka`: completed
2. `Kafka -> Spark Structured Streaming -> Iceberg raw data on MinIO`: completed
3. `Airflow` orchestration for dbt: implemented as the next operational layer
4. `Near real-time dashboard flow`: defined and optimized around a 3-minute cycle
5. `Superset` dashboards on top of curated data: final serving layer -->

## 3. Skills and Achievements After Completing the Project

This project delivers not only a working healthcare analytics platform, but also a clear set of skills and practical achievements gained through design, implementation, deployment, and dashboard delivery.

### 3.1 Technical Skills

Completing this project strengthens core technical skills in modern data engineering, especially:

- designing an end-to-end CDC-based lakehouse architecture
- building streaming data pipelines with `Debezium`, `Kafka`, and `Spark Structured Streaming`
- modeling layered data platforms using `Bronze`, `Silver`, and `Gold` architecture
- working with `Apache Iceberg` for ACID tables, snapshot management, schema evolution, and time travel
- implementing near real-time transformation logic with incremental processing and merge strategies
- exposing analytics-ready data through SQL serving layers for BI consumption

### 3.2 Tool Skills

The project also develops hands-on ability with the main tools used across the platform, including:

- configuring `Debezium` connectors for CDC ingestion
- managing Kafka topics and understanding event-driven data movement
- developing Spark jobs for continuous ingestion and table writes
- using `MinIO` as object storage for lakehouse data files
- maintaining the `PostgreSQL` Iceberg catalog for metadata management
- building transformation models in `dbt`
- scheduling workflows and maintenance jobs with `Airflow` and `Crontab`
- serving data through `Spark Thrift Server`
- creating dashboards and role-based access control in `Superset`
- operating the platform in Linux environments with tools such as `systemd`, `tmux`, and `nohup`

### 3.3 Domain Knowledge

Beyond technical implementation, this project builds domain knowledge in healthcare analytics and operational reporting, such as:

- understanding how hospital or clinic source systems generate transactional data
- identifying important healthcare entities such as patients, visits, services, billing records, departments, and service units
- understanding the difference between operational data capture and analytics-ready reporting data
- designing dashboards for different organizational roles such as the `Board of Directors`, `Head of Department Physicians`, `HR`, `Accounting`, and `Pharmacy Inventory Staff`
- balancing near real-time monitoring needs with periodic management reporting requirements
- recognizing operational constraints in healthcare environments, where source systems must remain stable while analytics workloads run in parallel

### 3.4 Business Process and Data Documentation

In addition to the data platform implementation, the project also includes supporting analysis and documentation artifacts.

These documents help connect the technical pipeline to the real business context:

- `.bpmn` files describe operational workflows and business processes in the hospital or clinic environment

<p align="center">
  <img src="./readme_pic/bpmn.png" alt="bpmn"
  <b>Figure 6:</b> Business process diagram example <br>
</p>

- `.dbml` files describe the database structure, table relationships, and business entities used in the source systems

This documentation is important because it helps explain how operational activities are translated into source data, how tables relate to actual business processes, and how the downstream analytics models should be designed.

## 4. Future Directions

Potential next steps already mentioned in the project planning notes include:

- machine learning dashboards built on Spark-readable curated data
- AI or chatbot workflows for treatment history lookup and decision support
- richer hospital management dashboards for executive reporting

## Summary

This project demonstrates a practical hospital lakehouse architecture that combines streaming ingestion, transactional table storage, scheduled transformations, and near real-time dashboards on constrained infrastructure.

The most important engineering decisions in the project are:

- keeping Spark Structured Streaming as the core ingestion engine
- using Iceberg on MinIO instead of plain raw files
- using PostgreSQL as a metadata catalog rather than a business-data warehouse
- using dbt plus Airflow to bridge raw CDC data into curated NRT and reporting models
