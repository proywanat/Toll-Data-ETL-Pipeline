# ETL Toll Data Pipeline using Apache Airflow

## Overview
This project implements an automated ETL pipeline for toll data using Apache Airflow. The pipeline extracts data from multiple file formats, transforms the data into a standardized structure, and prepares it for downstream analytics or storage. Airflow is used to orchestrate the workflow, manage task dependencies, and ensure reliable execution with retry and email alerts.

## Objectives
- Automate the daily processing of toll-related datasets.
- Extract and standardize data from CSV, TSV, and fixed-width text files.
- Consolidate the extracted datasets into a unified file.
- Perform basic transformations to prepare the data for analysis.
- Demonstrate Airflow orchestration using BashOperator tasks.

## Data Sources
The input data is packaged in a `.tgz` archive and includes:
- **CSV**: `vehicle-data.csv`
- **TSV**: `tollplaza-data.tsv`
- **Fixed-width text**: `payment-data.txt`

These datasets represent vehicle information, toll plaza details, and payment records.

## Pipeline Architecture
The Airflow DAG consists of six tasks:

### 1. `unzip_data`
Extracts the compressed archive `tolldata.tgz` into the working directory.

### 2. `extract_data_from_csv`
Uses `cut` to extract columns 1–4 from `vehicle-data.csv`.

### 3. `extract_data_from_tsv`
Extracts fields 5–7 from `tollplaza-data.tsv`.

### 4. `extract_data_from_fixed_width`
Extracts characters 59–68 from `payment-data.txt`.

### 5. `consolidate_data`
Combines all extracted datasets using the `paste` command, producing `extracted_data.csv`.

### 6. `transform_data`
Applies a basic transformation that converts all text to uppercase.

## DAG Scheduling
- **Schedule:** Daily (`timedelta(days=1)`)
- **Retry:** 1 retry with 5-minute delay
- **Alerting:** Email notifications on failure or retries

## Project Features
- Fully automated ETL workflow managed by Airflow.
- Practical use of BashOperator for file processing.
- Clear task dependency chain.
- Easy extensibility for data validation, loading, or enrichment.
- Suitable foundation for building production-grade data pipelines.
