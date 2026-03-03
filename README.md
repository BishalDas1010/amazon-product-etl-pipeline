# Amazon Product ETL Pipeline

A production-style data engineering pipeline built with **Apache Spark (PySpark)** that processes Amazon product data through the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Pipeline Layers](#pipeline-layers)
  - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
  - [Silver Layer — Cleaning & Transformation](#silver-layer--cleaning--transformation)
  - [Gold Layer — Business Aggregations](#gold-layer--business-aggregations)
- [Dataset](#dataset)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Output](#output)
- [Spark Configuration](#spark-configuration)

---

## Project Overview

This project implements a full **ETL (Extract, Transform, Load)** pipeline for Amazon product catalog data. Raw CSV data is ingested, cleaned, and aggregated through three progressive data quality layers, producing analytics-ready Parquet files.

---

## Problems This Project Solves

### 1. Raw Data is Unreliable
Real-world product catalogs contain missing values, duplicate entries, inconsistent formatting, and wrong data types. This pipeline solves that by enforcing a dedicated cleaning layer (Silver) that validates and standardizes every record before it reaches analytics.

### 2. No Data Lineage or Auditability
When something breaks in a pipeline, it is hard to trace where bad data came from. The Bronze layer preserves every raw record with `ingestion_timestamp` and `source_file` metadata, making it possible to audit exactly when and where each record entered the system.

### 3. Reprocessing is Risky Without Raw Backups
If transformations are applied directly to source data, mistakes are irreversible. The Medallion Architecture separates raw storage (Bronze) from transformations (Silver/Gold), so you can always reprocess from scratch without touching the original data.

### 4. Analytics Queries Are Slow on Raw Data
Running business queries directly on raw CSV files is slow and expensive. This pipeline produces compressed **Parquet** outputs at each layer — columnar storage that is significantly faster for analytical queries than row-based CSV.

### 5. No Structured Business Metrics
Raw product data does not directly answer business questions like "Which product types dominate the catalog?" or "What are the longest/largest products?". The Gold layer solves this by producing pre-aggregated tables (`type_summary`, `top_by_length`) ready for dashboards or reporting tools.

### 6. Scalability on Large Datasets
Processing large CSV files in-memory with Pandas fails at scale. By using **Apache Spark**, this pipeline distributes computation across cores and can scale from thousands to millions of product records with configuration changes only.

### 7. Inconsistent Data Types Across Systems
Product length and type IDs often arrive as strings from source systems. The Silver layer safely casts them to `FLOAT` and `INT` using `try_cast`, ensuring downstream consumers always receive correctly typed data without crashes.

---

## Architecture

```
CSV Source
    │
    ▼
┌──────────────┐
│ BRONZE LAYER │  ← Raw ingestion with metadata (Parquet)
└──────────────┘
    │
    ▼
┌──────────────┐
│ SILVER LAYER │  ← Cleaned, deduplicated, type-cast (Parquet)
└──────────────┘
    │
    ▼
┌─────────────┐
│  GOLD LAYER │  ← Business aggregations (Parquet)
└─────────────┘
    │
    ├── top_by_length/    ← Top products ranked by PRODUCT_LENGTH
    └── type_summary/     ← Product count & avg length per PRODUCT_TYPE_ID
```

---

## Project Structure

```
amazon-product-etl-pipeline/
│
├── Src/
│   ├── main.py                   # Pipeline orchestrator (entry point)
│   ├── spark_session.py          # Spark session factory with tuned configs
│   ├── extract.py                # Bronze layer: raw CSV → Parquet
│   ├── transform.py              # Silver layer: cleaning & transformation
│   ├── load.py                   # Gold layer: business aggregations
│   └── medallionArchitecture.md  # Architecture documentation
│
├── db/
│   ├── train.csv                 # Source dataset (Amazon product catalog)
│   └── pg_connection.py          # PostgreSQL connection (reserved for future use)
│
├── output/
│   ├── Bronze/                   # Raw Parquet output
│   ├── silver/                   # Cleaned Parquet output
│   └── Gold/
│       ├── top_by_length/        # Products ranked by PRODUCT_LENGTH
│       └── type_summary/         # Aggregated summary by product type
│
├── requirments.txt               # Python dependencies
└── README.md
```

---

## Tech Stack

| Component         | Technology               |
|-------------------|--------------------------|
| Processing Engine | Apache Spark (PySpark)   |
| Language          | Python 3.12              |
| Storage Format    | Apache Parquet (Snappy)  |
| Environment       | Python virtualenv         |
| Database (future) | PostgreSQL (psycopg2)    |

---

## Pipeline Layers

### Bronze Layer — Raw Ingestion

**File:** `Src/extract.py`

- Reads raw CSV from `db/train.csv` using Spark with `inferSchema=True`
- Adds three metadata columns for data lineage:
  - `ingestion_timestamp` — when the record was ingested
  - `source_file` — the origin file path
  - `layer` — tagged as `"bronze"`
- Writes output in **Parquet (Snappy compressed)** format to `output/Bronze/`

```python
bronze_df = raw_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("layer", lit("bronze"))
```

---

### Silver Layer — Cleaning & Transformation

**File:** `Src/transform.py`

Reads Bronze Parquet and applies the following transformations:

| Operation           | Detail                                                         |
|---------------------|----------------------------------------------------------------|
| **Deduplication**   | Removes duplicate rows by `PRODUCT_ID`                         |
| **Null Handling**   | Drops rows missing `PRODUCT_ID` or `TITLE`                     |
| **Text Trimming**   | Strips whitespace from `TITLE`, `DESCRIPTION`, `BULLET_POINTS` |
| **Type Casting**    | `PRODUCT_TYPE_ID` → INT, `PRODUCT_LENGTH` → FLOAT              |
| **Null Imputation** | Fills NULL numeric fields with `0` / `0.0`                     |
| **Metadata Cleanup**| Drops `ingestion_timestamp`, `source_file`, `layer`            |

Output is cached before writing to avoid double computation, then persisted to `output/silver/`.

---

### Gold Layer — Business Aggregations

**File:** `Src/load.py`

Reads Silver Parquet and produces two business-ready aggregation tables:

#### `top_by_length`
Products ranked by `PRODUCT_LENGTH` in descending order — useful for identifying physically large products in the catalog.

```
output/Gold/top_by_length/
```

#### `type_summary`
Aggregated metrics grouped by `PRODUCT_TYPE_ID`:
- Count of products per type
- Average `PRODUCT_LENGTH` per type

```
output/Gold/type_summary/
```

Both outputs are written as **Parquet** files.

---

## Dataset

**File:** `db/train.csv`

Amazon product catalog data containing:

| Column            | Type    | Description                       |
|-------------------|---------|-----------------------------------|
| `PRODUCT_ID`      | String  | Unique product identifier         |
| `TITLE`           | String  | Product title                     |
| `BULLET_POINTS`   | String  | Product feature bullet points     |
| `DESCRIPTION`     | String  | Full product description          |
| `PRODUCT_TYPE_ID` | Integer | Product category/type identifier  |
| `PRODUCT_LENGTH`  | Float   | Physical length of the product    |

---

## Setup & Installation

### Prerequisites

- Python 3.12
- Java 8 or 11 (required by Apache Spark)

### Steps

```bash
# Clone the repository
git clone <repo-url>
cd amazon-product-etl-pipeline

# Create and activate virtual environment
python3 -m venv myenv
source myenv/bin/activate

# Install dependencies
pip install -r requirments.txt
```

---

## Running the Pipeline

```bash
# Activate the virtual environment
source myenv/bin/activate

# Run from the Src directory
cd Src
python main.py
```

Expected console output:

```
============================================================
Amazon Product ETL Pipeline - Medallion Architecture
============================================================

 BRONZE LAYER: Raw Data Ingestion
----------------------------------------
Bronze ingested to <N> rows to output/Bronze

SILVER LAYER: Data Cleaning & Transformation
----------------------------------------
[SILVER]  Cleaned <N> rows to output/silver

 GOLD LAYER: Business Aggregations
----------------------------------------

============================================================
Pipeline completed successfully!
============================================================
```

---

## Output

After a successful run, the following directories are populated:

```
output/
├── Bronze/
│   └── _SUCCESS                 ← Raw data as Parquet
├── silver/
│   └── _SUCCESS                 ← Cleaned data as Parquet
└── Gold/
    ├── top_by_length/
    │   └── _SUCCESS             ← Products ranked by PRODUCT_LENGTH
    └── type_summary/
        └── _SUCCESS             ← Count & avg length per product type
```

To inspect results interactively:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Top by Length")
spark.read.parquet("output/Gold/top_by_length").show(5, truncate=False)

print("Type Summary")
spark.read.parquet("output/Gold/type_summary").show(5, truncate=False)
```

---

## Spark Configuration

The Spark session is tuned for local execution on memory-constrained machines:

| Configuration                         | Value        | Reason                                              |
|---------------------------------------|--------------|-----------------------------------------------------|
| `spark.master`                        | `local[2]`   | Limits to 2 cores to reduce memory pressure         |
| `spark.driver.memory`                 | `4g`         | Handles large text columns (DESCRIPTION, BULLET_POINTS) |
| `spark.executor.memory`               | `4g`         | Sufficient for Silver/Gold transformations          |
| `spark.sql.shuffle.partitions`        | `4`          | Reduced from default 200 for single-node mode       |
| `spark.sql.parquet.compression.codec` | `snappy`     | Fast, lightweight Parquet compression               |
| `spark.local.dir`                     | `/tmp/spark_local` | Spill directory with adequate free disk space |
| `spark.driver.maxResultSize`          | `2g`         | Prevents OOM on collect/count operations            |
| `spark.network.timeout`               | `800s`       | Avoids task timeout during GC pressure              |
| `spark.sql.files.maxPartitionBytes`   | `64MB`       | Reduces per-task memory by capping partition size   |