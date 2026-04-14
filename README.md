# Casino Big Data Pipeline
### End-to-End Big Data Exploitation Solution
Gabriela Carolina Velasco Floriano — 0252748  
**Subject:** Big Data  
**Due Date:** April 14th, 2026

---

## Project Overview

This project implements a complete big data pipeline for analyzing 1.2 million online casino game records. The pipeline covers data ingestion, storage (PostgreSQL  and Parquet data lake), batch processing with Apache Spark, incremental streaming simulation, analytical modeling, visualization with Power BI, and K-Means clustering.

---

## Project Structure

```
casino_bigdata/
│
├── data/
│   ├── raw/
│   │   ├── input/
│   │   │   └── online_casino_games_dataset_v2.csv   ← original dataset
│   │   ├── logs/
│   │   └── ingestion_log.json
│   ├── processed/        ← Parquet files (Spark output)
│   ├── curated/          ← Parquet aggregations (Spark output)
│   │   ├── by_provider/
│   │   ├── by_game_type/
│   │   ├── by_volatility/
│   │   ├── by_rtp_category/
│   │   └── by_bet_tier/
│   ├── streaming/
│   │   ├── input/        
│   │   └── output/    
│   └── exports/         
│
├── ingestion/
│   ├── ingest_raw_data.py
│   └── setup_database.py
│
├── processing/
│   └── spark_processing.py
│
├── streaming/
│   └── streaming_simulation.py
│
├── analytics/
│   ├── analytical_model.py
│   ├── export_for_powerbi.py
│   └── ai_clustering.py
│
├── docker-compose.yml
└── README.md
```

---

## Prerequisites

### 1. Required Software

| Software |
| Python | Java JDK |  Apache Spark | Hadoop winutils | Docker Desktop | Power BI Desktop 

### 2. Environment Variables (Windows)

After installing Java and Spark,  we need to set these system environment variables:

```
JAVA_HOME   = C:\Program Files\Eclipse Adoptium\jdk-17...
SPARK_HOME  = C:\spark
HADOOP_HOME = C:\hadoop
PATH        += %JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin
```

> Download `winutils.exe` and `hadoop.dll`, and place both inside `C:\hadoop\bin\`

### 3. Python Dependencies

Now we go to the PowerShell and run:
pip install pyspark pandas psycopg2-binary pyarrow scikit-learn matplotlib


## Step-by-Step Execution

---

### PHASE 1 — Start the Database (Docker)

Docker must be running in the project root for this to work, in this case:

```powershell
cd C:\Users\caro_\OneDrive\Desktop\UP\1OCTAVO\datosMasivos\casino_bigdata
docker-compose up -d
```
just use the 'docker-compose up-d' commmand  and verify with 'docker ps'

> **Expected output:**
> ```
> CONTAINER ID   IMAGE              PORTS
> xxxxxxxxxxxx   nameOftheimage    0.0.0.0:5433->5432/tcp
> ```

---

### PHASE 2 — Data Ingestion
Using the python files from this porject, use the following commands in the order below.


We need to first read the original CSV, validate it, and generate an ingestion log.
```powershell
python ingestion/ingest_raw_data.py online_casino_games_dataset_v2.csv
```

**Expected output:**
```
Rows         : 1,200,000
Size         : 240.56 MB
Duplicates   : 0
Nulls        : 1,285,357
Log saved    : data/raw/ingestion_log.json
```

**Verify:** Check that `data/raw/ingestion_log.json` was created.

---

### PHASE 3 — Load Data into PostgreSQL

In this step we creare the 'casino_games_raw' table and loads all 1.2 million rows at once.

```powershell
python ingestion/setup_database.py
```

**Expected output:**
```
Connecting to PostgreSQL...
Connected
Table 'casino_games_raw' ready
Reading CSV...
1,200,000 rows loaded into PostgreSQL
Done. Table: casino_db → casino_games_raw
```

**Verify row count inside Docker:**

```powershell
docker exec -it casino_postgres psql -U postgres -d casino_db -c "SELECT COUNT(*) FROM casino_games_raw;"
```
---

### PHASE 4 & 5 — Batch Processing with Apache Spark

This step runs the full Spark pipeline indicated in the instructions: cleaning, transformations, feature generation, joins, and aggregations. Output is saved to Parquet.

```powershell
python processing/spark_processing.py
```

**Verify Parquet files were created:**

```powershell
ls data/processed
ls data/curated
```

> After this, there should be `.parquet` files in `data/processed/` and 5 subfolders in `data/curated/`.

---

### PHASE 6 — Streaming / Incremental Processing

This step simulates streaming by splitting the dataset into 5 batches and processing them incrementally.

```powershell
python streaming/streaming_simulation.py
```

**Expected output:**
```
Casino Streaming Simulation - Phase 6
total rows     : 1,200,000
batches        : 5
rows per batch : 240,000

  batch |   input rows |  output rows | status
      0 |      240,000 |      240,000 | processed
      1 |      240,000 |      480,000 | processed
      2 |      240,000 |      720,000 | processed
      3 |      240,000 |      960,000 | processed
      4 |      240,000 |    1,200,000 | processed

incremental processing complete
total rows written : 1,200,000
```

---

### PHASE 7 — Analytical Data Model

Throughout this phase we load the curated Parquet files into PostgreSQL as structured analytical tables.

```powershell
python analytics/analytical_model.py
```

**Expected output:**
```
connected to PostgreSQL (casino_db)
creating table: provider_stats...     rows inserted: 59
creating table: game_type_stats...    rows inserted: 7
creating table: volatility_stats...   rows inserted: 4
creating table: rtp_category_stats... rows inserted: 3
creating table: bet_tier_stats...     rows inserted: 3

```
---

### PHASE 8 — Export Data for Power BI

This step exports all analytical tables + raw data to CSV files for Power BI.

```powershell
python analytics/export_for_powerbi.py
```


**Import into Power BI:**
1. Open Power BI Desktop
2. Click **Get Data → Text/CSV**
3. Import each file from `data/exports/`

---

### PHASE 9 — AI Component (K-Means Clustering)

This step applies K-Means clustering to group games into 4 profiles.

```powershell
python analytics/ai_clustering.py
```
---

## Summary of full execution  steps in order

```powershell
# Start Docker
docker-compose up -d

# Ingestion
python ingestion/ingest_raw_data.py online_casino_games_dataset_v2.csv

# Load to PostgreSQL
python ingestion/setup_database.py

# Spark Batch Processing
python processing/spark_processing.py

# Streaming Simulation
python streaming/streaming_simulation.py

# Analytical Model
python analytics/analytical_model.py

# Export for Power BI
python analytics/export_for_powerbi.py

# AI Clustering but it's optional
python analytics/ai_clustering.py
```

---

