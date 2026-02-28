# 🚀 VN30 Stock Lakehouse Project

A Data Lakehouse system that collects VN30 stock data. The pipeline processes data through Bronze and Silver layers using Spark and Delta Lake on Docker.

## 🏗️ Data Architecture
- **Bronze Layer:** Raw data ingested directly into MinIO as Parquet/JSON.
- **Silver Layer:** Cleaned, transformed, and secured data (PII Masking) stored in **Delta Lake** format with partitioning.
- **Infrastructure:** Docker Compose (MinIO & Spark).

---

## 🛠️ Operations Guide

### 1. Start Infrastructure
Launch storage (MinIO) and compute (Spark Master/Worker) services:
```powershell
docker-compose up -d
```
*Access MinIO Console: [http://localhost:9001](http://localhost:9001) (User/Pass: minioadmin/minioadmin)*

### 2. Data Ingestion (Bronze Layer)
Run the pipeline to collect data from APIs and Web Crawl:
```powershell
python main.py
```

### 3. Data Transformation (Silver Layer with Delta Lake)
Execute Spark scripts inside the Docker environment. These scripts use Delta Lake for ACID transactions and efficient metadata handling.

**Process Stock Data:**
```powershell
docker exec -e HADOOP_USER_NAME=bitnami spark-master-2 spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0 `
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" `
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" `
  /opt/bitnami/spark/app/transform_stock.py
```

### 4. Query & Analytics
Run consolidated reports from the Silver Delta Lake layer:
```powershell
docker exec -e HADOOP_USER_NAME=bitnami spark-master-2 spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0 `
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" `
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" `
  /opt/bitnami/spark/app/query_silver.py
```

---

## 📝 Key Files
- `main.py`: Entry point for data ingestion.
- `transform_*.py`: Spark transformation logic.
- `query_silver.py`: Analytics and summary reports.
- `spark_minio.py`: Shared Spark-MinIO configuration.
- `docker-compose.yaml`: Infrastructure definition.
