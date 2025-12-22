# 🚀 End-to-End Azure Data Engineering Project (Use Case : Spotify)  
**Azure SQL → ADF → ADLS Gen2 → Databricks → DLT (Gold) with AutoCDC**

---

## 📌 Project Overview

This project demonstrates an **end-to-end Azure Data Engineering pipeline** that ingests data from an **Azure SQL Database**, performs **incremental ingestion using Azure Data Factory**, applies transformations using **Azure Databricks**, and builds **Gold-layer curated tables using Databricks Delta Live Tables (DLT) with AutoCDC enabled**.

The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** and uses **Databricks Asset Bundles** for deployment and environment management.

---

## 🏗️ High-Level Architecture

**Source → Ingestion → Storage → Transformation → Curated Analytics**

<img width="321" height="363" alt="image" src="https://github.com/user-attachments/assets/89b46b1f-f366-4e5a-a731-7a9864011dd3" />

---

## 🧰 Technology Stack

| Layer           | Technology                          |
|-----------------|-------------------------------------|
| Source          | Azure SQL Database                  |
| Ingestion       | Azure Data Factory (ADF)            |
| Storage         | Azure Data Lake Storage Gen2        |
| Processing      | Azure Databricks                    |
| Gold Layer      | Databricks Delta Live Tables (DLT)  |
| CDC             | AutoCDC (DLT)                       |
| Deployment      | Databricks Asset Bundles            |
| Version Control | GitHub                              |

---

## 🔄 Data Ingestion (Azure SQL → ADLS Gen2)

### 🔹 Source: Azure SQL Database

- Acts as the transactional system (**OLTP**)
- Contains tables such as:
  - `DimUser`
  - `DimDate`
  - `DimArtist`
  - `DimTrack`

---

### 🔹 Incremental Load Strategy

- A **CDC / watermark column** ('updated_at, date_key') is used to identify new records
- The pipeline maintains the **last successfully processed timestamp**
- Each execution fetches **only records greater than the stored watermark**, avoiding full reloads and improving performance

---

### 🔹 ADF Pipeline Flow

1. **Lookup Activity – `last_cdc`**
   - Retrieves the last processed CDC timestamp from a **JSON file**
   - Acts as the watermark for the current run

2. **Set Variable – `current_timestamp`**
   - Captures the current pipeline execution timestamp
   - Used to update the watermark after successful ingestion

3. **Copy Data Activity – `AzureSQLToLake`**
   - Extracts incremental records from Azure SQL using a parameterized SQL query
   - Example logic:
     ```sql
     WHERE cdc_timestamp > last_cdc
     ```
   - Loads data into **ADLS Gen2 (Bronze layer)** in raw format

4. **If Condition – `if_new_records`**
   - Checks whether new records were ingested

   **True Path:**
   - `max_cdc` calculates the maximum CDC timestamp from ingested data
   - `copy_last_cdc` updates the control file/table with the new watermark

   **False Path:**
   - Executes cleanup or logging activity when no new records are found

---

### 🔹 Key Benefits

- Prevents duplicate data ingestion
- Optimized for large datasets
- Supports restartability and fault tolerance
- Follows Medallion Architecture (Bronze layer ingestion)
- Production-ready incremental design

---

## 🟤 Bronze Layer (Raw Data)

- Stored in **ADLS Gen2**
- Data ingested **as-is** from Azure SQL
- No transformations applied
- File format:
  - **Parquet**

**Purpose:**
- ✔ Data traceability  
- ✔ Reprocessing capability  
- ✔ Source data preservation  

---

## ⚙️ Silver Layer (Databricks Transformations)

Databricks notebooks process raw Bronze data and create **clean, structured Silver tables**.

### Silver Layer Transformations

- Data type casting
- Deduplication
- Null handling
- Column standardization
- Business rule enforcement

**Example Transformations:**
- Removing duplicate customers
- Standardizing date formats
- Filtering invalid records

Silver data is stored back into **ADLS Gen2 in Delta format**.

---

## 🥇 Gold Layer (Delta Live Tables + AutoCDC)

### 🔹 Databricks Delta Live Tables (DLT)

Gold layer tables are created using **Delta Live Tables**, which:
- Automate data quality checks
- Handle dependencies
- Provide pipeline observability

---

### 🔹 AutoCDC Implementation

AutoCDC automatically handles:
- Inserts
- Updates
- Deletes

DLT detects changes in Silver tables and applies them to Gold tables **without manual merge logic**.

**Benefits:**
- ✔ No custom `MERGE` statements  
- ✔ Simplified SCD handling  
- ✔ Scalable & maintainable pipelines  

Gold tables represent **analytics-ready, business-curated data**.

---

## 📦 Databricks Asset Bundles

This project uses **Databricks Asset Bundles** for:

- Environment management (**dev / test / prod**)
- Infrastructure as code
- Repeatable deployments

### Asset Bundle Capabilities

- Declarative pipeline configuration
- CI/CD integration
- Version-controlled deployments

---

## 🚀 How to Deploy & Run

### 1️⃣ Clone Repository

git clone https://github.com/yourusername/azure-end-to-end-data-engineering.git

### 2️⃣ Deploy Databricks Asset Bundle
databricks bundle deploy

### 3️⃣ Trigger ADF Pipeline

Run the incremental ingestion pipeline

Data lands in the Bronze layer

### 4️⃣ Run Databricks Workflows

Silver transformations execute

DLT pipeline builds Gold tables


### 📊 Data Quality & Monitoring

DLT enforces data quality rules

Pipeline health visible in Databricks UI

Failed records can be quarantined

End-to-end data lineage supported


### 📸 Screenshots

The screenshots/ directory contains:

ADF pipeline design

DLT pipeline DAG

### 🧠 Key Learnings & Skills Demonstrated

✔ Incremental ingestion using Azure Data Factory

✔ Medallion architecture implementation

✔ Databricks Delta & Delta Live Tables (DLT)

✔ AutoCDC for change data capture

✔ Databricks Asset Bundles & IaC concepts

✔ Production-ready Azure data pipelines

