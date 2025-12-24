# 🚀 End-to-End Azure Data Engineering Project
**Azure SQL → ADF → ADLS Gen2 → Databricks → Delta Live Tables (Gold) with AutoCDC**

---

## 📌 Project Overview

This project demonstrates an **end-to-end Azure Data Engineering pipeline** that ingests data from an **Azure SQL Database**, performs **incremental ingestion using Azure Data Factory**, applies transformations using **Azure Databricks**, and builds **Gold-layer curated tables using Databricks Delta Live Tables (DLT) with AutoCDC enabled**.

The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** and uses **Databricks Asset Bundles** for deployment and environment management.

---

## 🏗️ High-Level Architecture

**Source → Ingestion → Storage → Transformation → Curated Analytics**

<img width="321" height="363" alt="image" src="https://github.com/user-attachments/assets/bf54e7fe-8e06-49a2-9dc2-981ed0c33ef5" />

---

## 🧰 Technology Stack

| Layer | Technology |
|------|-----------|
| Source | Azure SQL Database |
| Ingestion | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks |
| Gold Layer | Delta Live Tables (DLT) |
| CDC | AutoCDC |
| Deployment | Databricks Asset Bundles |
| Version Control | GitHub |

---

## 🔄 Data Ingestion (Azure SQL → ADLS Gen2)

### Source System
- Azure SQL Database (OLTP)
- Tables:
  - `DimUser`
  - `DimDate`
  - `DimArtist`
  - `DimTrack`

### Incremental Load Strategy
- CDC / watermark column (e.g., `updated_at`, `date_key`)
- Last processed timestamp maintained
- Only new or updated records ingested

### ADF Pipeline Flow
1. **Lookup – `last_cdc`**
   - Reads last processed timestamp

2. **Set Variable – `current_timestamp`**
   - Captures execution time

3. **Copy Data – `AzureSQLToLake`**
   ```sql
   WHERE cdc_timestamp > last_cdc
   ```
   - Loads data into **Bronze layer**

4. **If Condition – `if_new_records`**
   - Updates watermark or logs activity

---

## 🟤 Bronze Layer
- Raw data
- Stored in **ADLS Gen2**
- Format: **Parquet**

---

## ⚙️ Silver Layer
- Cleansing and transformations
- Stored as **Delta tables**

---

## 🥇 Gold Layer (DLT + AutoCDC)
- Automated quality checks
- Insert / Update / Delete handling
- No manual MERGE logic

---

## 📦 Databricks Asset Bundles
- IaC based deployments
- Environment isolation
- CI/CD ready

---

## 🚀 How to Deploy & Run

### 1️⃣ Clone Repository
```bash
git clone https://github.com/yourusername/azure-end-to-end-data-engineering.git
```

### 2️⃣ Deploy Databricks Asset Bundle
```bash
databricks bundle deploy
```

### 3️⃣ Trigger ADF Pipeline
- Run incremental ingestion
- Data lands in **Bronze layer**

### 4️⃣ Run Databricks Workflows
- Silver transformations execute
- Gold tables built using DLT

---

## 📊 Data Quality & Monitoring
- DLT quality rules
- Databricks UI monitoring
- Failed record quarantine
- End-to-end lineage

---

## 📸 Screenshots
- ADF pipeline design
- DLT DAG

---

## 🧠 Key Learnings 
- Azure Data Factory incremental loads
- Medallion Architecture
- Databricks Delta & DLT
- AutoCDC
- Asset Bundles & IaC
- Production-grade pipelines

## 🧑‍💻 Author
### Jay Kothawade
**Azure Data Engineer**

GitHub: https://github.com/jayvkothawade

LinkedIn: https://linkedin.com/in/jaykothawade

