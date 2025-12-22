🚀 End-to-End Azure Data Engineering Project
Azure SQL → ADF → ADLS Gen2 → Databricks → DLT (Gold) with AutoCDC

📌 Project Overview
This project demonstrates a Azure Data Engineering pipeline that ingests data from an Azure SQL Database, performs incremental ingestion using Azure Data Factory, applies transformations using Databricks, and builds Gold-layer curated tables using Databricks Delta Live Tables (DLT) with AutoCDC enabled.
The solution follows a Medallion Architecture (Bronze → Silver → Gold) and uses Databricks Asset Bundles for deployment and environment management.

🏗️ High-Level Architecture
Source → Ingestion → Storage → Transformation → Curated Analytics

Azure SQL DB
    │
    │ (Incremental Load – Watermark)
    ▼
Azure Data Factory
    │
    │ (Raw Ingestion)
    ▼
ADLS Gen2 (Bronze Layer)
    │
    │ (Transformations)
    ▼
Databricks (Silver Layer)
    │
    │ (DLT + AutoCDC)
    ▼
Databricks Delta Tables (Gold Layer)

🧰 Technology Stack
Layer	Technology
Source	Azure SQL Database
Ingestion	Azure Data Factory (ADF)
Storage	Azure Data Lake Storage Gen2
Processing	Azure Databricks
Gold Layer	Databricks Delta Live Tables (DLT)
CDC	AutoCDC (DLT)
Deployment	Databricks Asset Bundles
Version Control	GitHub

🔄 Data Ingestion (Azure SQL → ADLS Gen2)
🔹 Source: Azure SQL Database
	• Acts as the transactional system (OLTP)
	• Contains tables such as:
		○ DimUser
		○ DimDate
		○ DimArtist
		○ DimTrack

🔹 Incremental Load Strategy
	• A CDC / watermark column (e.g., LastModifiedDate or CDC_Timestamp) is used to identify new records
	• The pipeline maintains the last successfully processed timestamp
	• Each execution fetches only records greater than the stored watermark, avoiding full reloads and improving performance

🔹 ADF Pipeline Flow
	1. Lookup Activity – last_cdc
		○ Retrieves the last processed CDC timestamp from a json file
		○ This value acts as the watermark for the current run
	2. Set Variable – current_timestamp
		○ Captures the current pipeline execution timestamp
		○ Used later to update the watermark after a successful load
	3. Copy Data Activity – AzureSQLToLake
		○ Extracts incremental records from Azure SQL using a parameterized SQL query
		○ Example logic:

WHERE cdc_timestamp > last_cdc
		○ Loads data into ADLS Gen2 (Bronze layer) in raw format
	4. If Condition – if_new_records
		○ Checks whether new records were ingested
		○ True path:
			§ max_cdc activity calculates the maximum CDC timestamp from ingested data
			§ copy_last_cdc updates the control table with the new watermark
		○ False path:
			§ Executes cleanup/logging activity when no new records are found

🔹 Key Benefits
	• Prevents duplicate data ingestion
	• Optimized for large datasets
	• Supports restartability and fault tolerance
	• Follows medallion architecture (Bronze layer ingestion)
	• Production-ready incremental design

🟤 Bronze Layer (Raw Data)
	• Stored in ADLS Gen2
	• Data is ingested as-is from Azure SQL
	• No transformations applied
	• File formats:
		○ Parquet
Purpose:
✔ Data traceability
✔ Reprocessing capability
✔ Source data preservation

⚙️ Silver Layer (Databricks Transformations)
Databricks notebooks process raw Bronze data and create clean, structured Silver tables.
Silver Layer Transformations:
	• Data type casting
	• Deduplication
	• Null handling
	• Column standardization
	• Business rule enforcement
Example transformations:
	• Removing duplicate customers
	• Standardizing date formats
	• Filtering invalid records
Silver data is stored back into ADLS Gen2 in Delta format.

🥇 Gold Layer (Delta Live Tables + AutoCDC)
🔹 Databricks Delta Live Tables (DLT)
Gold layer tables are created using Delta Live Tables, which:
	• Automate data quality checks
	• Handle dependencies
	• Provide pipeline observability

🔹 AutoCDC Implementation
AutoCDC is used to automatically handle:
	• Inserts
	• Updates
	• Deletes
DLT detects changes in the Silver tables and applies them to Gold tables without manual merge logic.
Benefits:
✔ No custom MERGE statements
✔ Simplified SCD handling
✔ Scalable & maintainable pipelines
Gold tables represent analytics-ready, business-curated data.

📦 Databricks Asset Bundles
This project uses Databricks Asset Bundles for:
	• Environment management (dev/test/prod)
	• Infrastructure as code
	• Repeatable deployments
Asset Bundle Capabilities:
	• Declarative pipeline configuration
	• CI/CD integration
Version-controlled deployments<img width="596" height="2818" alt="image" src="https://github.com/user-attachments/assets/a42eaebf-d08e-4149-9f1c-d67adc1a0af5" />
