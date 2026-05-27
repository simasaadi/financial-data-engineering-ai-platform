# Financial Data Engineering AI Platform

[![Data Engineering CI](https://github.com/simasaadi/financial-data-engineering-ai-platform/actions/workflows/data-engineering-ci.yml/badge.svg)](https://github.com/simasaadi/financial-data-engineering-ai-platform/actions/workflows/data-engineering-ci.yml)
[![Open in Streamlit](https://img.shields.io/badge/Open%20in-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://financial-data-engineering-ai-platfrom.streamlit.app)

A production-style financial data engineering platform built with synthetic banking data. The project demonstrates batch, API-style, and simulated streaming ingestion; bronze/silver/gold data architecture; automated data quality controls; ML-ready feature generation; CI/CD testing; and AI-assisted pipeline triage.

This repository is designed to show how raw financial data can be turned into trusted analytics and machine-learning-ready datasets in a governed, auditable, and production-oriented workflow.

---

## Project summary

Financial institutions rely on data pipelines that are reliable, tested, auditable, and usable by both technical and business teams. This project simulates that environment using synthetic data and a modern local data engineering stack.

The pipeline creates a full data lifecycle:

1. Generate synthetic financial source data
2. Ingest raw files into a bronze layer
3. Standardize and validate data into a silver layer
4. Build gold analytics marts
5. Create ML-ready customer risk features
6. Run AI-assisted data quality triage
7. Test the pipeline through GitHub Actions CI

No real customer, banking, or confidential data is used.

---

## Live dashboard

The Streamlit dashboard provides a visual monitoring layer for the pipeline.

[![Open in Streamlit](https://img.shields.io/badge/Open%20in-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://financial-data-engineering-ai-platfrom.streamlit.app)

If the public Streamlit deployment is not active yet, the dashboard can be run locally:

```powershell
.\.venv\Scripts\streamlit.exe run dashboards/streamlit_app.py
```

---

## What this project demonstrates

| Capability | How it is demonstrated |
|---|---|
| Data ingestion | Synthetic batch CSV, JSON API-style data, and JSONL streaming-style fraud events |
| Data lakehouse pattern | Raw, bronze, silver, gold, and ML feature layers |
| Data quality | Required-field checks, uniqueness checks, positive amount checks, currency validation, rejection capture |
| Pipeline reliability | End-to-end orchestration script and automated tests |
| CI/CD | GitHub Actions workflow runs the full pipeline and test suite |
| Analytics engineering | Gold marts for customer 360, product performance, transaction summaries, and data quality monitoring |
| ML readiness | Customer-level feature table with synthetic high-risk label |
| AI automation | AI-assisted data quality triage that classifies priority, recommends owners, and produces issue-log outputs |
| Governance | Metadata outputs, rejection records, audit tables, lineage-style layer separation, and governance issue log |

---

## Architecture

```text
Synthetic source systems
  +-- customers.csv
  +-- accounts.csv
  +-- products.csv
  +-- transactions.csv
  +-- customer_service_notes.csv
  +-- exchange_rates_api.json
  +-- fraud_alert_events.jsonl

Pipeline layers
  +-- Raw landing
  +-- Bronze ingestion with load metadata
  +-- Silver validation and standardization
  +-- Gold analytics marts
  +-- ML-ready customer risk features
  +-- AI-assisted data quality triage

Outputs
  +-- DuckDB analytical database
  +-- Parquet files by layer
  +-- Data quality dashboard table
  +-- Governance issue log
  +-- Streamlit monitoring dashboard
  +-- CI-tested pipeline
```

---

## Data layers

### Raw layer

The raw layer contains synthetic source-system data.

| Dataset | Format | Description |
|---|---|---|
| customers | CSV | Synthetic customer records |
| accounts | CSV | Synthetic account records |
| products | CSV | Product reference data |
| transactions | CSV | Synthetic transaction history |
| customer_service_notes | CSV | Semi-structured customer service cases |
| exchange_rates_api | JSON | API-style reference data |
| fraud_alert_events | JSONL | Simulated streaming fraud alert events |

### Bronze layer

The bronze layer preserves source-aligned data and adds ingestion metadata:

- bronze load ID
- ingestion timestamp
- source file
- source system
- raw row hash

### Silver layer

The silver layer applies standardization and validation:

- column standardization
- text normalization
- date and numeric conversion
- required-field checks
- duplicate detection
- currency checks
- positive amount validation
- rejected-record capture
- silver transform audit table

### Gold layer

The gold layer creates business-ready analytics marts:

| Gold table | Purpose |
|---|---|
| gold_customer_360 | Customer-level integrated analytics view |
| gold_daily_transaction_summary | Daily transaction reporting mart |
| gold_product_performance | Product/account/customer performance view |
| gold_data_quality_dashboard | Data quality monitoring and acceptance-rate summary |

### ML feature layer

The ML feature layer creates:

| Table | Purpose |
|---|---|
| ml_customer_risk_features | Customer-level risk features for downstream modelling |

The feature table includes transaction activity, product/account relationships, international transaction ratios, fraud alert patterns, service-case counts, and a synthetic high-risk label.

---

## AI-assisted data quality triage

The AI automation module reads the data quality dashboard and rejected records, then produces a triage output.

It generates:

- issue ID
- affected dataset
- priority
- triage status
- recommended owner
- plain-language issue summary
- recommended action
- governance issue log

Example output:

| Dataset | Priority | Triage status | Recommended owner |
|---|---|---|---|
| transactions | p1_high | escalate_immediately | payments_data_engineering_team |

This simulates an enterprise-safe AI/decision-engine pattern. It does not change production data automatically. It creates auditable recommendations for human review.

---

## Dashboard views

The Streamlit dashboard includes:

1. Pipeline overview
2. Data quality and AI triage
3. Gold marts
4. ML-ready features

Key dashboard metrics include:

- silver transaction count
- customer 360 record count
- data quality rule hits
- synthetic high-risk customer count
- pipeline row counts by layer
- data quality acceptance rates
- AI triage recommendations
- customer and product analytics
- ML feature exploration

---

## Project structure

```text
financial-data-engineering-ai-platform/
¦
+-- .github/
¦   +-- workflows/
¦       +-- data-engineering-ci.yml
¦
+-- architecture/
¦
+-- config/
¦   +-- settings.yml
¦
+-- dashboards/
¦   +-- streamlit_app.py
¦
+-- data/
¦   +-- raw/
¦   +-- bronze/
¦   +-- silver/
¦   +-- gold/
¦   +-- ml_features/
¦   +-- metadata/
¦
+-- src/
¦   +-- ingestion/
¦   ¦   +-- generate_synthetic_data.py
¦   ¦   +-- raw_to_bronze.py
¦   ¦
¦   +-- transformations/
¦   ¦   +-- bronze_to_silver.py
¦   ¦   +-- silver_to_gold.py
¦   ¦
¦   +-- ai_automation/
¦   ¦   +-- dq_failure_triage.py
¦   ¦
¦   +-- orchestration/
¦       +-- run_pipeline.py
¦
+-- tests/
¦   +-- test_data_quality.py
¦   +-- test_dashboard.py
¦   +-- test_pipeline_outputs.py
¦
+-- .env.example
+-- .gitignore
+-- pytest.ini
+-- requirements.txt
+-- run_all.ps1
+-- README.md
```

---

## Quick start

### 1. Clone the repository

```powershell
git clone https://github.com/simasaadi/financial-data-engineering-ai-platform.git
cd financial-data-engineering-ai-platform
```

### 2. Create a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 3. Run the full pipeline

```powershell
.\.venv\Scripts\python.exe src/orchestration/run_pipeline.py
```

Or run the PowerShell helper:

```powershell
.\run_all.ps1
```

### 4. Run tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```

### 5. Run the dashboard

```powershell
.\.venv\Scripts\streamlit.exe run dashboards/streamlit_app.py
```

---

## CI/CD

The GitHub Actions workflow runs on push and pull request to `main`.

It performs:

1. Repository checkout
2. Python setup
3. Dependency installation
4. Full pipeline execution
5. Pytest validation

Workflow file:

```text
.github/workflows/data-engineering-ci.yml
```

---

## Example pipeline outputs

After a successful pipeline run, the project produces:

```text
data/raw/
data/bronze/
data/silver/
data/gold/
data/ml_features/
data/metadata/
data/financial_platform.duckdb
```

Key output files include:

```text
data/gold/gold_customer_360.parquet
data/gold/gold_daily_transaction_summary.parquet
data/gold/gold_product_performance.parquet
data/gold/gold_data_quality_dashboard.parquet
data/ml_features/ml_customer_risk_features.parquet
data/metadata/ai_pipeline_triage.csv
data/metadata/governance_issue_log.csv
```

---

## Data quality controls

The project includes rule-based checks such as:

- required customer/account/transaction identifiers
- unique primary keys
- valid transaction timestamps
- positive transaction amounts
- CAD currency standardization
- risk score range checks
- exchange rate validity
- rejected-record quarantine
- transform audit tracking

The data quality dashboard summarizes:

- bronze row count
- silver accepted row count
- acceptance rate
- critical rule hits
- high rule hits
- total rejection rule hits

---

## Technology stack

| Category | Tools |
|---|---|
| Language | Python, SQL |
| Database | DuckDB |
| Data processing | pandas, NumPy |
| Data formats | CSV, JSON, JSONL, Parquet |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Dashboard | Streamlit, Plotly |
| Configuration | YAML, dotenv pattern |
| Automation | Python orchestration, PowerShell helper |
| Governance outputs | audit tables, rejection logs, issue logs, triage outputs |

---

## Why DuckDB?

DuckDB is used as a lightweight analytical engine for local development. It supports SQL-based transformation, local analytical workloads, and Parquet export without requiring cloud infrastructure.

The architecture is intentionally portable. The same pattern could be adapted to:

- AWS S3
- AWS Glue
- Amazon Athena
- Redshift
- Databricks
- Snowflake
- Microsoft Fabric
- Airflow
- dbt

---

## Relevance to enterprise data engineering

This project reflects common enterprise data engineering requirements:

- reliable multi-source ingestion
- data validation before analytics consumption
- production-style layer separation
- reusable gold marts
- ML-ready feature engineering
- automated test coverage
- CI/CD-based quality gates
- auditable metadata and issue logs
- business-readable pipeline monitoring
- AI-assisted operational triage

---

## Synthetic data notice

All data in this repository is synthetic and generated for demonstration purposes only.

The project does not use:

- real customer data
- real bank data
- confidential financial information
- production credentials
- private APIs

---

## Future enhancements

Planned improvements:

- Add dbt models and dbt tests
- Add Airflow or Prefect DAG orchestration
- Add Docker Compose support
- Add AWS S3-style local object storage pattern
- Add Kafka or Redpanda streaming simulation
- Add Great Expectations or Soda Core validation suite
- Add OpenMetadata or DataHub-compatible metadata export
- Add architecture diagrams
- Add model training notebook using ML-ready features
- Add deployment instructions for Streamlit Community Cloud

---

## Author

**Sima Saadi**  
Data Governance, Data Engineering, Analytics, and GIS  
GitHub: [github.com/simasaadi](https://github.com/simasaadi)
LinkedIn: [linkedin.com/in/sima-saadi](https://www.linkedin.com/in/sima-saadi/)

