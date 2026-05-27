"""
Tests for core pipeline outputs.

These tests validate that the end-to-end financial data engineering pipeline
creates the expected DuckDB tables and output files.
"""

from pathlib import Path

import duckdb


DUCKDB_PATH = Path("data/financial_platform.duckdb")


EXPECTED_TABLES = [
    "bronze_customers",
    "bronze_accounts",
    "bronze_products",
    "bronze_transactions",
    "bronze_customer_service_notes",
    "bronze_exchange_rates_api",
    "bronze_fraud_alert_events",
    "silver_customers",
    "silver_accounts",
    "silver_products",
    "silver_transactions",
    "silver_customer_service_notes",
    "silver_exchange_rates_api",
    "silver_fraud_alert_events",
    "silver_data_quality_rejects",
    "silver_transform_audit",
    "gold_customer_360",
    "gold_daily_transaction_summary",
    "gold_product_performance",
    "gold_data_quality_dashboard",
    "ml_customer_risk_features",
    "ai_pipeline_triage",
]


EXPECTED_FILES = [
    "data/raw/customers.csv",
    "data/raw/accounts.csv",
    "data/raw/products.csv",
    "data/raw/transactions.csv",
    "data/raw/customer_service_notes.csv",
    "data/raw/exchange_rates_api.json",
    "data/raw/fraud_alert_events.jsonl",
    "data/bronze/bronze_transactions.parquet",
    "data/silver/silver_transactions.parquet",
    "data/gold/gold_customer_360.parquet",
    "data/gold/gold_data_quality_dashboard.parquet",
    "data/ml_features/ml_customer_risk_features.parquet",
    "data/metadata/ai_pipeline_triage.csv",
    "data/metadata/governance_issue_log.csv",
]


def test_duckdb_database_exists() -> None:
    assert DUCKDB_PATH.exists(), "DuckDB database was not created."


def test_expected_output_files_exist() -> None:
    missing_files = [file_path for file_path in EXPECTED_FILES if not Path(file_path).exists()]
    assert not missing_files, f"Missing expected output files: {missing_files}"


def test_expected_tables_exist() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))

    available_tables = {
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    }

    con.close()

    missing_tables = [table for table in EXPECTED_TABLES if table not in available_tables]
    assert not missing_tables, f"Missing expected DuckDB tables: {missing_tables}"


def test_gold_customer_360_has_expected_rows() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))
    row_count = con.execute("SELECT COUNT(*) FROM gold_customer_360").fetchone()[0]
    con.close()

    assert row_count == 1000


def test_ml_feature_table_has_expected_rows() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))
    row_count = con.execute("SELECT COUNT(*) FROM ml_customer_risk_features").fetchone()[0]
    con.close()

    assert row_count == 1000
