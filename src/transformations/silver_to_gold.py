"""
Silver-to-gold transformation pipeline.

This script builds business-ready gold marts and ML-ready feature tables
from standardized silver financial datasets.

Gold layer outputs:
- gold_customer_360
- gold_daily_transaction_summary
- gold_product_performance
- gold_data_quality_dashboard
- ml_customer_risk_features
"""

from __future__ import annotations

from pathlib import Path

import duckdb


DUCKDB_PATH = Path("data/financial_platform.duckdb")
GOLD_PATH = Path("data/gold")
ML_FEATURES_PATH = Path("data/ml_features")

GOLD_PATH.mkdir(parents=True, exist_ok=True)
ML_FEATURES_PATH.mkdir(parents=True, exist_ok=True)


def export_table(connection: duckdb.DuckDBPyConnection, table_name: str, output_path: Path) -> None:
    parquet_path = str(output_path).replace("\\", "/")
    connection.execute(f"""
        COPY {table_name}
        TO '{parquet_path}'
        (FORMAT PARQUET)
    """)


def build_gold_customer_360(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TABLE gold_customer_360 AS
        WITH account_agg AS (
            SELECT
                customer_id,
                COUNT(DISTINCT account_id) AS account_count,
                COUNT(DISTINCT product_id) AS product_count,
                SUM(CASE WHEN account_status = 'active' THEN 1 ELSE 0 END) AS active_account_count,
                SUM(current_balance) AS total_current_balance,
                AVG(current_balance) AS average_current_balance
            FROM silver_accounts
            GROUP BY customer_id
        ),

        transaction_agg AS (
            SELECT
                customer_id,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_transaction_amount,
                AVG(amount) AS average_transaction_amount,
                MAX(transaction_timestamp) AS most_recent_transaction_at,
                SUM(CASE WHEN is_international = true THEN 1 ELSE 0 END) AS international_transaction_count,
                COUNT(DISTINCT channel) AS channel_count
            FROM silver_transactions
            GROUP BY customer_id
        ),

        fraud_agg AS (
            SELECT
                customer_id,
                COUNT(*) AS fraud_alert_count,
                AVG(risk_score) AS average_fraud_risk_score,
                MAX(risk_score) AS maximum_fraud_risk_score,
                SUM(CASE WHEN recommended_action = 'review' THEN 1 ELSE 0 END) AS fraud_review_count
            FROM silver_fraud_alert_events
            GROUP BY customer_id
        ),

        service_agg AS (
            SELECT
                customer_id,
                COUNT(*) AS service_case_count,
                SUM(CASE WHEN priority = 'high' THEN 1 ELSE 0 END) AS high_priority_case_count,
                SUM(CASE WHEN case_status IN ('open', 'in_progress') THEN 1 ELSE 0 END) AS open_case_count,
                MAX(created_at) AS most_recent_service_case_at
            FROM silver_customer_service_notes
            GROUP BY customer_id
        )

        SELECT
            c.customer_id,
            c.province,
            c.customer_segment,
            c.risk_tier,
            c.created_at AS customer_created_at,
            c.is_active,

            COALESCE(a.account_count, 0) AS account_count,
            COALESCE(a.product_count, 0) AS product_count,
            COALESCE(a.active_account_count, 0) AS active_account_count,
            ROUND(COALESCE(a.total_current_balance, 0), 2) AS total_current_balance,
            ROUND(COALESCE(a.average_current_balance, 0), 2) AS average_current_balance,

            COALESCE(t.transaction_count, 0) AS transaction_count,
            ROUND(COALESCE(t.total_transaction_amount, 0), 2) AS total_transaction_amount,
            ROUND(COALESCE(t.average_transaction_amount, 0), 2) AS average_transaction_amount,
            t.most_recent_transaction_at,
            COALESCE(t.international_transaction_count, 0) AS international_transaction_count,
            COALESCE(t.channel_count, 0) AS channel_count,

            COALESCE(f.fraud_alert_count, 0) AS fraud_alert_count,
            ROUND(COALESCE(f.average_fraud_risk_score, 0), 3) AS average_fraud_risk_score,
            ROUND(COALESCE(f.maximum_fraud_risk_score, 0), 3) AS maximum_fraud_risk_score,
            COALESCE(f.fraud_review_count, 0) AS fraud_review_count,

            COALESCE(s.service_case_count, 0) AS service_case_count,
            COALESCE(s.high_priority_case_count, 0) AS high_priority_case_count,
            COALESCE(s.open_case_count, 0) AS open_case_count,
            s.most_recent_service_case_at

        FROM silver_customers c
        LEFT JOIN account_agg a
            ON c.customer_id = a.customer_id
        LEFT JOIN transaction_agg t
            ON c.customer_id = t.customer_id
        LEFT JOIN fraud_agg f
            ON c.customer_id = f.customer_id
        LEFT JOIN service_agg s
            ON c.customer_id = s.customer_id
    """)


def build_gold_daily_transaction_summary(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TABLE gold_daily_transaction_summary AS
        SELECT
            CAST(transaction_date AS DATE) AS transaction_date,
            transaction_type,
            channel,
            merchant_category,
            COUNT(*) AS transaction_count,
            ROUND(SUM(amount), 2) AS total_transaction_amount,
            ROUND(AVG(amount), 2) AS average_transaction_amount,
            ROUND(MAX(amount), 2) AS maximum_transaction_amount,
            SUM(CASE WHEN is_international = true THEN 1 ELSE 0 END) AS international_transaction_count
        FROM silver_transactions
        GROUP BY
            CAST(transaction_date AS DATE),
            transaction_type,
            channel,
            merchant_category
    """)


def build_gold_product_performance(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TABLE gold_product_performance AS
        WITH transaction_agg AS (
            SELECT
                account_id,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_transaction_amount,
                AVG(amount) AS average_transaction_amount
            FROM silver_transactions
            GROUP BY account_id
        )

        SELECT
            p.product_id,
            p.product_name,
            p.product_type,
            p.business_domain,
            COUNT(DISTINCT a.account_id) AS account_count,
            COUNT(DISTINCT a.customer_id) AS customer_count,
            ROUND(SUM(a.current_balance), 2) AS total_current_balance,
            ROUND(AVG(a.current_balance), 2) AS average_current_balance,
            COALESCE(SUM(t.transaction_count), 0) AS transaction_count,
            ROUND(COALESCE(SUM(t.total_transaction_amount), 0), 2) AS total_transaction_amount,
            ROUND(COALESCE(AVG(t.average_transaction_amount), 0), 2) AS average_transaction_amount
        FROM silver_products p
        LEFT JOIN silver_accounts a
            ON p.product_id = a.product_id
        LEFT JOIN transaction_agg t
            ON a.account_id = t.account_id
        GROUP BY
            p.product_id,
            p.product_name,
            p.product_type,
            p.business_domain
    """)


def build_gold_data_quality_dashboard(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TABLE gold_data_quality_dashboard AS
        WITH reject_summary AS (
            SELECT
                dataset_name,
                COUNT(*) AS total_reject_rule_hits,
                SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_reject_rule_hits,
                SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_reject_rule_hits,
                SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) AS medium_reject_rule_hits
            FROM silver_data_quality_rejects
            GROUP BY dataset_name
        )

        SELECT
            a.dataset_name,
            a.bronze_table,
            a.silver_table,
            a.bronze_row_count,
            a.silver_row_count,
            a.rejected_rule_hits,
            CAST(a.silver_row_count AS DOUBLE) / NULLIF(CAST(a.bronze_row_count AS DOUBLE), 0) AS acceptance_rate,
            COALESCE(r.total_reject_rule_hits, 0) AS total_reject_rule_hits,
            COALESCE(r.critical_reject_rule_hits, 0) AS critical_reject_rule_hits,
            COALESCE(r.high_reject_rule_hits, 0) AS high_reject_rule_hits,
            COALESCE(r.medium_reject_rule_hits, 0) AS medium_reject_rule_hits,
            a.processed_at_utc,
            a.status
        FROM silver_transform_audit a
        LEFT JOIN reject_summary r
            ON a.dataset_name = r.dataset_name
    """)


def build_ml_customer_risk_features(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE OR REPLACE TABLE ml_customer_risk_features AS
        SELECT
            customer_id,
            province,
            customer_segment,
            risk_tier,
            is_active,

            account_count,
            product_count,
            active_account_count,
            total_current_balance,
            average_current_balance,

            transaction_count,
            total_transaction_amount,
            average_transaction_amount,
            international_transaction_count,
            channel_count,

            fraud_alert_count,
            average_fraud_risk_score,
            maximum_fraud_risk_score,
            fraud_review_count,

            service_case_count,
            high_priority_case_count,
            open_case_count,

            CASE
                WHEN transaction_count = 0 THEN 0
                ELSE CAST(international_transaction_count AS DOUBLE) / CAST(transaction_count AS DOUBLE)
            END AS international_transaction_ratio,

            CASE
                WHEN fraud_alert_count = 0 THEN 0
                ELSE CAST(fraud_review_count AS DOUBLE) / CAST(fraud_alert_count AS DOUBLE)
            END AS fraud_review_ratio,

            CASE
                WHEN account_count = 0 THEN 0
                ELSE CAST(transaction_count AS DOUBLE) / CAST(account_count AS DOUBLE)
            END AS transactions_per_account,

            CASE
                WHEN risk_tier = 'high'
                  OR maximum_fraud_risk_score >= 0.85
                  OR fraud_review_count >= 3
                  OR high_priority_case_count >= 2
                THEN 1
                ELSE 0
            END AS synthetic_high_risk_label

        FROM gold_customer_360
    """)


def main() -> None:
    print("Starting stable silver-to-gold transformation...")

    with duckdb.connect(str(DUCKDB_PATH)) as connection:
        print("Building gold_customer_360...")
        build_gold_customer_360(connection)

        print("Building gold_daily_transaction_summary...")
        build_gold_daily_transaction_summary(connection)

        print("Building gold_product_performance...")
        build_gold_product_performance(connection)

        print("Building gold_data_quality_dashboard...")
        build_gold_data_quality_dashboard(connection)

        print("Building ml_customer_risk_features...")
        build_ml_customer_risk_features(connection)

        gold_tables = [
            "gold_customer_360",
            "gold_daily_transaction_summary",
            "gold_product_performance",
            "gold_data_quality_dashboard",
        ]

        ml_tables = [
            "ml_customer_risk_features",
        ]

        for table in gold_tables:
            export_table(connection, table, GOLD_PATH / f"{table}.parquet")

        for table in ml_tables:
            export_table(connection, table, ML_FEATURES_PATH / f"{table}.parquet")

        print("\nGold table counts:")
        for table in gold_tables:
            count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table}: {count:,} rows")

        print("\nML feature table counts:")
        for table in ml_tables:
            count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table}: {count:,} rows")

    print("\nStable silver-to-gold transformation completed successfully.")


if __name__ == "__main__":
    main()
