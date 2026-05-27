"""
Bronze-to-silver transformation pipeline.

This script reads bronze financial datasets from DuckDB, standardizes them,
applies data quality checks, writes clean silver tables, and captures rejected
records for auditability.

The silver layer creates trusted, standardized datasets for downstream gold marts,
ML-ready feature tables, dashboards, and AI-assisted pipeline monitoring.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


DUCKDB_PATH = Path("data/financial_platform.duckdb")
SILVER_PATH = Path("data/silver")
SILVER_PATH.mkdir(parents=True, exist_ok=True)


DATASETS = {
    "customers": {
        "bronze_table": "bronze_customers",
        "silver_table": "silver_customers",
        "required_columns": ["customer_id"],
        "unique_key": "customer_id",
    },
    "accounts": {
        "bronze_table": "bronze_accounts",
        "silver_table": "silver_accounts",
        "required_columns": ["account_id", "customer_id", "product_id"],
        "unique_key": "account_id",
    },
    "products": {
        "bronze_table": "bronze_products",
        "silver_table": "silver_products",
        "required_columns": ["product_id", "product_name"],
        "unique_key": "product_id",
    },
    "transactions": {
        "bronze_table": "bronze_transactions",
        "silver_table": "silver_transactions",
        "required_columns": ["transaction_id", "account_id", "customer_id"],
        "unique_key": "transaction_id",
    },
    "customer_service_notes": {
        "bronze_table": "bronze_customer_service_notes",
        "silver_table": "silver_customer_service_notes",
        "required_columns": ["case_id", "customer_id"],
        "unique_key": "case_id",
    },
    "exchange_rates_api": {
        "bronze_table": "bronze_exchange_rates_api",
        "silver_table": "silver_exchange_rates_api",
        "required_columns": ["rate_date", "target_currency", "exchange_rate"],
        "unique_key": None,
    },
    "fraud_alert_events": {
        "bronze_table": "bronze_fraud_alert_events",
        "silver_table": "silver_fraud_alert_events",
        "required_columns": ["event_id", "transaction_id", "account_id"],
        "unique_key": "event_id",
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [col.strip().lower() for col in cleaned.columns]
    return cleaned


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    for column in cleaned.columns:
        if cleaned[column].dtype == "object":
            if column.startswith("_"):
                continue
            cleaned[column] = cleaned[column].astype("string").str.strip()

    return cleaned


def standardize_known_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    date_columns = [
        "created_at",
        "opened_at",
        "transaction_timestamp",
        "event_timestamp",
        "rate_date",
        "loaded_at",
    ]

    numeric_columns = [
        "current_balance",
        "amount",
        "exchange_rate",
        "risk_score",
    ]

    lowercase_columns = [
        "province",
        "customer_segment",
        "risk_tier",
        "account_status",
        "currency",
        "product_name",
        "product_type",
        "business_domain",
        "transaction_type",
        "channel",
        "merchant_category",
        "ingestion_source",
        "issue_type",
        "priority",
        "case_status",
        "base_currency",
        "target_currency",
        "source_system",
        "alert_reason",
        "recommended_action",
        "event_source",
    ]

    for column in date_columns:
        if column in cleaned.columns:
            cleaned[column] = pd.to_datetime(cleaned[column], errors="coerce")

    for column in numeric_columns:
        if column in cleaned.columns:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    for column in lowercase_columns:
        if column in cleaned.columns:
            cleaned[column] = cleaned[column].astype("string").str.strip().str.lower()

    if "transaction_timestamp" in cleaned.columns:
        cleaned["transaction_date"] = cleaned["transaction_timestamp"].dt.date

    cleaned["_silver_processed_at_utc"] = utc_now()

    return cleaned


def build_rejects(
    df: pd.DataFrame,
    dataset_name: str,
    rule_name: str,
    severity: str,
    mask: pd.Series,
) -> pd.DataFrame:
    rejected = df.loc[mask].copy()

    if rejected.empty:
        return pd.DataFrame(
            columns=[
                "dataset_name",
                "rule_name",
                "severity",
                "rejected_at_utc",
                "record_payload_json",
            ]
        )

    rejected_payload = rejected.apply(
        lambda row: json.dumps(row.to_dict(), default=str),
        axis=1,
    )

    return pd.DataFrame(
        {
            "dataset_name": dataset_name,
            "rule_name": rule_name,
            "severity": severity,
            "rejected_at_utc": utc_now(),
            "record_payload_json": rejected_payload,
        }
    )


def validate_dataset(
    df: pd.DataFrame,
    dataset_name: str,
    required_columns: list[str],
    unique_key: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    reject_frames = []
    invalid_mask = pd.Series(False, index=df.index)

    for column in required_columns:
        if column in df.columns:
            mask = df[column].isna() | (df[column].astype("string").str.strip() == "")
            invalid_mask = invalid_mask | mask
            reject_frames.append(
                build_rejects(
                    df=df,
                    dataset_name=dataset_name,
                    rule_name=f"{column}_required",
                    severity="critical",
                    mask=mask,
                )
            )

    if unique_key and unique_key in df.columns:
        mask = df.duplicated(subset=[unique_key], keep=False)
        invalid_mask = invalid_mask | mask
        reject_frames.append(
            build_rejects(
                df=df,
                dataset_name=dataset_name,
                rule_name=f"{unique_key}_must_be_unique",
                severity="critical",
                mask=mask,
            )
        )

    if dataset_name == "transactions":
        if "amount" in df.columns:
            mask = df["amount"].isna() | (df["amount"] <= 0)
            invalid_mask = invalid_mask | mask
            reject_frames.append(
                build_rejects(df, dataset_name, "amount_must_be_positive", "critical", mask)
            )

        if "currency" in df.columns:
            mask = df["currency"] != "cad"
            invalid_mask = invalid_mask | mask
            reject_frames.append(
                build_rejects(df, dataset_name, "currency_must_be_cad", "high", mask)
            )

    if dataset_name == "accounts":
        if "currency" in df.columns:
            mask = df["currency"] != "cad"
            invalid_mask = invalid_mask | mask
            reject_frames.append(
                build_rejects(df, dataset_name, "currency_must_be_cad", "high", mask)
            )

    if dataset_name == "exchange_rates_api":
        if "exchange_rate" in df.columns:
            mask = df["exchange_rate"].isna() | (df["exchange_rate"] <= 0)
            invalid_mask = invalid_mask | mask
            reject_frames.append(
                build_rejects(df, dataset_name, "exchange_rate_must_be_positive", "critical", mask)
            )

    if dataset_name == "fraud_alert_events":
        if "risk_score" in df.columns:
            mask = df["risk_score"].isna() | (df["risk_score"] < 0) | (df["risk_score"] > 1)
            invalid_mask = invalid_mask | mask
            reject_frames.append(
                build_rejects(df, dataset_name, "risk_score_between_zero_and_one", "critical", mask)
            )

    valid_df = df.loc[~invalid_mask].copy()

    if reject_frames:
        reject_df = pd.concat(reject_frames, ignore_index=True)
    else:
        reject_df = pd.DataFrame(
            columns=[
                "dataset_name",
                "rule_name",
                "severity",
                "rejected_at_utc",
                "record_payload_json",
            ]
        )

    return valid_df, reject_df


def write_table_and_parquet(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
    parquet_file: Path,
) -> None:
    connection.register("temp_df", df)

    connection.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM temp_df
    """)

    parquet_path = str(parquet_file).replace("\\", "/")

    connection.execute(f"""
        COPY {table_name}
        TO '{parquet_path}'
        (FORMAT PARQUET)
    """)

    connection.unregister("temp_df")


def main() -> None:
    print("Starting robust bronze-to-silver transformation...")

    audit_records = []
    reject_frames = []

    with duckdb.connect(str(DUCKDB_PATH)) as connection:
        for dataset_name, config in DATASETS.items():
            bronze_table = config["bronze_table"]
            silver_table = config["silver_table"]

            print(f"\nProcessing {bronze_table} -> {silver_table}")

            bronze_df = connection.execute(f"SELECT * FROM {bronze_table}").fetchdf()

            silver_candidate = (
                bronze_df
                .pipe(clean_column_names)
                .pipe(clean_text_columns)
                .pipe(standardize_known_columns)
            )

            valid_df, reject_df = validate_dataset(
                df=silver_candidate,
                dataset_name=dataset_name,
                required_columns=config["required_columns"],
                unique_key=config["unique_key"],
            )

            write_table_and_parquet(
                connection=connection,
                table_name=silver_table,
                df=valid_df,
                parquet_file=SILVER_PATH / f"{silver_table}.parquet",
            )

            if not reject_df.empty:
                reject_frames.append(reject_df)

            audit_records.append(
                {
                    "dataset_name": dataset_name,
                    "bronze_table": bronze_table,
                    "silver_table": silver_table,
                    "bronze_row_count": len(bronze_df),
                    "silver_row_count": len(valid_df),
                    "rejected_rule_hits": len(reject_df),
                    "processed_at_utc": utc_now(),
                    "status": "success",
                }
            )

            print(
                f"{dataset_name}: bronze={len(bronze_df):,}, "
                f"silver={len(valid_df):,}, reject_rule_hits={len(reject_df):,}"
            )

        audit_df = pd.DataFrame(audit_records)

        if reject_frames:
            rejects_df = pd.concat(reject_frames, ignore_index=True)
        else:
            rejects_df = pd.DataFrame(
                columns=[
                    "dataset_name",
                    "rule_name",
                    "severity",
                    "rejected_at_utc",
                    "record_payload_json",
                ]
            )

        write_table_and_parquet(
            connection=connection,
            table_name="silver_transform_audit",
            df=audit_df,
            parquet_file=SILVER_PATH / "silver_transform_audit.parquet",
        )

        write_table_and_parquet(
            connection=connection,
            table_name="silver_data_quality_rejects",
            df=rejects_df,
            parquet_file=SILVER_PATH / "silver_data_quality_rejects.parquet",
        )

        print("\nSilver tables now available:")
        tables = connection.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name LIKE 'silver_%'
            ORDER BY table_name
        """).fetchall()

        for table in tables:
            print(f"- {table[0]}")

    print("\nSilver transformation completed successfully.")


if __name__ == "__main__":
    main()
