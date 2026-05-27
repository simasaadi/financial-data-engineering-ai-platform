"""
Raw-to-bronze ingestion pipeline.

This script loads synthetic financial raw files into DuckDB bronze tables.
It preserves raw fields and adds ingestion metadata for traceability.

Bronze layer purpose:
- Store source-aligned data
- Add ingestion timestamp
- Add source file name
- Add load ID
- Add row-level hash for audit and duplicate investigation
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


RAW_PATH = Path("data/raw")
BRONZE_PATH = Path("data/bronze")
DUCKDB_PATH = Path("data/financial_platform.duckdb")

BRONZE_PATH.mkdir(parents=True, exist_ok=True)
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)


DATASETS = [
    {
        "name": "customers",
        "file": RAW_PATH / "customers.csv",
        "format": "csv",
        "source_system": "core_customer_platform",
    },
    {
        "name": "accounts",
        "file": RAW_PATH / "accounts.csv",
        "format": "csv",
        "source_system": "core_banking_platform",
    },
    {
        "name": "products",
        "file": RAW_PATH / "products.csv",
        "format": "csv",
        "source_system": "product_reference_platform",
    },
    {
        "name": "transactions",
        "file": RAW_PATH / "transactions.csv",
        "format": "csv",
        "source_system": "payments_and_card_platforms",
    },
    {
        "name": "customer_service_notes",
        "file": RAW_PATH / "customer_service_notes.csv",
        "format": "csv",
        "source_system": "customer_service_platform",
    },
    {
        "name": "exchange_rates_api",
        "file": RAW_PATH / "exchange_rates_api.json",
        "format": "json",
        "source_system": "external_exchange_rate_api",
    },
    {
        "name": "fraud_alert_events",
        "file": RAW_PATH / "fraud_alert_events.jsonl",
        "format": "jsonl",
        "source_system": "fraud_event_stream",
    },
]


def read_dataset(file_path: Path, file_format: str) -> pd.DataFrame:
    """Read a raw dataset from CSV, JSON, or JSONL."""
    if not file_path.exists():
        raise FileNotFoundError(f"Missing raw file: {file_path}")

    if file_format == "csv":
        return pd.read_csv(file_path)

    if file_format == "json":
        with open(file_path, "r", encoding="utf-8") as file:
            records = json.load(file)
        return pd.DataFrame(records)

    if file_format == "jsonl":
        records = []
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    records.append(json.loads(line))
        return pd.DataFrame(records)

    raise ValueError(f"Unsupported file format: {file_format}")


def add_bronze_metadata(
    df: pd.DataFrame,
    source_file: Path,
    source_system: str,
    load_id: str,
) -> pd.DataFrame:
    """Add ingestion metadata columns to a bronze dataframe."""
    bronze_df = df.copy()

    bronze_df["_bronze_load_id"] = load_id
    bronze_df["_ingested_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    bronze_df["_source_file"] = source_file.name
    bronze_df["_source_system"] = source_system

    row_hash_values = pd.util.hash_pandas_object(
        bronze_df.astype(str),
        index=False,
    )

    bronze_df["_raw_row_hash"] = row_hash_values.astype(str)

    return bronze_df


def write_bronze_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
) -> None:
    """Write dataframe to DuckDB bronze table and export to Parquet."""
    bronze_table_name = f"bronze_{table_name}"

    connection.register("incoming_bronze_df", df)

    connection.execute(f"""
        CREATE OR REPLACE TABLE {bronze_table_name} AS
        SELECT * FROM incoming_bronze_df
    """)

    parquet_path = BRONZE_PATH / f"{bronze_table_name}.parquet"
    parquet_path_sql = str(parquet_path).replace("\\", "/")

    connection.execute(f"""
        COPY {bronze_table_name}
        TO '{parquet_path_sql}'
        (FORMAT PARQUET)
    """)

    connection.unregister("incoming_bronze_df")


def write_load_audit(
    connection: duckdb.DuckDBPyConnection,
    audit_records: list[dict],
) -> None:
    """Write bronze load audit records."""
    audit_df = pd.DataFrame(audit_records)

    connection.register("bronze_load_audit_df", audit_df)

    connection.execute("""
        CREATE OR REPLACE TABLE bronze_load_audit AS
        SELECT * FROM bronze_load_audit_df
    """)

    audit_path = BRONZE_PATH / "bronze_load_audit.parquet"
    audit_path_sql = str(audit_path).replace("\\", "/")

    connection.execute(f"""
        COPY bronze_load_audit
        TO '{audit_path_sql}'
        (FORMAT PARQUET)
    """)

    connection.unregister("bronze_load_audit_df")


def main() -> None:
    load_id = str(uuid.uuid4())
    audit_records = []

    print("Starting raw-to-bronze ingestion...")
    print(f"Load ID: {load_id}")

    with duckdb.connect(str(DUCKDB_PATH)) as connection:
        for dataset in DATASETS:
            dataset_name = dataset["name"]
            file_path = dataset["file"]
            file_format = dataset["format"]
            source_system = dataset["source_system"]

            print(f"\nLoading {dataset_name} from {file_path}...")

            raw_df = read_dataset(file_path, file_format)
            bronze_df = add_bronze_metadata(
                raw_df,
                source_file=file_path,
                source_system=source_system,
                load_id=load_id,
            )

            write_bronze_table(connection, dataset_name, bronze_df)

            audit_records.append(
                {
                    "bronze_load_id": load_id,
                    "dataset_name": dataset_name,
                    "source_file": file_path.name,
                    "source_system": source_system,
                    "file_format": file_format,
                    "row_count": len(bronze_df),
                    "column_count": len(bronze_df.columns),
                    "loaded_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "success",
                }
            )

            print(f"Loaded bronze_{dataset_name}: {len(bronze_df):,} rows")

        write_load_audit(connection, audit_records)

        print("\nBronze tables created in DuckDB:")
        tables = connection.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name LIKE 'bronze_%'
            ORDER BY table_name
        """).fetchall()

        for table in tables:
            print(f"- {table[0]}")

    print("\nRaw-to-bronze ingestion completed successfully.")


if __name__ == "__main__":
    main()
