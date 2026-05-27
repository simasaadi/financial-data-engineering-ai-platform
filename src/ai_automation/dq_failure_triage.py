"""
AI-assisted data quality triage module.

This module reads data quality results from the gold dashboard and rejected-record
table, then creates an auditable triage output.

It does not use real customer data or make autonomous production changes.
Instead, it simulates an enterprise-safe AI/decision-engine pattern:

- classify issue severity
- summarize likely issue
- recommend owner/team
- recommend next action
- generate an issue-log style output
- write results back to DuckDB and CSV

This is designed to demonstrate AI-ready automation for regulated data environments.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


DUCKDB_PATH = Path("data/financial_platform.duckdb")
METADATA_PATH = Path("data/metadata")
METADATA_PATH.mkdir(parents=True, exist_ok=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def assign_owner(dataset_name: str) -> str:
    owner_map = {
        "customers": "customer_data_steward",
        "accounts": "core_banking_data_owner",
        "products": "product_reference_data_steward",
        "transactions": "payments_data_engineering_team",
        "customer_service_notes": "customer_operations_data_steward",
        "exchange_rates_api": "external_reference_data_owner",
        "fraud_alert_events": "fraud_analytics_data_owner",
    }

    return owner_map.get(dataset_name, "enterprise_data_governance_team")


def classify_triage_status(
    critical_hits: int,
    high_hits: int,
    total_hits: int,
    acceptance_rate: float,
) -> str:
    if critical_hits >= 10 or acceptance_rate < 0.98:
        return "escalate_immediately"

    if critical_hits > 0 or high_hits > 0:
        return "investigate_this_sprint"

    if total_hits > 0:
        return "monitor"

    return "no_action_required"


def assign_priority(
    critical_hits: int,
    high_hits: int,
    total_hits: int,
    acceptance_rate: float,
) -> str:
    if critical_hits >= 10 or acceptance_rate < 0.98:
        return "p1_high"

    if critical_hits > 0 or high_hits > 0:
        return "p2_medium"

    if total_hits > 0:
        return "p3_low"

    return "p4_none"


def generate_plain_language_summary(row: pd.Series, top_rules: list[str]) -> str:
    dataset_name = row["dataset_name"]
    bronze_count = int(row["bronze_row_count"])
    silver_count = int(row["silver_row_count"])
    total_hits = int(row["total_reject_rule_hits"])
    acceptance_rate = float(row["acceptance_rate"])

    if total_hits == 0:
        return (
            f"{dataset_name} passed the current data quality checks. "
            f"{silver_count:,} of {bronze_count:,} records were accepted into silver."
        )

    rules_text = ", ".join(top_rules[:3]) if top_rules else "unspecified validation rules"

    return (
        f"{dataset_name} had {total_hits:,} data quality rule hits. "
        f"{silver_count:,} of {bronze_count:,} records were accepted into silver "
        f"for an acceptance rate of {acceptance_rate:.2%}. "
        f"The main issue patterns were: {rules_text}."
    )


def generate_recommended_action(row: pd.Series, top_rules: list[str]) -> str:
    dataset_name = row["dataset_name"]
    critical_hits = int(row["critical_reject_rule_hits"])
    high_hits = int(row["high_reject_rule_hits"])
    total_hits = int(row["total_reject_rule_hits"])

    if total_hits == 0:
        return "Continue monitoring through scheduled pipeline checks."

    if dataset_name == "transactions":
        return (
            "Review rejected transaction records, confirm source-system rules for required IDs, "
            "positive amounts, duplicate transaction IDs, and CAD currency standardization. "
            "Open a data quality issue with the payments data engineering team and confirm whether "
            "upstream validation or downstream quarantine logic should be adjusted."
        )

    if critical_hits > 0:
        return (
            "Create a data quality issue, assign an accountable data owner, inspect rejected records, "
            "and confirm whether the rule failure is caused by upstream source quality, ingestion logic, "
            "or transformation logic."
        )

    if high_hits > 0:
        return (
            "Review the affected records during the next sprint and decide whether source-system "
            "validation or transformation rules need adjustment."
        )

    return "Monitor trend over time and include in the next data quality review."


def get_top_rules(connection: duckdb.DuckDBPyConnection, dataset_name: str) -> list[str]:
    query = """
        SELECT
            rule_name,
            COUNT(*) AS rule_hits
        FROM silver_data_quality_rejects
        WHERE dataset_name = ?
        GROUP BY rule_name
        ORDER BY rule_hits DESC, rule_name
        LIMIT 5
    """

    rules = connection.execute(query, [dataset_name]).fetchall()
    return [f"{rule_name} ({rule_hits} hits)" for rule_name, rule_hits in rules]


def build_triage_outputs(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    dq_dashboard = connection.execute("""
        SELECT
            dataset_name,
            bronze_table,
            silver_table,
            bronze_row_count,
            silver_row_count,
            acceptance_rate,
            critical_reject_rule_hits,
            high_reject_rule_hits,
            medium_reject_rule_hits,
            total_reject_rule_hits,
            processed_at_utc,
            status
        FROM gold_data_quality_dashboard
        ORDER BY total_reject_rule_hits DESC, dataset_name
    """).fetchdf()

    triage_records = []

    for _, row in dq_dashboard.iterrows():
        dataset_name = row["dataset_name"]
        top_rules = get_top_rules(connection, dataset_name)

        critical_hits = int(row["critical_reject_rule_hits"])
        high_hits = int(row["high_reject_rule_hits"])
        total_hits = int(row["total_reject_rule_hits"])
        acceptance_rate = float(row["acceptance_rate"])

        triage_records.append(
            {
                "issue_id": f"DQ-{dataset_name.upper().replace('_', '-')}",
                "dataset_name": dataset_name,
                "bronze_table": row["bronze_table"],
                "silver_table": row["silver_table"],
                "acceptance_rate": round(acceptance_rate, 4),
                "critical_reject_rule_hits": critical_hits,
                "high_reject_rule_hits": high_hits,
                "medium_reject_rule_hits": int(row["medium_reject_rule_hits"]),
                "total_reject_rule_hits": total_hits,
                "top_failed_rules": "; ".join(top_rules) if top_rules else "none",
                "priority": assign_priority(
                    critical_hits=critical_hits,
                    high_hits=high_hits,
                    total_hits=total_hits,
                    acceptance_rate=acceptance_rate,
                ),
                "triage_status": classify_triage_status(
                    critical_hits=critical_hits,
                    high_hits=high_hits,
                    total_hits=total_hits,
                    acceptance_rate=acceptance_rate,
                ),
                "recommended_owner": assign_owner(dataset_name),
                "plain_language_summary": generate_plain_language_summary(row, top_rules),
                "recommended_action": generate_recommended_action(row, top_rules),
                "created_at_utc": utc_now(),
            }
        )

    return pd.DataFrame(triage_records)


def write_outputs(connection: duckdb.DuckDBPyConnection, triage_df: pd.DataFrame) -> None:
    connection.register("triage_df", triage_df)

    connection.execute("""
        CREATE OR REPLACE TABLE ai_pipeline_triage AS
        SELECT * FROM triage_df
    """)

    connection.unregister("triage_df")

    csv_path = METADATA_PATH / "ai_pipeline_triage.csv"
    parquet_path = METADATA_PATH / "ai_pipeline_triage.parquet"

    triage_df.to_csv(csv_path, index=False)

    parquet_path_sql = str(parquet_path).replace("\\", "/")

    connection.execute(f"""
        COPY ai_pipeline_triage
        TO '{parquet_path_sql}'
        (FORMAT PARQUET)
    """)

    issue_log = triage_df[
        [
            "issue_id",
            "dataset_name",
            "priority",
            "triage_status",
            "recommended_owner",
            "plain_language_summary",
            "recommended_action",
            "created_at_utc",
        ]
    ].copy()

    issue_log.to_csv(METADATA_PATH / "governance_issue_log.csv", index=False)


def main() -> None:
    print("Starting AI-assisted data quality triage...")

    with duckdb.connect(str(DUCKDB_PATH)) as connection:
        triage_df = build_triage_outputs(connection)
        write_outputs(connection, triage_df)

        print("\nAI triage output:")
        print(
            triage_df[
                [
                    "issue_id",
                    "dataset_name",
                    "priority",
                    "triage_status",
                    "recommended_owner",
                    "total_reject_rule_hits",
                ]
            ].to_string(index=False)
        )

    print("\nAI-assisted data quality triage completed successfully.")


if __name__ == "__main__":
    main()
