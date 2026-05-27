"""
Tests for data quality and AI triage outputs.
"""

from pathlib import Path

import duckdb


DUCKDB_PATH = Path("data/financial_platform.duckdb")


def test_transaction_rejects_are_captured() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))

    result = con.execute(
        """
        SELECT total_reject_rule_hits
        FROM gold_data_quality_dashboard
        WHERE dataset_name = 'transactions'
        """
    ).fetchone()

    con.close()

    assert result is not None
    assert result[0] == 30


def test_transaction_acceptance_rate_is_high_but_not_perfect() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))

    acceptance_rate = con.execute(
        """
        SELECT acceptance_rate
        FROM gold_data_quality_dashboard
        WHERE dataset_name = 'transactions'
        """
    ).fetchone()[0]

    con.close()

    assert 0.99 <= acceptance_rate < 1.0


def test_ai_triage_flags_transaction_issue_as_high_priority() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))

    result = con.execute(
        """
        SELECT priority, triage_status, recommended_owner
        FROM ai_pipeline_triage
        WHERE dataset_name = 'transactions'
        """
    ).fetchone()

    con.close()

    assert result is not None
    priority, triage_status, recommended_owner = result

    assert priority == "p1_high"
    assert triage_status == "escalate_immediately"
    assert recommended_owner == "payments_data_engineering_team"


def test_no_null_customer_ids_in_ml_feature_table() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))

    null_count = con.execute(
        """
        SELECT COUNT(*)
        FROM ml_customer_risk_features
        WHERE customer_id IS NULL
        """
    ).fetchone()[0]

    con.close()

    assert null_count == 0
