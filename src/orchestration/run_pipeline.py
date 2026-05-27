"""
End-to-end pipeline orchestration.

This script runs the full synthetic financial data engineering platform:

1. Generate synthetic source data
2. Load raw files into bronze DuckDB tables
3. Transform bronze tables into standardized silver tables
4. Build gold analytics marts and ML-ready feature tables
5. Run AI-assisted data quality triage

This simulates a production-style orchestrated data pipeline.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


PIPELINE_STEPS = [
    {
        "name": "Generate synthetic financial source data",
        "script": "src/ingestion/generate_synthetic_data.py",
    },
    {
        "name": "Ingest raw data to bronze layer",
        "script": "src/ingestion/raw_to_bronze.py",
    },
    {
        "name": "Transform bronze data to silver layer",
        "script": "src/transformations/bronze_to_silver.py",
    },
    {
        "name": "Build gold marts and ML-ready feature tables",
        "script": "src/transformations/silver_to_gold.py",
    },
    {
        "name": "Run AI-assisted data quality triage",
        "script": "src/ai_automation/dq_failure_triage.py",
    },
]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def run_step(step_number: int, step: dict[str, str]) -> None:
    step_name = step["name"]
    script_path = Path(step["script"])

    if not script_path.exists():
        raise FileNotFoundError(f"Missing pipeline script: {script_path}")

    print("\n" + "=" * 80)
    print(f"STEP {step_number}: {step_name}")
    print(f"Script: {script_path}")
    print(f"Started at UTC: {utc_now()}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        text=True,
        capture_output=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline step failed: {step_name}. "
            f"Script: {script_path}. "
            f"Return code: {result.returncode}"
        )

    print(f"Completed at UTC: {utc_now()}")


def main() -> None:
    print("Starting end-to-end financial data engineering pipeline...")
    print(f"Pipeline started at UTC: {utc_now()}")

    for index, step in enumerate(PIPELINE_STEPS, start=1):
        run_step(index, step)

    print("\n" + "=" * 80)
    print("Pipeline completed successfully.")
    print(f"Pipeline finished at UTC: {utc_now()}")
    print("=" * 80)


if __name__ == "__main__":
    main()
