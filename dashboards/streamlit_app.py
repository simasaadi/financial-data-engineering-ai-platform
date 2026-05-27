"""
Streamlit monitoring dashboard for the financial data engineering platform.

This deployment-safe version reads committed pipeline output files directly
from data/gold, data/ml_features, and data/metadata.

It does not need to rebuild DuckDB on Streamlit Cloud.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(
    page_title="Financial Data Engineering AI Platform",
    page_icon="??",
    layout="wide",
)


OUTPUT_FILES = {
    "gold_customer_360": Path("data/gold/gold_customer_360.parquet"),
    "gold_daily_transaction_summary": Path("data/gold/gold_daily_transaction_summary.parquet"),
    "gold_product_performance": Path("data/gold/gold_product_performance.parquet"),
    "gold_data_quality_dashboard": Path("data/gold/gold_data_quality_dashboard.parquet"),
    "ml_customer_risk_features": Path("data/ml_features/ml_customer_risk_features.parquet"),
    "ai_pipeline_triage": Path("data/metadata/ai_pipeline_triage.csv"),
}


LAYER_FILES = {
    "bronze": [
        Path("data/bronze/bronze_customers.parquet"),
        Path("data/bronze/bronze_accounts.parquet"),
        Path("data/bronze/bronze_products.parquet"),
        Path("data/bronze/bronze_transactions.parquet"),
        Path("data/bronze/bronze_customer_service_notes.parquet"),
        Path("data/bronze/bronze_exchange_rates_api.parquet"),
        Path("data/bronze/bronze_fraud_alert_events.parquet"),
    ],
    "silver": [
        Path("data/silver/silver_customers.parquet"),
        Path("data/silver/silver_accounts.parquet"),
        Path("data/silver/silver_products.parquet"),
        Path("data/silver/silver_transactions.parquet"),
        Path("data/silver/silver_customer_service_notes.parquet"),
        Path("data/silver/silver_exchange_rates_api.parquet"),
        Path("data/silver/silver_fraud_alert_events.parquet"),
    ],
    "gold": [
        Path("data/gold/gold_customer_360.parquet"),
        Path("data/gold/gold_daily_transaction_summary.parquet"),
        Path("data/gold/gold_product_performance.parquet"),
        Path("data/gold/gold_data_quality_dashboard.parquet"),
    ],
    "ml_features": [
        Path("data/ml_features/ml_customer_risk_features.parquet"),
    ],
    "ai_automation": [
        Path("data/metadata/ai_pipeline_triage.csv"),
    ],
}


def parquet_row_count(file_path: Path) -> int:
    path = str(file_path).replace("\\", "/")
    return duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]


@st.cache_data
def load_parquet(file_path: str) -> pd.DataFrame:
    path = str(file_path).replace("\\", "/")
    return duckdb.sql(f"SELECT * FROM read_parquet('{path}')").df()


@st.cache_data
def load_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


@st.cache_data
def load_output_table(table_name: str) -> pd.DataFrame:
    file_path = OUTPUT_FILES[table_name]

    if not file_path.exists():
        raise FileNotFoundError(
            f"Required dashboard file is missing: {file_path}. "
            "Run the pipeline locally and commit the generated outputs."
        )

    if file_path.suffix == ".parquet":
        return load_parquet(str(file_path))

    if file_path.suffix == ".csv":
        return load_csv(str(file_path))

    raise ValueError(f"Unsupported dashboard file type: {file_path}")


@st.cache_data
def get_table_counts() -> pd.DataFrame:
    records = []

    for layer, files in LAYER_FILES.items():
        for file_path in files:
            if not file_path.exists():
                continue

            if file_path.suffix == ".parquet":
                row_count = parquet_row_count(file_path)
            elif file_path.suffix == ".csv":
                row_count = len(pd.read_csv(file_path))
            else:
                row_count = 0

            records.append(
                {
                    "layer": layer,
                    "table_name": file_path.stem,
                    "row_count": row_count,
                }
            )

    return pd.DataFrame(records)


def show_header() -> None:
    st.title("Financial Data Engineering AI Platform")
    st.caption(
        "Synthetic financial data pipeline with bronze/silver/gold layers, "
        "data quality controls, ML-ready features, and AI-assisted triage."
    )


def show_kpis(table_counts: pd.DataFrame, dq_dashboard: pd.DataFrame, ml_features: pd.DataFrame) -> None:
    silver_transactions = table_counts.loc[
        table_counts["table_name"] == "silver_transactions",
        "row_count",
    ]

    gold_customers = table_counts.loc[
        table_counts["table_name"] == "gold_customer_360",
        "row_count",
    ]

    total_transactions = int(silver_transactions.iloc[0]) if not silver_transactions.empty else 0
    total_customers = int(gold_customers.iloc[0]) if not gold_customers.empty else 0
    total_rejects = int(dq_dashboard["total_reject_rule_hits"].sum())
    high_risk_customers = int(ml_features["synthetic_high_risk_label"].sum())

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Silver transactions", f"{total_transactions:,}")
    col2.metric("Customer 360 records", f"{total_customers:,}")
    col3.metric("DQ rule hits", f"{total_rejects:,}")
    col4.metric("Synthetic high-risk customers", f"{high_risk_customers:,}")


def show_pipeline_overview(table_counts: pd.DataFrame) -> None:
    st.subheader("Pipeline layer outputs")

    layer_summary = (
        table_counts
        .groupby("layer", as_index=False)
        .agg(
            table_count=("table_name", "count"),
            total_rows=("row_count", "sum"),
        )
        .sort_values("layer")
    )

    col1, col2 = st.columns([1, 2])

    with col1:
        st.dataframe(layer_summary, use_container_width=True)

    with col2:
        fig = px.bar(
            layer_summary,
            x="layer",
            y="total_rows",
            text="total_rows",
            title="Rows by pipeline layer",
        )
        st.plotly_chart(fig, use_container_width=True)


def show_data_quality(dq_dashboard: pd.DataFrame, triage: pd.DataFrame) -> None:
    st.subheader("Data quality monitoring")

    dq_display = dq_dashboard.copy()
    dq_display["acceptance_rate_pct"] = (dq_display["acceptance_rate"] * 100).round(2)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.dataframe(
            dq_display[
                [
                    "dataset_name",
                    "bronze_row_count",
                    "silver_row_count",
                    "acceptance_rate_pct",
                    "critical_reject_rule_hits",
                    "high_reject_rule_hits",
                    "total_reject_rule_hits",
                ]
            ],
            use_container_width=True,
        )

    with col2:
        fig = px.bar(
            dq_display,
            x="dataset_name",
            y="total_reject_rule_hits",
            title="Data quality rule hits by dataset",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("AI-assisted pipeline triage")

    st.dataframe(
        triage[
            [
                "issue_id",
                "dataset_name",
                "priority",
                "triage_status",
                "recommended_owner",
                "total_reject_rule_hits",
                "plain_language_summary",
                "recommended_action",
            ]
        ],
        use_container_width=True,
    )


def show_gold_marts(customer_360: pd.DataFrame, product_performance: pd.DataFrame) -> None:
    st.subheader("Gold marts")

    tab1, tab2 = st.tabs(["Customer 360", "Product performance"])

    with tab1:
        st.dataframe(
            customer_360[
                [
                    "customer_id",
                    "province",
                    "customer_segment",
                    "risk_tier",
                    "account_count",
                    "transaction_count",
                    "total_transaction_amount",
                    "fraud_alert_count",
                    "service_case_count",
                ]
            ].head(100),
            use_container_width=True,
        )

        segment_summary = (
            customer_360
            .groupby("customer_segment", as_index=False)
            .agg(
                customers=("customer_id", "count"),
                avg_transactions=("transaction_count", "mean"),
                avg_balance=("total_current_balance", "mean"),
            )
        )

        fig = px.bar(
            segment_summary,
            x="customer_segment",
            y="customers",
            title="Customers by segment",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.dataframe(product_performance, use_container_width=True)

        fig = px.bar(
            product_performance,
            x="product_name",
            y="total_transaction_amount",
            title="Total transaction amount by product",
        )
        st.plotly_chart(fig, use_container_width=True)


def show_ml_features(ml_features: pd.DataFrame) -> None:
    st.subheader("ML-ready customer risk features")

    col1, col2 = st.columns([1, 2])

    with col1:
        risk_summary = (
            ml_features
            .groupby(["risk_tier", "synthetic_high_risk_label"], as_index=False)
            .agg(customers=("customer_id", "count"))
        )
        st.dataframe(risk_summary, use_container_width=True)

    with col2:
        fig = px.scatter(
            ml_features,
            x="transaction_count",
            y="maximum_fraud_risk_score",
            size="fraud_alert_count",
            hover_data=["customer_id", "risk_tier", "customer_segment"],
            title="Transaction activity vs. fraud risk score",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        ml_features[
            [
                "customer_id",
                "province",
                "customer_segment",
                "risk_tier",
                "transaction_count",
                "international_transaction_ratio",
                "fraud_review_ratio",
                "synthetic_high_risk_label",
            ]
        ].head(100),
        use_container_width=True,
    )


def main() -> None:
    show_header()

    try:
        table_counts = get_table_counts()
        dq_dashboard = load_output_table("gold_data_quality_dashboard")
        triage = load_output_table("ai_pipeline_triage")
        customer_360 = load_output_table("gold_customer_360")
        product_performance = load_output_table("gold_product_performance")
        ml_features = load_output_table("ml_customer_risk_features")
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    show_kpis(table_counts, dq_dashboard, ml_features)

    st.divider()

    page = st.sidebar.radio(
        "View",
        [
            "Pipeline overview",
            "Data quality and AI triage",
            "Gold marts",
            "ML-ready features",
        ],
    )

    if page == "Pipeline overview":
        show_pipeline_overview(table_counts)
    elif page == "Data quality and AI triage":
        show_data_quality(dq_dashboard, triage)
    elif page == "Gold marts":
        show_gold_marts(customer_360, product_performance)
    elif page == "ML-ready features":
        show_ml_features(ml_features)


if __name__ == "__main__":
    main()
