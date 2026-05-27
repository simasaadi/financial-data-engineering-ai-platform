"""
Streamlit monitoring dashboard for the financial data engineering platform.

The dashboard shows:
- pipeline layer outputs
- data quality dashboard
- AI-assisted triage results
- customer 360 mart
- product performance mart
- ML-ready customer risk features
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


DUCKDB_PATH = Path("data/financial_platform.duckdb")


st.set_page_config(
    page_title="Financial Data Engineering AI Platform",
    page_icon="??",
    layout="wide",
)


@st.cache_data
def load_table(table_name: str) -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            "DuckDB database not found. Run the pipeline first with: "
            "python src/orchestration/run_pipeline.py"
        )

    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as connection:
        return connection.execute(f"SELECT * FROM {table_name}").fetchdf()


@st.cache_data
def get_table_counts() -> pd.DataFrame:
    tables = [
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
        "gold_customer_360",
        "gold_daily_transaction_summary",
        "gold_product_performance",
        "gold_data_quality_dashboard",
        "ml_customer_risk_features",
        "ai_pipeline_triage",
    ]

    records = []

    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as connection:
        for table in tables:
            count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

            if table.startswith("bronze_"):
                layer = "bronze"
            elif table.startswith("silver_"):
                layer = "silver"
            elif table.startswith("gold_"):
                layer = "gold"
            elif table.startswith("ml_"):
                layer = "ml_features"
            elif table.startswith("ai_"):
                layer = "ai_automation"
            else:
                layer = "other"

            records.append(
                {
                    "layer": layer,
                    "table_name": table,
                    "row_count": count,
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
    total_transactions = int(
        table_counts.loc[
            table_counts["table_name"] == "silver_transactions",
            "row_count",
        ].iloc[0]
    )

    total_customers = int(
        table_counts.loc[
            table_counts["table_name"] == "gold_customer_360",
            "row_count",
        ].iloc[0]
    )

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
        dq_dashboard = load_table("gold_data_quality_dashboard")
        triage = load_table("ai_pipeline_triage")
        customer_360 = load_table("gold_customer_360")
        product_performance = load_table("gold_product_performance")
        ml_features = load_table("ml_customer_risk_features")
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
