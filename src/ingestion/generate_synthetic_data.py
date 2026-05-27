"""
Generate synthetic financial datasets for a production-style data engineering platform.

This script creates fake banking data only. No real customer or financial data is used.

Generated datasets:
- customers.csv
- accounts.csv
- products.csv
- transactions.csv
- customer_service_notes.csv
- exchange_rates_api.json
- fraud_alert_events.jsonl
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker


SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("en_CA")
Faker.seed(SEED)

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)


def random_date(start_days_ago: int, end_days_ago: int = 0) -> datetime:
    """Return a random datetime between two relative day offsets."""
    days_ago = random.randint(end_days_ago, start_days_ago)
    return datetime.now() - timedelta(days=days_ago)


def generate_customers(n: int = 1000) -> pd.DataFrame:
    records = []

    provinces = ["ON", "QC", "BC", "AB", "MB", "NS", "NB", "SK"]
    segments = ["retail", "newcomer", "student", "small_business", "premium"]
    risk_tiers = ["low", "medium", "high"]

    for i in range(1, n + 1):
        customer_id = f"CUST{i:06d}"
        created_at = random_date(2200, 30)

        records.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "province": random.choice(provinces),
                "customer_segment": random.choices(
                    segments,
                    weights=[0.55, 0.12, 0.13, 0.10, 0.10],
                    k=1,
                )[0],
                "risk_tier": random.choices(
                    risk_tiers,
                    weights=[0.72, 0.22, 0.06],
                    k=1,
                )[0],
                "created_at": created_at.strftime("%Y-%m-%d"),
                "is_active": random.choices([True, False], weights=[0.92, 0.08], k=1)[0],
            }
        )

    return pd.DataFrame(records)


def generate_products() -> pd.DataFrame:
    products = [
        ("PRD001", "chequing_account", "deposit", "core_banking"),
        ("PRD002", "savings_account", "deposit", "core_banking"),
        ("PRD003", "credit_card_cashback", "credit_card", "cards"),
        ("PRD004", "credit_card_travel", "credit_card", "cards"),
        ("PRD005", "personal_loan", "loan", "lending"),
        ("PRD006", "line_of_credit", "loan", "lending"),
        ("PRD007", "mortgage", "loan", "lending"),
        ("PRD008", "investment_account", "investment", "wealth"),
    ]

    return pd.DataFrame(
        products,
        columns=["product_id", "product_name", "product_type", "business_domain"],
    )


def generate_accounts(customers: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    records = []
    account_counter = 1
    deposit_products = products[products["product_type"].isin(["deposit", "credit_card", "loan"])]

    for _, customer in customers.iterrows():
        number_of_accounts = random.choices([1, 2, 3, 4], weights=[0.35, 0.40, 0.18, 0.07], k=1)[0]

        selected_products = deposit_products.sample(
            n=min(number_of_accounts, len(deposit_products)),
            replace=False,
            random_state=random.randint(1, 999999),
        )

        for _, product in selected_products.iterrows():
            account_id = f"ACCT{account_counter:07d}"
            opened_at = random_date(1800, 5)
            balance = round(float(np.random.normal(6500, 12500)), 2)

            if product["product_type"] in ["credit_card", "loan"]:
                balance = round(abs(balance), 2)

            records.append(
                {
                    "account_id": account_id,
                    "customer_id": customer["customer_id"],
                    "product_id": product["product_id"],
                    "account_status": random.choices(
                        ["active", "dormant", "closed"],
                        weights=[0.86, 0.08, 0.06],
                        k=1,
                    )[0],
                    "opened_at": opened_at.strftime("%Y-%m-%d"),
                    "current_balance": balance,
                    "currency": "CAD",
                }
            )

            account_counter += 1

    return pd.DataFrame(records)


def generate_transactions(accounts: pd.DataFrame, n: int = 12000) -> pd.DataFrame:
    records = []

    transaction_types = ["purchase", "deposit", "withdrawal", "transfer", "payment", "fee"]
    channels = ["mobile_app", "web", "branch", "atm", "point_of_sale", "api"]
    merchant_categories = [
        "grocery",
        "transportation",
        "utilities",
        "restaurant",
        "travel",
        "health",
        "retail",
        "financial_services",
    ]

    active_accounts = accounts[accounts["account_status"] == "active"]

    for i in range(1, n + 1):
        account = active_accounts.sample(n=1, random_state=random.randint(1, 999999)).iloc[0]
        transaction_type = random.choices(
            transaction_types,
            weights=[0.38, 0.14, 0.11, 0.17, 0.15, 0.05],
            k=1,
        )[0]

        if transaction_type in ["deposit", "payment"]:
            amount = round(abs(float(np.random.normal(1200, 900))), 2)
        elif transaction_type == "fee":
            amount = round(abs(float(np.random.normal(9, 5))), 2)
        else:
            amount = round(abs(float(np.random.normal(110, 230))), 2)

        transaction_ts = random_date(365, 0)

        records.append(
            {
                "transaction_id": f"TXN{i:09d}",
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "transaction_timestamp": transaction_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_type": transaction_type,
                "channel": random.choice(channels),
                "merchant_category": random.choice(merchant_categories),
                "amount": amount,
                "currency": "CAD",
                "is_international": random.choices([True, False], weights=[0.07, 0.93], k=1)[0],
                "ingestion_source": random.choice(["batch_core_banking", "card_event_stream", "payments_api"]),
            }
        )

    transactions = pd.DataFrame(records)

    # Intentionally inject a few data quality issues for testing.
    issue_indices = transactions.sample(n=25, random_state=SEED).index
    transactions.loc[issue_indices[:8], "customer_id"] = None
    transactions.loc[issue_indices[8:14], "amount"] = -1
    transactions.loc[issue_indices[14:20], "currency"] = "UNKNOWN"

    duplicated_rows = transactions.sample(n=5, random_state=SEED + 1)
    transactions = pd.concat([transactions, duplicated_rows], ignore_index=True)

    return transactions


def generate_customer_service_notes(customers: pd.DataFrame, n: int = 300) -> pd.DataFrame:
    issue_types = [
        "card dispute",
        "address change",
        "login problem",
        "payment delay",
        "credit limit question",
        "fraud concern",
        "account closure request",
    ]

    records = []

    for i in range(1, n + 1):
        customer = customers.sample(n=1, random_state=random.randint(1, 999999)).iloc[0]
        issue_type = random.choice(issue_types)
        created_at = random_date(180, 0)

        records.append(
            {
                "case_id": f"CASE{i:06d}",
                "customer_id": customer["customer_id"],
                "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "issue_type": issue_type,
                "priority": random.choices(["low", "medium", "high"], weights=[0.55, 0.35, 0.10], k=1)[0],
                "case_note": fake.sentence(nb_words=18),
                "case_status": random.choice(["open", "in_progress", "resolved", "closed"]),
            }
        )

    return pd.DataFrame(records)


def generate_exchange_rates_api() -> list[dict]:
    base_date = datetime.now().date()
    currencies = {
        "USD": 1.36,
        "EUR": 1.48,
        "GBP": 1.73,
        "CAD": 1.00,
    }

    records = []

    for day_offset in range(0, 30):
        rate_date = base_date - timedelta(days=day_offset)

        for currency, base_rate in currencies.items():
            records.append(
                {
                    "rate_date": str(rate_date),
                    "base_currency": "CAD",
                    "target_currency": currency,
                    "exchange_rate": round(base_rate + random.uniform(-0.025, 0.025), 4),
                    "source_system": "synthetic_exchange_rate_api",
                    "loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    return records


def generate_fraud_alert_events(transactions: pd.DataFrame, n: int = 500) -> list[dict]:
    sampled_transactions = transactions.sample(n=n, random_state=SEED + 2)

    alert_reasons = [
        "unusual_amount",
        "new_location",
        "velocity_check",
        "international_transaction",
        "merchant_risk_pattern",
    ]

    events = []

    for i, (_, txn) in enumerate(sampled_transactions.iterrows(), start=1):
        risk_score = round(random.uniform(0.05, 0.99), 3)

        events.append(
            {
                "event_id": f"EVT{i:07d}",
                "transaction_id": txn["transaction_id"],
                "customer_id": txn["customer_id"],
                "account_id": txn["account_id"],
                "event_timestamp": txn["transaction_timestamp"],
                "risk_score": risk_score,
                "alert_reason": random.choice(alert_reasons),
                "recommended_action": "review" if risk_score >= 0.75 else "monitor",
                "event_source": "synthetic_fraud_stream",
            }
        )

    return events


def main() -> None:
    print("Generating synthetic financial datasets...")

    customers = generate_customers()
    products = generate_products()
    accounts = generate_accounts(customers, products)
    transactions = generate_transactions(accounts)
    service_notes = generate_customer_service_notes(customers)
    exchange_rates = generate_exchange_rates_api()
    fraud_events = generate_fraud_alert_events(transactions)

    customers.to_csv(RAW_PATH / "customers.csv", index=False)
    products.to_csv(RAW_PATH / "products.csv", index=False)
    accounts.to_csv(RAW_PATH / "accounts.csv", index=False)
    transactions.to_csv(RAW_PATH / "transactions.csv", index=False)
    service_notes.to_csv(RAW_PATH / "customer_service_notes.csv", index=False)

    with open(RAW_PATH / "exchange_rates_api.json", "w", encoding="utf-8") as f:
        json.dump(exchange_rates, f, indent=2)

    with open(RAW_PATH / "fraud_alert_events.jsonl", "w", encoding="utf-8") as f:
        for event in fraud_events:
            f.write(json.dumps(event) + "\n")

    print("Synthetic data generated successfully.")
    print(f"Customers: {len(customers):,}")
    print(f"Accounts: {len(accounts):,}")
    print(f"Products: {len(products):,}")
    print(f"Transactions: {len(transactions):,}")
    print(f"Customer service notes: {len(service_notes):,}")
    print(f"Exchange rate records: {len(exchange_rates):,}")
    print(f"Fraud alert events: {len(fraud_events):,}")


if __name__ == "__main__":
    main()
