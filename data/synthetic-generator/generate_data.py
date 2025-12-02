import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import random
from pathlib import Path

fake = Faker()
np.random.seed(42)
random.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 500
MAX_MONTHS = 18  # how far back activity goes


def generate_customers(n=N_CUSTOMERS):
    rows = []
    today = datetime.today()
    regions = ["NA", "EU", "APAC", "LATAM"]
    segments = ["free", "basic", "pro", "enterprise"]

    for cid in range(1, n + 1):
        signup_offset = random.randint(0, MAX_MONTHS * 30)
        signup_date = today - timedelta(days=signup_offset)
        rows.append(
            {
                "customer_id": cid,
                "signup_date": signup_date.date().isoformat(),
                "region": random.choice(regions),
                "segment": random.choice(segments),
                "email": fake.email(),
                "company_name": fake.company(),
            }
        )
    return pd.DataFrame(rows)


def generate_invoices(customers_df):
    rows = []
    today = datetime.today()
    price_map = {
        "free": 0,
        "basic": 20,
        "pro": 60,
        "enterprise": 200,
    }

    invoice_id = 1
    for _, row in customers_df.iterrows():
        segment = row["segment"]
        if segment == "free":
            # maybe trial charges only
            n_invoices = np.random.choice([0, 1, 2], p=[0.7, 0.2, 0.1])
        else:
            n_invoices = random.randint(3, 15)

        # generate monthly-ish invoices
        start_date = datetime.fromisoformat(row["signup_date"])
        for i in range(n_invoices):
            invoice_date = start_date + timedelta(days=30 * i + random.randint(-3, 3))
            if invoice_date > today:
                break

            amount = price_map[segment]
            # random failures
            status = np.random.choice(["paid", "failed"], p=[0.9, 0.1])
            if segment == "free":
                amount = np.random.choice([0, 10, 20])

            rows.append(
                {
                    "invoice_id": invoice_id,
                    "customer_id": row["customer_id"],
                    "invoice_date": invoice_date.date().isoformat(),
                    "amount": float(amount),
                    "status": status,
                }
            )
            invoice_id += 1

    return pd.DataFrame(rows)


def generate_events(customers_df):
    rows = []
    event_types = ["login", "page_view", "feature_use", "upgrade_click", "support_view"]
    today = datetime.today()
    event_id = 1

    for _, row in customers_df.iterrows():
        # more active if on paid plans
        base_events = {"free": 20, "basic": 60, "pro": 120, "enterprise": 300}
        n_events = random.randint(int(0.5 * base_events[row["segment"]]),
                                  int(1.5 * base_events[row["segment"]]))

        for _ in range(n_events):
            days_ago = random.randint(0, MAX_MONTHS * 30)
            ts = today - timedelta(days=days_ago, hours=random.randint(0, 23))
            rows.append(
                {
                    "event_id": event_id,
                    "customer_id": row["customer_id"],
                    "event_type": random.choice(event_types),
                    "event_timestamp": ts.isoformat(timespec="seconds"),
                    "metadata": "{}",
                }
            )
            event_id += 1

    return pd.DataFrame(rows)


def generate_marketing(customers_df):
    rows = []
    events = ["email_sent", "email_open", "email_click"]
    today = datetime.today()
    campaign_ids = [f"CMP-{i}" for i in range(1, 8)]

    for _, row in customers_df.iterrows():
        n_events = random.randint(3, 25)
        for _ in range(n_events):
            days_ago = random.randint(0, MAX_MONTHS * 30)
            ts = today - timedelta(days=days_ago, hours=random.randint(0, 23))
            ev = np.random.choice(events, p=[0.5, 0.3, 0.2])
            rows.append(
                {
                    "campaign_id": random.choice(campaign_ids),
                    "customer_id": row["customer_id"],
                    "marketing_event": ev,
                    "event_timestamp": ts.isoformat(timespec="seconds"),
                }
            )
    return pd.DataFrame(rows)


def main():
    customers = generate_customers()
    invoices = generate_invoices(customers)
    events = generate_events(customers)
    marketing = generate_marketing(customers)

    customers.to_csv(OUTPUT_DIR / "customers.csv", index=False)
    invoices.to_csv(OUTPUT_DIR / "invoices.csv", index=False)
    events.to_csv(OUTPUT_DIR / "events.csv", index=False)
    marketing.to_csv(OUTPUT_DIR / "marketing.csv", index=False)

    print("Wrote files to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
