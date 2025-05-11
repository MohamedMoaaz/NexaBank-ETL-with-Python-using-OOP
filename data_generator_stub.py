import pandas as pd
import numpy as np
import random
import faker
import json
from pathlib import Path
import re


class DataGenerator:
    def __init__(self, schema: dict):
        self._schema: dict[str, tuple[str]] = schema
        self._faker: faker.Faker = faker.Faker()
        self._profiles_df: pd.DataFrame = None
        self._output_path: Path = Path(".")

    def _generate_customer_profiles(self, count: int = 100000) -> None:
        schema: dict = self._schema["customer_profiles"]
        profiles = {k: [] for k in schema.keys()}

        for i in range(1, count + 1):
            profiles["name"].append(self._faker.name())
            profiles["gender"].append(random.choice(schema["gender"]["enum"]))
            profiles["age"].append(random.randint(*schema["age"]["range"]))
            profiles["city"].append(random.choice(schema["city"]["list"]))
            account_open_date = self._faker.date_between(
                start_date="-10y", end_date="-1y"
            )
            profiles["account_open_date"].append(account_open_date)
            product_type = random.choice(schema["product_type"]["enum"])
            profiles["product_type"].append(product_type)
            customer_tier = random.choice(schema["customer_tier"]["enum"])
            profiles["customer_tier"].append(customer_tier)
            profiles["customer_id"].append(schema["customer_id"]["format"].format(i=i))

        self._profiles_df = pd.DataFrame(profiles)
        filepath = self._output_path / "customer_profiles.csv"
        self._profiles_df.to_csv(filepath, index=False)

    def _generate_support_tickets(self, count: int = 15000) -> None:
        schema: dict = self._schema["support_tickets"]
        support_tickets = {k: [] for k in schema.keys()}
        sampled_customers = random.sample(
            self._profiles_df["customer_id"].tolist(), count
        )
        for i, cust_id in enumerate(sampled_customers, start=1):
            support_tickets["ticket_id"].append(
                schema["ticket_id"]["format"].format(i=i)
            )
            support_tickets["customer_id"].append(cust_id)
            support_tickets["complaint_category"].append(
                random.choice(schema["complaint_category"]["enum"])
            )
            support_tickets["complaint_date"].append(
                self._faker.date_between(start_date="-1y", end_date="today")
            )
            support_tickets["severity"].append(random.randint(0, 10))

        support_tickets_df = pd.DataFrame(support_tickets)
        filepath = self._output_path / "support_tickets.csv"
        support_tickets_df.to_csv(filepath, index=False)

    def _generate_credit_cards_billing(self, count: int = 2) -> None:
        schema: dict = self._schema["credit_cards_billing"]
        credit_cards_billing = {k: [] for k in schema.keys()}

        for cust_id in self._profiles_df["customer_id"]:
            for month_offset in range(count):
                bill_month = pd.Timestamp("2023-01-01") + pd.DateOffset(
                    months=month_offset
                )
                amount_due = round(random.uniform(10, 300), 2)
                payment_delay_days = random.choice([0, 0, 0, 1, 2, 5, 7])
                amount_paid = (
                    amount_due
                    if payment_delay_days <= 5
                    else round(amount_due * random.uniform(0.8, 1.0), 2)
                )
                payment_date = (
                    bill_month + pd.DateOffset(days=payment_delay_days)
                ).strftime(schema["payment_date"]["format"])
                credit_cards_billing["bill_id"].append(
                    schema["bill_id"]["format"].format(
                        i=random.randint(1000000, 9999999)
                    )
                )
                credit_cards_billing["customer_id"].append(cust_id)
                credit_cards_billing["month"].append(bill_month.strftime("%Y-%m"))
                credit_cards_billing["amount_due"].append(amount_due)
                credit_cards_billing["amount_paid"].append(amount_paid)
                credit_cards_billing["payment_date"].append(payment_date)

        billing_df = pd.DataFrame(credit_cards_billing)
        filepath = self._output_path / "credit_cards_billing.csv"
        billing_df.to_csv(filepath, index=False)

    def _generate_transactions(self) -> None:
        transactions_data = []
        for customer_id in self._profiles_df["customer_id"]:
            transaction_amount = random.randint(1, 100)
            receiver = np.random.choice(self._profiles_df["customer_id"])
            transactions_data.append(
                {
                    "sender": customer_id,
                    "receiver": receiver,
                    "transaction_amount": transaction_amount,
                    "transaction_date": str(
                        self._faker.date_between(
                            start_date="-1y",
                            end_date="today",
                        )
                    ),
                }
            )

        filepath = self._output_path / "transactions.json"
        with open(filepath, "w") as f:
            json.dump(transactions_data, f, indent=4)

    def _generate_loans(self) -> None:
        schema: dict = self._schema["loans"]

        with open(schema["loan_reason"]["file"]) as fp:
            messages = fp.read().split("\n")

        entry: list[str] = []
        for _ in range(1000):
            customer_id = np.random.choice(self._profiles_df["customer_id"])
            loan_type = np.random.choice(schema["loan_type"]["enum"])
            amount_utilized = random.randint(10, 1000)
            amount_utilized *= 1000
            utilization_date = str(
                self._faker.date_between(start_date="-1y", end_date="today")
            )
            loan_reason = random.choice(messages)
            entry.append(
                f"{customer_id}|{loan_type}|{amount_utilized}|{utilization_date}|{loan_reason}"
            )

        with open(self._output_path / "loans.txt", "w") as fp:
            fp.write("|".join(schema.keys()) + "\n")
            fp.write("\n".join(entry))

    def _set_output_path(self, output_path: str) -> None:
        self._output_path = Path(output_path)
        self._output_path.mkdir(parents=True, exist_ok=True)

    def generate(self, date: str, hour: int) -> bool:
        if not re.match(r"\d{4}-\d{2}-\d{2}", date) or 0 > hour > 24:
            print(f"[FAIL] Cannot generate using {date=} {hour=}")
            return False

        self._set_output_path(f"incoming_data/{date}/{hour:02d}")

        print("_generate_customer_profiles()")
        self._generate_customer_profiles()
        print("_generate_support_tickets()")
        self._generate_support_tickets()
        print("_generate_credit_cards_billing()")
        self._generate_credit_cards_billing()
        print("_generate_transactions()")
        self._generate_transactions()
        print("_generate_loans()")
        self._generate_loans()

        return True


if __name__ == "__main__":
    import yaml

    try:
        with open("data/schema.yaml") as fp:
            schema = yaml.safe_load(fp)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"[EXIT] {e}")
        exit(1)

    gen = DataGenerator(schema)
    gen.generate("2025-04-18", 15)
