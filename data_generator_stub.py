"""
data_generator.py

This module defines the `DataGenerator` class for generating synthetic datasets
based on a provided YAML schema. It supports the creation of:

- Customer profiles
- Support tickets
- Credit card billing records
- Transactions (JSON format)
- Loan applications (TXT format)

The generated data is saved to structured files in directories organized by date and hour.

Dependencies:
- pandas
- numpy
- faker
- yaml
- json
- pathlib
- re
- logging
"""

import pandas as pd
import numpy as np
import random
import faker
import json
from pathlib import Path
import re
import logging
from datetime import datetime

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataGenerator:
    """
    Generates synthetic data files for simulation or testing purposes
    based on a given schema.

    Attributes:
        _schema (dict): Loaded schema defining structure of each dataset.
        _faker (Faker): Faker instance used for synthetic data generation.
        _profiles_df (DataFrame): Generated customer profiles used across other datasets.
        _output_path (Path): Path where generated data will be stored.
    """

    def __init__(self, schema: dict):
        """
        Initialize the data generator with a given schema.

        Args:
            schema (dict): The full schema used to guide data generation.
        """
        self._schema: dict[str, tuple[str]] = schema
        self._faker: faker.Faker = faker.Faker()
        self._profiles_df: pd.DataFrame = None
        self._output_path: Path = Path(".")
        logger.info("DataGenerator initialized with provided schema")

    def _generate_customer_profiles(self, count: int = 100000) -> None:
        """
        Generate fake customer profiles and save them as a CSV file.

        Args:
            count (int): Number of customer profiles to generate.
        """
        logger.info(f"Starting customer profiles generation for {count} records")
        try:
            schema: dict = self._schema["customer_profiles"]
            profiles = {k: [] for k in schema.keys()}
            for i in range(1, count + 1):
                profiles["name"].append(self._faker.name())
                profiles["gender"].append(random.choice(schema["gender"]["enum"]))
                profiles["age"].append(random.randint(*schema["age"]["range"]))
                profiles["city"].append(random.choice(schema["city"]["list"]))
                account_open_date = self._faker.date_between(start_date="-10y", end_date="-1y")
                profiles["account_open_date"].append(account_open_date)
                profiles["product_type"].append(random.choice(schema["product_type"]["enum"]))
                profiles["customer_tier"].append(random.choice(schema["customer_tier"]["enum"]))
                profiles["customer_id"].append(schema["customer_id"]["format"].format(i=i))

            self._profiles_df = pd.DataFrame(profiles)
            filepath = self._output_path / "customer_profiles.csv"
            self._profiles_df.to_csv(filepath, index=False)
            logger.info(f"Successfully generated customer profiles at {filepath}")
        except Exception as e:
            logger.error(f"Error generating customer profiles: {str(e)}")
            raise

    def _generate_support_tickets(self, count: int = 15000) -> None:
        """
        Generate fake customer support tickets and save them as a CSV file.

        Args:
            count (int): Number of tickets to generate.
        """
        logger.info(f"Starting support tickets generation for {count} records")
        try:
            schema: dict = self._schema["support_tickets"]
            support_tickets = {k: [] for k in schema.keys()}
            sampled_customers = random.sample(self._profiles_df["customer_id"].tolist(), count)

            for i, cust_id in enumerate(sampled_customers, start=1):
                support_tickets["ticket_id"].append(schema["ticket_id"]["format"].format(i=i))
                support_tickets["customer_id"].append(cust_id)
                support_tickets["complaint_category"].append(random.choice(schema["complaint_category"]["enum"]))
                support_tickets["complaint_date"].append(self._faker.date_between(start_date="-1y", end_date="today"))
                support_tickets["severity"].append(random.randint(0, 10))

            support_tickets_df = pd.DataFrame(support_tickets)
            filepath = self._output_path / "support_tickets.csv"
            support_tickets_df.to_csv(filepath, index=False)
            logger.info(f"Successfully generated support tickets at {filepath}")
        except Exception as e:
            logger.error(f"Error generating support tickets: {str(e)}")
            raise

    def _generate_credit_cards_billing(self, count: int = 2) -> None:
        """
        Generate fake credit card billing records and save as a CSV file.

        Args:
            count (int): Number of monthly billing records per customer.
        """
        logger.info(f"Starting credit card billing generation for {count} months per customer")
        try:
            schema: dict = self._schema["credit_cards_billing"]
            credit_cards_billing = {k: [] for k in schema.keys()}

            for cust_id in self._profiles_df["customer_id"]:
                for month_offset in range(count):
                    bill_month = pd.Timestamp("2023-01-01") + pd.DateOffset(months=month_offset)
                    amount_due = round(random.uniform(10, 300), 2)
                    payment_delay_days = random.choice([0, 0, 0, 1, 2, 5, 7])
                    amount_paid = amount_due if payment_delay_days <= 5 else round(amount_due * random.uniform(0.8, 1.0), 2)
                    payment_date = (bill_month + pd.DateOffset(days=payment_delay_days)).strftime(schema["payment_date"]["format"])

                    credit_cards_billing["bill_id"].append(
                        schema["bill_id"]["format"].format(i=random.randint(1000000, 9999999))
                    )
                    credit_cards_billing["customer_id"].append(cust_id)
                    credit_cards_billing["month"].append(bill_month.strftime("%Y-%m"))
                    credit_cards_billing["amount_due"].append(amount_due)
                    credit_cards_billing["amount_paid"].append(amount_paid)
                    credit_cards_billing["payment_date"].append(payment_date)

            billing_df = pd.DataFrame(credit_cards_billing)
            filepath = self._output_path / "credit_cards_billing.csv"
            billing_df.to_csv(filepath, index=False)
            logger.info(f"Successfully generated credit card billing at {filepath}")
        except Exception as e:
            logger.error(f"Error generating credit card billing: {str(e)}")
            raise

    def _generate_transactions(self) -> None:
        """
        Generate random transactions between customers and save as a JSON file.
        """
        logger.info("Starting transactions generation")
        try:
            transactions_data = []
            for customer_id in self._profiles_df["customer_id"]:
                transaction_amount = random.randint(1, 100)
                receiver = np.random.choice(self._profiles_df["customer_id"])
                transactions_data.append({
                    "sender": customer_id,
                    "receiver": receiver,
                    "transaction_amount": transaction_amount,
                    "transaction_date": str(self._faker.date_between(start_date="-1y", end_date="today")),
                })

            filepath = self._output_path / "transactions.json"
            with open(filepath, "w") as f:
                json.dump(transactions_data, f, indent=4)
            logger.info(f"Successfully generated transactions at {filepath}")
        except Exception as e:
            logger.error(f"Error generating transactions: {str(e)}")
            raise

    def _generate_loans(self) -> None:
        """
        Generate loan records and save as a pipe-delimited TXT file.
        """
        logger.info("Starting loans generation")
        try:
            schema: dict = self._schema["loans"]
            with open(schema["loan_reason"]["file"]) as fp:
                messages = fp.read().split("\n")

            entry: list[str] = []
            for _ in range(1000):
                customer_id = np.random.choice(self._profiles_df["customer_id"])
                loan_type = np.random.choice(schema["loan_type"]["enum"])
                amount_utilized = random.randint(10, 1000) * 1000
                utilization_date = str(self._faker.date_between(start_date="-1y", end_date="today"))
                loan_reason = random.choice(messages)
                entry.append(f"{customer_id}|{loan_type}|{amount_utilized}|{utilization_date}|{loan_reason}")

            with open(self._output_path / "loans.txt", "w") as fp:
                fp.write("|".join(schema.keys()) + "\n")
                fp.write("\n".join(entry))
            logger.info(f"Successfully generated loans at {self._output_path / 'loans.txt'}")
        except Exception as e:
            logger.error(f"Error generating loans: {str(e)}")
            raise

    def _set_output_path(self, output_path: str) -> None:
        """
        Set the output directory path for generated files.

        Args:
            output_path (str): Path to the output directory.
        """
        self._output_path = Path(output_path)
        self._output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output path set to: {self._output_path}")

    def generate(self, date: str, hour: int) -> bool:
        """
        Entry point to generate all datasets.

        Args:
            date (str): Generation date in format YYYY-MM-DD.
            hour (int): Hour of generation (0–24).

        Returns:
            bool: True if generation succeeded, False otherwise.
        """
        logger.info(f"Starting data generation for date={date}, hour={hour}")
        if not re.match(r"\d{4}-\d{2}-\d{2}", date) or 0 > hour > 24:
            error_msg = f"Cannot generate using date={date} hour={hour}"
            logger.error(error_msg)
            print(f"[FAIL] {error_msg}")
            return False

        try:
            self._set_output_path(f"incoming_data/{date}/{hour:02d}")

            logger.info("Generating customer profiles...")
            self._generate_customer_profiles()
            logger.info("Generating support tickets...")
            self._generate_support_tickets()
            logger.info("Generating credit cards billing...")
            self._generate_credit_cards_billing()
            logger.info("Generating transactions...")
            self._generate_transactions()
            logger.info("Generating loans...")
            self._generate_loans()

            logger.info("Data generation completed successfully")
            return True
        except Exception as e:
            logger.error(f"Data generation failed: {str(e)}")
            return False


if __name__ == "__main__":
    import yaml

    try:
        logger.info("Starting data generator script")
        with open("data/schema.yaml") as fp:
            schema = yaml.safe_load(fp)
        logger.info("Schema loaded successfully")
    except FileNotFoundError as e:
        logger.error(f"Schema file not found: {str(e)}")
        print(f"[EXIT] {e}")
        exit(1)
    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error: {str(e)}")
        print(f"[EXIT] {e}")
        exit(1)

    gen = DataGenerator(schema)
    current_datetime = datetime.now()
    current_date = current_datetime.strftime("%Y-%m-%d")
    current_hour = current_datetime.hour
    logger.info(f"Generating data for current datetime: {current_date} {current_hour}:00")
    
    result = gen.generate(current_date, current_hour)
    if result:
        logger.info("Script completed successfully")
    else:
        logger.error("Script completed with errors")
