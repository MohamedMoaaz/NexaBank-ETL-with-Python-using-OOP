"""
transformer.py

This module defines the `Transformer` class which handles data transformation for various
financial datasets, including:

- Customer profiles
- Support tickets
- Credit card billing
- Transactions
- Loan applications

Each transformation adds derived metrics (e.g., tenure, age, cost, fine) and performs
calculations based on the date extracted from the file path.

The `transform` method automatically dispatches the appropriate transformation logic
based on the file name.
"""

import pandas as pd


class Transformer:
    """
    A utility class for transforming different types of financial data.
    Handles customer profiles, support tickets, credit card billing, transactions, and loans.
    """

    @staticmethod
    def _key(filepath: str) -> str:
        """
        Extract the dataset key (e.g., 'customer_profiles') from the filepath.

        Args:
            filepath (str): The full path to the input file.

        Returns:
            str: Lowercase dataset key for transformation dispatch.
        """
        return filepath.replace("\\", "/").split("/")[-1].split(".")[0].lower()

    @staticmethod
    def _get_timestamp(filepath: str) -> dict[str, str]:
        """
        Extract date and hour from the filepath.

        Args:
            filepath (str): A path containing the directory structure '.../YYYY-MM-DD/HH/filename.ext'

        Returns:
            dict: Dictionary with 'date' and 'hour' keys extracted from the path.
        """
        splitted = filepath.replace("\\", "/").split("/")
        return {"date": splitted[-3], "hour": splitted[-2]}

    @staticmethod
    def _transform_customer_profiles(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Transform customer profile data.

        Adds:
        - `tenure`: Years since account was opened
        - `customer_segment`: Segment classification based on tenure

        Args:
            df (pd.DataFrame): DataFrame with customer profile data.
            filepath (str): Optional file path for extracting reference date.
        """
        date = pd.to_datetime(Transformer._get_timestamp(filepath)["date"]) if filepath else pd.to_datetime("today")        
        df["tenure"] = ((date.now() - pd.to_datetime(df["account_open_date"])).dt.days / 365).astype(int)
        df["customer_segment"] = df["tenure"].apply(lambda x: "Loyal" if x > 5 else "Newcomer" if x < 1 else "Normal")
        
    @staticmethod
    def _transform_support_tickets(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Transform support ticket data by calculating ticket age.

        Adds:
        - `age`: Number of days since complaint date

        Args:
            df (pd.DataFrame): Support ticket data.
            filepath (str): Optional path for extracting reference date.
        """
        date = pd.to_datetime(Transformer._get_timestamp(filepath)["date"]) if filepath else pd.to_datetime("today")     
        df["age"] = (date.now() - pd.to_datetime(df["complaint_date"])).dt.days
        
    @staticmethod
    def _transform_credit_cards_billing(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Transform credit card billing data.

        Adds:
        - `fully_paid`: Boolean if full payment was made
        - `debt`: Amount still owed
        - `late_days`: Days of payment delay
        - `fine`: Monetary fine based on delay
        - `total_amount`: Sum of due amount and fine

        Args:
            df (pd.DataFrame): Credit card billing data.
            filepath (str): Optional (not used here).
        """
        df["fully_paid"] = df["amount_paid"] <= df["amount_due"]
        df["debt"] = df.apply(lambda row: 0 if row["fully_paid"] else row["amount_due"] - row["amount_paid"], axis=1)
        df["due_date"] = pd.to_datetime(df["month"] + "-01")
        df["payment_date"] = pd.to_datetime(df["payment_date"])
        df["late_days"] = (df["payment_date"] - df["due_date"]).dt.days.astype(int)
        df["fine"] = df["late_days"] * 5.15
        df["total_amount"] = df["amount_due"] + df["fine"]
        


    @staticmethod
    def _transform_transactions(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Transform transaction data.

        Adds:
        - `cost`: Transaction fee
        - `total_amount`: Amount + fee

        Args:
            df (pd.DataFrame): Transaction data.
            filepath (str): Optional (not used here).
        """
        df["cost"] = 0.50 + 0.001 * df["transaction_amount"]
        df["total_amount"] = df["transaction_amount"] + df["cost"]

    @staticmethod
    def _transform_loans(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Transform loan data.

        Adds:
        - `age`: Days since loan utilization
        - `total_cost`: Annual cost of loan including base fee

        Args:
            df (pd.DataFrame): Loan data.
            filepath (str): Used to extract reference date.
        """
        date = pd.to_datetime(Transformer._get_timestamp(filepath)["date"]) if filepath else pd.to_datetime("today")
        utilization_date = pd.to_datetime(df["utilization_date"])
        df["age"] = (date - utilization_date).dt.days
        df["total_cost"] = df["amount_utilized"] * 0.20 + 1000

    @staticmethod
    def transform(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Main method to apply the appropriate transformation based on the file name.

        Args:
            df (pd.DataFrame): The DataFrame to transform.
            filepath (str): File path used to determine which transformer to use.

        Raises:
            KeyError: If no matching transformer function is found for the file.
        """
        TRANSFORMERS = {
            "customer_profiles": Transformer._transform_customer_profiles,
            "support_tickets": Transformer._transform_support_tickets,
            "credit_cards_billing": Transformer._transform_credit_cards_billing,
            "transactions": Transformer._transform_transactions,
            "loans": Transformer._transform_loans,
        }

        key = Transformer._key(filepath)
        if key not in TRANSFORMERS:
            raise KeyError(f"No transformer available for {key}")

        TRANSFORMERS[key](df, filepath)


if __name__ == "__main__":
    """
    Example CLI usage to test a transformer.
    """
    from extractor import Extractor

    filepath = r"D:\ITI\Python\incoming_data\2025-04-18\14\support_tickets.csv"
    extractor = Extractor()
    flag, df = extractor.extract(filepath)
    print(f"[INFO] Extraction {'successful' if flag else 'failed'}")

    if flag:
        transformer = Transformer()
        transformer.transform(df, filepath)
        print("\nTransformed Data Sample:")
        print(df.head())