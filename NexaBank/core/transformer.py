import pandas as pd


class Transformer:
    """
    A utility class for transforming different types of financial data.
    Handles customer profiles, support tickets, credit card billing, transactions, and loans.
    """

    @staticmethod
    def _key(filepath: str) -> str:
        """Extract the key identifier from filepath for transformer selection."""
        return filepath.replace("\\", "/").split("/")[-1].split(".")[0].lower()

    @staticmethod
    def _get_timestamp(filepath: str) -> dict[str, str]:
        """
        Extract timestamp information from filepath.

        Args:
            filepath: Path containing date and hour information

        Returns:
            dict with 'date' and 'hour' keys
        """
        splitted = filepath.replace("\\", "/").split("/")
        return {"date": splitted[-3], "hour": splitted[-2]}

    @staticmethod
    def _transform_customer_profiles(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Transform customer profile data:
        - Calculate customer tenure
        - Classify customers into segments based on tenure

        Args:
            df: DataFrame containing customer profile data
            filepath: Optional filepath for timestamp extraction
        """
        # Step 1: Get date of the file.
        if filepath:
            timestamp = Transformer._get_timestamp(filepath)
            date = pd.to_datetime(timestamp["date"])
        else:
            date = pd.to_datetime("today")

        # Step 2: Convert account open date to datetime type.
        account_open_date = pd.to_datetime(
            df["account_open_date"],
            errors="coerce",
        )

        # Step 3: Calculate tenure in years
        dt: pd.Series = date - account_open_date
        tenure: pd.Series = dt.dt.days // 365.25
        df["tenure"] = tenure.astype(int)

        def classify_customer_segment(value):
            """Classify customers based on their tenure."""
            if value > 5:
                return "Loyal"
            elif value < 1:
                return "Newcomer"
            else:
                return "Normal"

        # Step 4: Classify customer based on tenure.
        df["customer_segment"] = df["tenure"].apply(classify_customer_segment)

    @staticmethod
    def _transform_support_tickets(df: pd.DataFrame, filepath: str = ""):
        """
        Transform support ticket data:
        - Calculate ticket age in days

        Args:
            df: DataFrame containing support ticket data
            filepath: Optional filepath for timestamp extraction
        """
        # Step 1: Get date of the file.
        if filepath:
            timestamp = Transformer._get_timestamp(filepath)
            date = pd.to_datetime(timestamp["date"])
        else:
            date = pd.to_datetime("today")

        # Step 2: Convert complaint date to datetime type.
        complaint_date = pd.to_datetime(df["complaint_date"])

        # Step 3: Calculate tenure.
        dt: pd.Series = date - complaint_date
        tenure: pd.Series = dt.dt.days
        df["age"] = tenure.astype(int)

    @staticmethod
    def _transform_credit_cards_billing(df: pd.DataFrame, filepath: str = ""):
        # Step 1: Check if fully paid.
        df["fully_paid"] = df["amount_due"] <= df["amount_paid"]

        # Step 2: Calculate the debt amount.
        df["debt"] = (df["amount_due"] - df["amount_paid"]).clip(lower=0).astype(int)

        # Step 3: Calculate late days count.
        due_date = pd.to_datetime(df["month"], format="%Y-%m")
        df["late_days"] = (pd.to_datetime(df["payment_date"]) - due_date).dt.days

        # Step 4: Calculate fine--based on late days.
        df["fine"] = df["late_days"].clip(lower=0) * 5.15

        # Step 5: Calculate total amount.
        df["total_amount"] = df["amount_due"] + df["fine"]

    @staticmethod
    def _transform_transactions(df: pd.DataFrame, filepath: str = ""):
        # Step 1: Calculate transaction cost.
        df["cost"] = 0.50 + 0.001 * df["transaction_amount"]

        # Step 2: Calculate total amount.
        df["total_amount"] = df["transaction_amount"] + df["cost"]

    @staticmethod
    def _transform_loans(df: pd.DataFrame, filepath: str = ""):
        # Step 1: Get date of the file.
        if filepath:
            timestamp = Transformer._get_timestamp(filepath)
            date = pd.to_datetime(timestamp["date"])
        else:
            date = pd.to_datetime("today")

        # Step 2: Calculate transaction age.
        dt: pd.Series = date - pd.to_datetime(df["utilization_date"])
        tenure: pd.Series = dt.dt.days
        df["age"] = tenure.astype(int)

        # Step 3: Calculate the annual cost for the loan.
        df["total_cost"] = df["amount_utilized"] * 0.20 + 1000

    @staticmethod
    def transform(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Main transformation method that routes to specific transformers based on data type.

        Args:
            df: DataFrame to transform
            filepath: Path to source file (used for routing and timestamp extraction)

        Raises:
            KeyError: If no transformer exists for the given file type
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
    # Example usage and testing
    from extractor import Extractor

    filepath = "incoming_data/2025-04-18/14/customer_profiles.csv"
    extractor = Extractor()
    flag, df = extractor.extract(filepath)
    print(f"[INFO] Extraction {'successful' if flag else 'failed'}")

    if flag:
        transformer = Transformer()
        transformer.transform(df, filepath)
        print("\nTransformed Data Sample:")
        print(df.head())  # Display first few rows of transformed data
