"""
transformer.py

This module defines the `Transformer` class which handles data transformation for various
financial datasets with comprehensive logging.
"""

import pandas as pd
import logging
from typing import Dict
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Transformer:
    """
    A utility class for transforming different types of financial data with logging.
    """

    @staticmethod
    def _key(filepath: str) -> str:
        """Extract dataset key from filepath with logging."""
        try:
            key = Path(filepath).stem.lower()
            logger.debug(f"Extracted key '{key}' from filepath: {filepath}")
            return key
        except Exception as e:
            logger.error(f"Error extracting key from {filepath}: {str(e)}")
            raise

    @staticmethod
    def _get_timestamp(filepath: str) -> Dict[str, str]:
        """Extract date and hour from filepath with logging."""
        try:
            parts = Path(filepath).parts
            if len(parts) < 3:
                raise ValueError("Invalid filepath structure for timestamp extraction")
            
            result = {"date": parts[-3], "hour": parts[-2]}
            logger.debug(f"Extracted timestamp {result} from {filepath}")
            return result
        except Exception as e:
            logger.error(f"Error extracting timestamp from {filepath}: {str(e)}")
            raise

    @staticmethod
    def _transform_customer_profiles(df: pd.DataFrame, filepath: str = "") -> None:
        """Transform customer profile data with logging."""
        logger.info(f"Starting customer profiles transformation (shape: {df.shape})")
        try:
            date = pd.to_datetime(Transformer._get_timestamp(filepath)["date"]) if filepath else pd.to_datetime("today")
            account_open_date = pd.to_datetime(df["account_open_date"], errors="coerce")
            
            tenure = (date - account_open_date).dt.days // 365.25
            df["tenure"] = tenure.astype(int)
            logger.debug(f"Calculated tenure (min: {df['tenure'].min()}, max: {df['tenure'].max()})")

            def classify_customer_segment(value):
                if value > 5:
                    return "Loyal"
                elif value < 1:
                    return "Newcomer"
                return "Normal"

            df["customer_segment"] = df["tenure"].apply(classify_customer_segment)
            segment_counts = df["customer_segment"].value_counts().to_dict()
            logger.debug(f"Customer segments created: {segment_counts}")
            
            logger.info("Successfully transformed customer profiles")
        except Exception as e:
            logger.error(f"Error transforming customer profiles: {str(e)}")
            raise

    @staticmethod
    def _transform_support_tickets(df: pd.DataFrame, filepath: str = "") -> None:
        """Transform support ticket data with logging."""
        logger.info(f"Starting support tickets transformation (shape: {df.shape})")
        try:
            date = pd.to_datetime(Transformer._get_timestamp(filepath)["date"]) if filepath else pd.to_datetime("today")
            complaint_date = pd.to_datetime(df["complaint_date"])
            
            df["age"] = (date - complaint_date).dt.days.astype(int)
            logger.debug(f"Calculated ticket ages (min: {df['age'].min()}, max: {df['age'].max()})")
            
            logger.info("Successfully transformed support tickets")
        except Exception as e:
            logger.error(f"Error transforming support tickets: {str(e)}")
            raise

    @staticmethod
    def _transform_credit_cards_billing(df: pd.DataFrame, filepath: str = "") -> None:
        """Transform credit card billing data with logging."""
        logger.info(f"Starting credit card billing transformation (shape: {df.shape})")
        try:
            df["fully_paid"] = df["amount_due"] <= df["amount_paid"]
            paid_percentage = df["fully_paid"].mean() * 100
            logger.debug(f"Fully paid percentage: {paid_percentage:.2f}%")

            df["debt"] = (df["amount_due"] - df["amount_paid"]).clip(lower=0).astype(int)
            logger.debug(f"Total debt: {df['debt'].sum()}")

            due_date = pd.to_datetime(df["month"], format="%Y-%m")
            df["late_days"] = (pd.to_datetime(df["payment_date"]) - due_date).dt.days
            df["fine"] = df["late_days"].clip(lower=0) * 5.15
            df["total_amount"] = df["amount_due"] + df["fine"]
            
            logger.debug(f"Total fines calculated: {df['fine'].sum()}")
            logger.info("Successfully transformed credit card billing")
        except Exception as e:
            logger.error(f"Error transforming credit card billing: {str(e)}")
            raise

    @staticmethod
    def _transform_transactions(df: pd.DataFrame, filepath: str = "") -> None:
        """Transform transaction data with logging."""
        logger.info(f"Starting transactions transformation (shape: {df.shape})")
        try:
            df["cost"] = 0.50 + 0.001 * df["transaction_amount"]
            df["total_amount"] = df["transaction_amount"] + df["cost"]
            
            logger.debug(f"Total transaction costs: {df['cost'].sum()}")
            logger.debug(f"Total processed amount: {df['total_amount'].sum()}")
            logger.info("Successfully transformed transactions")
        except Exception as e:
            logger.error(f"Error transforming transactions: {str(e)}")
            raise

    @staticmethod
    def _transform_loans(df: pd.DataFrame, filepath: str = "") -> None:
        """Transform loan data with logging."""
        logger.info(f"Starting loans transformation (shape: {df.shape})")
        try:
            date = pd.to_datetime(Transformer._get_timestamp(filepath)["date"]) if filepath else pd.to_datetime("today")
            utilization_date = pd.to_datetime(df["utilization_date"])
            
            df["age"] = (date - utilization_date).dt.days.astype(int)
            df["total_cost"] = df["amount_utilized"] * 0.20 + 1000
            
            logger.debug(f"Loan ages (min: {df['age'].min()} days, max: {df['age'].max()} days)")
            logger.debug(f"Total loan costs: {df['total_cost'].sum()}")
            logger.info("Successfully transformed loans")
        except Exception as e:
            logger.error(f"Error transforming loans: {str(e)}")
            raise

    @staticmethod
    def transform(df: pd.DataFrame, filepath: str = "") -> None:
        """
        Main transformation method with comprehensive logging.
        """
        TRANSFORMERS = {
            "customer_profiles": Transformer._transform_customer_profiles,
            "support_tickets": Transformer._transform_support_tickets,
            "credit_cards_billing": Transformer._transform_credit_cards_billing,
            "transactions": Transformer._transform_transactions,
            "loans": Transformer._transform_loans,
        }

        try:
            logger.info(f"Starting transformation for file: {filepath}")
            key = Transformer._key(filepath)
            logger.debug(f"Determined dataset type: {key}")

            if key not in TRANSFORMERS:
                error_msg = f"No transformer available for {key}"
                logger.error(error_msg)
                raise KeyError(error_msg)

            logger.info(f"Applying {key} transformation")
            TRANSFORMERS[key](df, filepath)
            logger.info(f"Successfully completed transformation for {key}")
            
        except Exception as e:
            logger.exception(f"Failed to transform {filepath}: {str(e)}")
            raise


if __name__ == "__main__":
    """Example usage with logging."""
    from extractor import Extractor

    filepath = "incoming_data/2025-04-29/21/loans.txt"
    logger.info(f"Starting transformation example for {filepath}")

    try:
        flag, df = Extractor.extract(filepath)
        if not flag:
            logger.error("Extraction failed, cannot proceed with transformation")
        else:
            logger.info("Extraction successful, starting transformation")
            transformer = Transformer()
            transformer.transform(df, filepath)
            logger.info("Transformation completed successfully")
            print("\nTransformed Data Sample:")
            print(df.head())
            
    except Exception as e:
        logger.exception(f"Error during example transformation: {str(e)}")
