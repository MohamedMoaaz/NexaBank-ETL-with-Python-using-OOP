import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ChurnAnalyzer:
    """
    A simplified analyzer to identify cities with highest churn rates.
    """
    
    def __init__(self, churn_threshold_days: int = 90):
        """
        Initialize with churn threshold.
        
        Args:
            churn_threshold_days (int): Days of inactivity to consider churn
        """
        self.churn_threshold = churn_threshold_days
        self.current_date = datetime.now()
        self.customer_data = None
        self.transaction_data = None
    
    def load_data_from_directory(self, base_dir: str) -> None:
        """
        Load all customer and transaction data from directory structure:
        base_dir/YYYY-MM-DD/HH/customer_profiles.csv
        base_dir/YYYY-MM-DD/HH/transactions.json
        
        Args:
            base_dir (str): Path to base directory containing dated folders
        """
        from extractor import Extractor
        
        # Get current date folder
        date_folder = self.current_date.strftime("%Y-%m-%d")
        date_path = Path(base_dir) / date_folder
        
        if not date_path.exists():
            raise ValueError(f"Date directory {date_path} does not exist")
        
        customer_dfs = []
        transaction_dfs = []
        
        # Iterate through all hour directories
        for hour_dir in date_path.iterdir():
            if not hour_dir.is_dir():
                continue
                
            try:
                # Load customer data
                customer_file = hour_dir / "customer_profiles.csv"
                if customer_file.exists():
                    success, df = Extractor.extract(str(customer_file))
                    if success:
                        customer_dfs.append(df)
                        logger.info(f"Loaded customer data from {customer_file}")
                
                # Load transaction data
                transaction_file = hour_dir / "transactions.json"
                if transaction_file.exists():
                    success, df = Extractor.extract(str(transaction_file))
                    if success:
                        transaction_dfs.append(df)
                        logger.info(f"Loaded transaction data from {transaction_file}")
                        
            except Exception as e:
                logger.warning(f"Error processing {hour_dir}: {str(e)}")
                continue
        
        if not customer_dfs:
            raise ValueError("No customer data files found")
        if not transaction_dfs:
            raise ValueError("No transaction data files found")
        
        # Combine all data
        self.customer_data = pd.concat(customer_dfs, ignore_index=True)
        self.transaction_data = pd.concat(transaction_dfs, ignore_index=True)
        
        logger.info(f"Combined customer data: {len(self.customer_data)} records")
        logger.info(f"Combined transaction data: {len(self.transaction_data)} records")
    
    def identify_churned_customers(self) -> pd.DataFrame:
        """
        Mark customers as churned based on transaction inactivity.
        
        Returns:
            DataFrame: Customers with churn status (1=churned, 0=active)
        """
        if self.customer_data is None or self.transaction_data is None:
            raise ValueError("Required data not loaded")
            
        # Get last transaction date for each customer
        last_trans = (
            self.transaction_data
            .groupby('sender')['transaction_date']
            .max()
            .reset_index()
            .rename(columns={'sender': 'customer_id'})
        )
        
        # Merge with customer data
        customers = pd.merge(
            self.customer_data,
            last_trans,
            on='customer_id',
            how='left'
        )
        
        # Calculate inactivity and mark churn
        customers['days_inactive'] = (
            self.current_date - pd.to_datetime(customers['transaction_date'])
        ).dt.days
        
        customers['churned'] = (
            (customers['days_inactive'] > self.churn_threshold) |
            customers['days_inactive'].isna()
        ).astype(int)
        
        logger.info(f"Identified {customers['churned'].sum()} churned customers")
        return customers
    
    def get_high_churn_cities(self, top_n: int = 10) -> pd.DataFrame:
        """
        Find cities with highest churn rates.
        
        Args:
            top_n (int): Number of top cities to return
            
        Returns:
            DataFrame: Cities sorted by churn rate (highest first)
        """
        customers = self.identify_churned_customers()
        
        # Calculate churn rate by city
        city_churn = (
            customers.groupby('city')['churned']
            .agg(['sum', 'count', 'mean'])
            .rename(columns={
                'sum': 'churned_customers',
                'count': 'total_customers',
                'mean': 'churn_rate'
            })
            .sort_values('churn_rate', ascending=False)
            .head(top_n)
        )
        
        logger.info(f"Top {top_n} high-churn cities identified")
        return city_churn

if __name__ == "__main__":
    # Example usage
    analyzer = ChurnAnalyzer(churn_threshold_days=60)
    
    try:
        # Load all data from directory structure
        analyzer.load_data_from_directory(
            base_dir='/Users/mohamedmoaaz/Desktop/nexabank/incoming_data'  # Point to your base directory
        )
        
        # Get high churn cities
        high_churn_cities = analyzer.get_high_churn_cities(top_n=5)
        print("\nCities with Highest Churn Rates:")
        print(high_churn_cities)
        
    except Exception as e:
        logger.error(f"Error in churn analysis: {str(e)}")
