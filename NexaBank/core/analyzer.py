"""
churn_analyzer.py

This module provides specialized churn analysis capabilities for financial datasets.
It identifies churn patterns across customer segments, payment behavior, activity trends,
and geographical locations.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

class ChurnAnalyzer:
    """
    A class for analyzing customer churn patterns across multiple financial datasets.
    
    Attributes:
        data (dict): Dictionary of loaded DataFrames
        churn_threshold (int): Days of inactivity to consider a customer churned
        current_date (datetime): Reference date for churn calculation
        report_path (Path): Directory to save analysis reports
    """
    
    def __init__(self, churn_threshold_days: int = 90, report_dir: str = "churn_reports"):
        """
        Initialize the ChurnAnalyzer with churn definition parameters.
        
        Args:
            churn_threshold_days (int): Days of inactivity to consider churn (default 90)
            report_dir (str): Directory to save analysis reports
        """
        self.data: Dict[str, pd.DataFrame] = {}
        self.churn_threshold = churn_threshold_days
        self.current_date = datetime.now()
        self.report_path = Path(report_dir)
        self.report_path.mkdir(parents=True, exist_ok=True)
    
    def load_data(self, datasets: Dict[str, str]) -> None:
        """
        Load required datasets for churn analysis.
        
        Args:
            datasets (dict): Dictionary mapping dataset names to filepaths
                           Expected keys: 'customer_profiles', 'transactions',
                           'credit_cards_billing', 'support_tickets', 'loans'
        """
        from extractor import Extractor  # Using your existing Extractor
        
        for name, path in datasets.items():
            success, df = Extractor.extract(path)
            if success:
                self.data[name] = df
                print(f"[INFO] Loaded {name} dataset with {len(df)} records")
            else:
                print(f"[ERROR] Failed to load {name} dataset from {path}")
    
    def identify_churned_customers(self) -> pd.DataFrame:
        """
        Identify churned customers based on transaction inactivity.
        
        Returns:
            DataFrame: Customer profiles marked with churn status (1=churned, 0=active)
        """
        if 'customer_profiles' not in self.data or 'transactions' not in self.data:
            raise ValueError("Required datasets not loaded")
            
        # Get last transaction date for each customer
        last_transactions = (
            self.data['transactions']
            .groupby('sender')['transaction_date']
            .max()
            .reset_index()
            .rename(columns={'sender': 'customer_id'}))
        
        # Convert dates to datetime
        last_transactions['transaction_date'] = pd.to_datetime(last_transactions['transaction_date'])
        self.data['customer_profiles']['account_open_date'] = pd.to_datetime(
            self.data['customer_profiles']['account_open_date'])
        
        # Merge with customer profiles
        customers = pd.merge(
            self.data['customer_profiles'],
            last_transactions,
            on='customer_id',
            how='left'
        )
        
        # Calculate days since last activity
        customers['days_inactive'] = (
            self.current_date - customers['transaction_date']).dt.days
        
        # Mark churned customers
        customers['churned'] = np.where(
            (customers['days_inactive'] > self.churn_threshold) |
            (customers['days_inactive'].isna()),
            1, 0)
        
        return customers
    
    def segment_churn_analysis(self) -> Dict[str, Dict]:
        """
        Analyze churn rates across different customer segments.
        
        Returns:
            dict: Churn statistics by age group, city, product type, and customer tier
        """
        customers = self.identify_churned_customers()
        results = {}
        
        # Age group analysis
        bins = [18, 30, 40, 50, 60, 80]
        labels = ['18-29', '30-39', '40-49', '50-59', '60+']
        customers['age_group'] = pd.cut(customers['age'], bins=bins, labels=labels)
        
        age_churn = customers.groupby('age_group')['churned'].mean().to_dict()
        results['by_age'] = age_churn
        
        # City analysis
        city_churn = customers.groupby('city')['churned'].mean().sort_values(ascending=False).to_dict()
        results['by_city'] = city_churn
        
        # Product type analysis
        product_churn = customers.groupby('product_type')['churned'].mean().to_dict()
        results['by_product'] = product_churn
        
        # Customer tier analysis
        tier_churn = customers.groupby('customer_tier')['churned'].mean().to_dict()
        results['by_tier'] = tier_churn
        
        return results
    
    def payment_behavior_analysis(self) -> Dict[str, float]:
        """
        Analyze correlation between late payments and churn.
        
        Returns:
            dict: Churn rates by payment behavior metrics
        """
        if 'credit_cards_billing' not in self.data:
            raise ValueError("Credit card billing data not loaded")
            
        customers = self.identify_churned_customers()
        billing = self.data['credit_cards_billing'].copy()
        
        # Convert dates and calculate payment delay
        billing['month'] = pd.to_datetime(billing['month'] + '-01')
        billing['payment_date'] = pd.to_datetime(billing['payment_date'])
        billing['payment_delay'] = (billing['payment_date'] - billing['month']).dt.days
        
        # Aggregate payment behavior by customer
        payment_stats = billing.groupby('customer_id').agg({
            'payment_delay': 'mean',
            'amount_due': 'mean',
            'amount_paid': 'mean'
        }).reset_index()
        
        payment_stats['payment_ratio'] = payment_stats['amount_paid'] / payment_stats['amount_due']
        
        # Merge with churn data
        merged = pd.merge(customers, payment_stats, on='customer_id', how='left')
        
        # Calculate churn rates by payment behavior
        results = {
            'churn_rate_late_payers': merged[merged['payment_delay'] > 5]['churned'].mean(),
            'churn_rate_on_time': merged[merged['payment_delay'] <= 5]['churned'].mean(),
            'churn_rate_low_payment_ratio': merged[merged['payment_ratio'] < 0.9]['churned'].mean(),
            'churn_rate_high_payment_ratio': merged[merged['payment_ratio'] >= 0.9]['churned'].mean()
        }
        
        return results
    
    def activity_trend_analysis(self) -> Dict[str, Dict]:
        """
        Compare activity patterns between churned and active customers.
        
        Returns:
            dict: Activity metrics comparison (transactions, loans, support tickets)
        """
        customers = self.identify_churned_customers()
        results = {}
        
        # Transaction activity
        if 'transactions' in self.data:
            tx_counts = self.data['transactions'].groupby('sender').size().reset_index(name='tx_count')
            merged = pd.merge(customers, tx_counts, left_on='customer_id', right_on='sender', how='left')
            results['transactions'] = {
                'active_mean': merged[merged['churned'] == 0]['tx_count'].mean(),
                'churned_mean': merged[merged['churned'] == 1]['tx_count'].mean()
            }
        
        # Loan activity
        if 'loans' in self.data:
            loan_counts = self.data['loans'].groupby('customer_id').size().reset_index(name='loan_count')
            merged = pd.merge(customers, loan_counts, on='customer_id', how='left')
            results['loans'] = {
                'active_mean': merged[merged['churned'] == 0]['loan_count'].mean(),
                'churned_mean': merged[merged['churned'] == 1]['loan_count'].mean()
            }
        
        # Support ticket activity
        if 'support_tickets' in self.data:
            ticket_counts = self.data['support_tickets'].groupby('customer_id').size().reset_index(name='ticket_count')
            merged = pd.merge(customers, ticket_counts, on='customer_id', how='left')
            results['support_tickets'] = {
                'active_mean': merged[merged['churned'] == 0]['ticket_count'].mean(),
                'churned_mean': merged[merged['churned'] == 1]['ticket_count'].mean()
            }
        
        return results
    
    def revenue_analysis(self) -> Dict[str, Dict]:
        """
        Analyze revenue patterns and ARPU for churned vs active customers.
        
        Returns:
            dict: Revenue metrics including ARPU and spending patterns
        """
        if 'transactions' not in self.data:
            raise ValueError("Transactions data not loaded")
            
        customers = self.identify_churned_customers()
        
        # Calculate customer revenue from transactions
        customer_revenue = (
            self.data['transactions']
            .groupby('sender')['transaction_amount']
            .sum()
            .reset_index()
            .rename(columns={'sender': 'customer_id', 'transaction_amount': 'total_spend'}))
        
        merged = pd.merge(customers, customer_revenue, on='customer_id', how='left')
        merged['total_spend'] = merged['total_spend'].fillna(0)
        
        # Calculate metrics
        active_customers = merged[merged['churned'] == 0]
        churned_customers = merged[merged['churned'] == 1]
        
        results = {
            'arpu_active': active_customers['total_spend'].mean(),
            'arpu_churned': churned_customers['total_spend'].mean(),
            'high_spenders_churn_rate': merged[merged['total_spend'] > merged['total_spend'].quantile(0.9)]['churned'].mean(),
            'low_spenders_churn_rate': merged[merged['total_spend'] < merged['total_spend'].quantile(0.1)]['churned'].mean()
        }
        
        return results
    
    def tenure_analysis(self) -> Dict[str, float]:
        """
        Analyze churn rates by customer tenure.
        
        Returns:
            dict: Churn rates by tenure groups
        """
        customers = self.identify_churned_customers()
        
        # Calculate tenure in days first, then convert to months
        customers['tenure_days'] = (self.current_date - customers['account_open_date']).dt.days
        customers['tenure_months'] = (customers['tenure_days'] / 30.44).astype(int)  # Approximate months
        
        # Group by tenure
        bins = [0, 6, 12, 24, 36, 60, 120]
        labels = ['0-6m', '6-12m', '1-2y', '2-3y', '3-5y', '5y+']
        customers['tenure_group'] = pd.cut(customers['tenure_months'], bins=bins, labels=labels)
        
        tenure_churn = customers.groupby('tenure_group')['churned'].mean().to_dict()
        return tenure_churn
    
    def generate_churn_report(self) -> str:
        """
        Generate a comprehensive churn analysis report with all metrics.
        
        Returns:
            str: Path to the generated HTML report
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.report_path / f"churn_analysis_{timestamp}.html"
        
        # Collect all analysis results
        results = {
            "segment_analysis": self.segment_churn_analysis(),
            "payment_behavior": self.payment_behavior_analysis(),
            "activity_trends": self.activity_trend_analysis(),
            "revenue_analysis": self.revenue_analysis(),
            "tenure_analysis": self.tenure_analysis()
        }
        
        # Generate HTML report
        html_content = f"""
        <html>
            <head>
                <title>Customer Churn Analysis Report</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                    h1 {{ color: #2c3e50; }}
                    h2 {{ color: #3498db; margin-top: 30px; }}
                    h3 {{ color: #7f8c8d; }}
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    .highlight {{ background-color: #fffde7; }}
                    .negative {{ color: #e74c3c; }}
                    .positive {{ color: #2ecc71; }}
                </style>
            </head>
            <body>
                <h1>Customer Churn Analysis Report</h1>
                <p>Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                <p>Churn threshold: {self.churn_threshold} days of inactivity</p>
                <h2>1. Churn by Customer Segments</h2>
                <h3>By Age Group</h3>
                <table>
                    <tr><th>Age Group</th><th>Churn Rate</th></tr>
        """
        
        # Add age group data
        for age, rate in results['segment_analysis']['by_age'].items():
            html_content += f"<tr><td>{age}</td><td>{rate:.1%}</td></tr>"
        
        html_content += """
                </table>
                <h3>By City (Top 5 Highest Churn)</h3>
                <table>
                    <tr><th>City</th><th>Churn Rate</th></tr>
        """
        
        # Add city data
        for city, rate in sorted(results['segment_analysis']['by_city'].items(), 
                              key=lambda x: x[1], reverse=True)[:5]:
            html_content += f"<tr><td>{city}</td><td>{rate:.1%}</td></tr>"
        
        # Add visualizations
        self._generate_churn_visualizations(results)
        html_content += """
                </table>
                <h2>2. Churn Visualizations</h2>
                <img src="churn_by_segment.png" width="800">
                <img src="payment_behavior_churn.png" width="800">
            </body>
        </html>
        """
        
        with open(report_file, 'w') as f:
            f.write(html_content)
        
        return str(report_file)
    
    def _generate_churn_visualizations(self, results: Dict) -> None:
        """
        Generate visualizations for churn analysis.
        
        Args:
            results (dict): Analysis results to visualize
        """
        # Churn by segment plot
        plt.figure(figsize=(10, 6))
        pd.DataFrame(results['segment_analysis']['by_age'].items(), 
                    columns=['Age Group', 'Churn Rate']).plot.bar(
            x='Age Group', y='Churn Rate', legend=False)
        plt.title('Churn Rate by Age Group')
        plt.ylabel('Churn Rate')
        plt.savefig(self.report_path / 'churn_by_segment.png')
        plt.close()
        
        # Payment behavior plot
        payment_data = pd.DataFrame({
            'Category': ['Late Payers', 'On-Time Payers'],
            'Churn Rate': [
                results['payment_behavior']['churn_rate_late_payers'],
                results['payment_behavior']['churn_rate_on_time']
            ]
        })
        plt.figure(figsize=(8, 5))
        sns.barplot(x='Category', y='Churn Rate', data=payment_data)
        plt.title('Churn Rate by Payment Behavior')
        plt.savefig(self.report_path / 'payment_behavior_churn.png')
        plt.close()

if __name__ == "__main__":
    # Example usage
    analyzer = ChurnAnalyzer(churn_threshold_days=60)
    
    # Load datasets (replace with your actual paths)
    datasets = {
        'customer_profiles': 'incoming_data/2025-04-29/21/customer_profiles.csv',
        'transactions': 'incoming_data/2025-04-29/21/transactions.json',
        'credit_cards_billing': 'incoming_data/2025-04-29/21/credit_cards_billing.csv',
        'support_tickets': 'incoming_data/2025-04-29/21/support_tickets.csv',
        'loans': 'incoming_data/2025-04-29/21/loans.txt'
    }
    
    analyzer.load_data(datasets)
    
    # Generate and save report
    report_path = analyzer.generate_churn_report()
    print(f"Churn analysis report generated at: {report_path}")