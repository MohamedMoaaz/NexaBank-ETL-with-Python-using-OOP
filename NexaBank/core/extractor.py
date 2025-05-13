import pandas as pd
import json
import csv

class Extractor:
    @staticmethod
    def extract(file_path: str) -> pd.DataFrame:
        if file_path.endswith(".json"):
            with open(file_path, "r") as f:
                return pd.DataFrame(json.load(f))

        elif file_path.endswith(".txt") or file_path.endswith(".csv"):
            with open(file_path, "r") as f:
                sample = f.read(2048)  # Read a small portion to sniff
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    delimiter = dialect.delimiter
                except csv.Error:
                    # Fallback if it cannot detect a delimiter (assume comma)
                    delimiter = ","
                f.seek(0)
                return pd.read_csv(f, delimiter=delimiter)

        else:
            raise ValueError(f"Unsupported file format: {file_path}")

if __name__ == "__main__":
    # Example usage
    df = Extractor.extract(r"D:\ITI\Python\test\tab.txt")  # Change to your file path
    print(df.head())

    #"D:\ITI\Python\incoming_data\2025-04-18\14\loans.txt"
    #"D:\ITI\Python\incoming_data\2025-04-18\14\customer_profiles.csv"
    #"D:\ITI\Python\incoming_data\2025-04-18\14\transactions.json"
    #"D:\ITI\Python\test\semicolon.txt"