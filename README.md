
## 📄 extractor.py
The extractor.py module defines the Extractor class, which provides a utility method for loading structured data from various file formats into a pandas DataFrame. This is useful for ETL pipelines, data analysis, and preprocessing workflows.

✅ Supported File Formats
CSV (.csv) – Comma-delimited or auto-detected delimiters
TXT (.txt) – Typically pipe-delimited, but supports auto-detection
JSON (.json) – Records-oriented JSON (a list of objects/dictionaries)

🧠 Key Features
Automatic delimiter detection for .csv and .txt using csv.Sniffer
Reads JSON in records format with pandas.read_json(..., orient="records")
Comprehensive error handling for:
Missing or empty files
Malformed JSON
Parsing issues
File encoding errors
Integrated logging to both console and file (logs.log) with clear, structured messages

🔄 Return Value
The extract() method returns:
(success: bool, dataframe: pd.DataFrame | None)
success: Indicates whether the extraction was successful
dataframe: The loaded DataFrame if successful; otherwise None

🪵 Logging
Logging is configured to:
Print messages to the console (stdout)
Save logs to a file (logs.log)
Both high-level status messages and detailed debug information (e.g., DataFrame shape) are recorded.
