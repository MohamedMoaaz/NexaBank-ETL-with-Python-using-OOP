
## 📄 extractor.py

The `extractor.py` module defines the `Extractor` class, which provides a utility method for loading structured data from various file formats into a pandas `DataFrame`. This is useful for ETL pipelines, data analysis, and preprocessing workflows.

---

### ✅ Supported File Formats

- **CSV (.csv)** – Comma-delimited or auto-detected delimiters  
- **TXT (.txt)** – Typically pipe-delimited, but supports auto-detection  
- **JSON (.json)** – Records-oriented JSON (a list of objects/dictionaries)

---

### 🧠 Key Features

- Automatic delimiter detection for `.csv` and `.txt` using `csv.Sniffer`
- Reads JSON in records format using `pandas.read_json(..., orient="records")`
- Comprehensive error handling for:
  - Missing or empty files  
  - Malformed JSON  
  - Parsing issues  
  - File encoding errors  
- Integrated logging to both console and file (`logs.log`) with clear, structured messages

---

### 🔄 Return Value

The `extract()` method returns:

```python```
(success: bool, dataframe: pd.DataFrame | None)

---

### 🪵 Logging

Logging is configured to:

- **Print to console** (`stdout`) for real-time feedback  
- **Write to file** (`logs.log`) for persistent log history

The logger captures both:

- **High-level messages** — e.g., success or failure of extraction  
- **Detailed debug info** — e.g., the shape of the extracted DataFrame

----

# 📦 Transformers Module

The `transformers` module is responsible for extracting metadata from file paths and applying transformation logic to raw datasets. It is intended to be used as part of a larger ETL or data warehousing pipeline.

---

## 📁 Expected File Structure

The module assumes input files are organized as follows:

/<base_path>/landing/YYYY-MM-DD/HH/filename.csv

**Example:**
/data/landing/2025-05-17/14/deliveries.csv

Where:

- `YYYY-MM-DD` is the date of the data load
- `HH` is the hour (24-hour format)
- `filename.csv` is the name of the dataset

---

## 🧠 Overview

The `Transformer` class provides the following core functionality:

- Extract dataset key (e.g., `"deliveries"`) from file paths
- Extract timestamp metadata (e.g., date and hour) from file paths
- Apply business logic transformations (e.g., calculate fines based on delivery delays)

---


