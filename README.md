
# NexaBank Real-Time Churn Prediction Pipeline

This project implements a **real-time ETL data pipeline** for NexaBank to process customer data from multiple formats and systems, prepare it for churn prediction modeling, and deliver it into a Hive-compatible HDFS storage.

---

## 🧠 Objective

- Predict customer churn using enriched, real-time customer data.
- Clean and transform messy multi-source data into a structured, machine-learning-ready format.
- Automatically monitor, validate, transform, and export incoming data to HDFS.

---

## ⚙️ Implemented Components

### `main.py`
- The **core orchestrator** for the entire pipeline.
- Loads the schema and coordinates: extraction, validation, transformation, and loading.
- Logs every step and handles exceptions gracefully with retry logic.

### `file_listener.py`
- Real-time **file monitoring service** using `watchdog`.
- Automatically triggers processing when a new file appears in the partitioned folder.

### `extractor.py`
- Loads data from:
  - `.csv` (customer, billing, support)
  - `.json` (transactions)
  - `.txt` (loans)
- Converts all inputs into Pandas DataFrames.

### `validator.py` & `validator_func.py`
- Validates files against a **YAML schema**.
- Supports:
  - Type checking
  - Required field presence
  - Regex pattern matching
  - Enum values
- Rejects non-compliant files and sends email alerts.

### `transformer.py`
- Applies all required transformations:
  - Adds derived columns (tenure, segments, cost, fine, total amount).
  - Computes metrics (e.g., ARPU, late days, loan costs).
  - Handles timestamp-based transformations (e.g., ticket age).
  - Applies **Caesar cipher encryption** on `loan_reason`.

### `encryption.py`
- Caesar cipher-based encryption and brute-force decryption using dictionary match.
- Supports both random-key generation and keyless recovery.

### `folder_status.py`
- Manages a `_status.json` file that tracks which files have been processed to avoid reprocessing.
- Works as a lightweight **state store**.

### `loader.py`
- Saves transformed tables in **Parquet format**.
- Uploads them to **HDFS** using subprocess and Docker client.
- Organizes data by partition (`date/hour`).

### `analyzer.py`
- Performs **optional churn-related analysis**:
  - Segment churn rates
  - Correlation between late payments and churn
  - ARPU by customer tier or geography

---

## 🔄 Pipeline Flow

```
Incoming_data/
  └── YYYY-MM-DD/
        └── HH/
             ├── customer_profiles.csv
             ├── credit_cards_billing.csv
             ├── support_tickets.csv
             ├── loans.txt
             └── transactions.json
```

1. Listener detects new files.
2. Extractor loads them into DataFrames.
3. Validator checks schema compliance.
4. Transformer enriches data.
5. Encryption secures sensitive fields.
6. Loader saves Parquet files and sends to HDFS.
7. Analyzer produces insights.

---

## 🔐 Security & Email Notifications

- Passwords for email alerts are stored in external files.
- Sensitive fields like `loan_reason` are encrypted.
- On:
  - Schema mismatch → send rejection email.
  - Pipeline error → log, retry, and alert.

---

## 📁 Output

- ✅ Enriched datasets in `.parquet`
- ✅ Partitioned by `date/hour`
- ✅ Ready for ingestion by **Hive tables**

---

## 📈 Insights Examples (from `analyzer.py`)

- 📊 **Churn segments**: Newcomers (<1 year) churn 2× more.
- 💳 **Late payers**: Customers with 5+ late payments more likely to churn.
- 🌍 **City-based churn**: Certain cities show >20% churn rate.

---

## 🧰 Tech Stack

- Python 3.11
- Pandas, PyArrow
- Watchdog (file monitoring)
- SMTP (email alerts)
- HDFS (via Docker subprocess)
- Hive-ready Parquet export
- Caesar cipher encryption module

---

## 📝 Logging

- Logs stored locally for traceability.
- Format:
  ```
  [Timestamp] [Stage] [Status] - Details
  ```
- Includes:
  - Row counts
  - Schema summaries
  - Processing duration
  - Encryption key (if applicable)

---

## 📬 Email Setup

- Email password read from external `.txt`
- Alerts sent on:
  - Validation failure
  - Processing errors
  - Completion summary (optional)

---

## ✅ Real-Time Constraints

- No duplicate processing due to `_status.json`
- Fully automated triggering on new data
- Quick retries on failure

---

## 📁 Project Structure

```bash
NexaBank-ETL/
├── Incoming_data/
├── analyzer.py
├── encryption.py
├── extractor.py
├── file_listener.py
├── folder_status.py
├── loader.py
├── main.py
├── transformer.py
├── validator.py
├── validator_func.py
├── _status.json
└── README.md
```

---

## 📣 Authors

Mohamed Moaaz, Mariam Eid, Ahmed Elshikh
