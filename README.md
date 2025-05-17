
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

