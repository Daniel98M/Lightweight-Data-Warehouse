# Data Warehouse

## 📊 Overview
Lightweight data warehouse for Business Intelligence using Python, DuckDB, and Parquet files.

## 🏗️ Project Structure
```
dwh/
├── data/
│   ├── raw/                    # Raw data partitioned by year/month/day
│   │   └── case_history/
│   │       └── YYYY/MM/DD/
│   ├── staging/                # Cleaned and validated data
│   └── warehouse/              # Final dimensional model (dims + facts)
│
├── src/                        # Source code
│   ├── config/                 # Database configuration
│   ├── extraction/             # Data extraction scripts
│   ├── transformation/         # ETL logic
│   └── reporting/              # Business queries and reports
│
├── sql/                        # SQL queries
│   └── queries/
│
├── scripts/                    # Executable scripts
│   └── setup_project.py        # This setup script
│   
│
├── tests/                      # Executable tests scripts
│   └── test_connection.py      # Connection test
│
├── backups/                    # Weekly snapshots
├── logs/                       # Application logs
└── rrhh.duckdb                 # DuckDB database file
```

## 🚀 Getting Started

1. **Create virtual environment:**
```bash
python -m venv .dwhenv
```

2. **Initialize virtual environment:**
```bash
.dwhenv\Scriptsctivate
```
3. **Update pip:**
```bash
python.exe -m pip install --upgrade pip
``` 

4. **Install dependencies:**
```bash
pip install duckdb pandas pyarrow
```

5. **Test connection:**
```bash
python tests/test_connection.py
```

6. **Start using the warehouse!**

## 📝 Notes
- Raw data uses hierarchical partitioning: `year/month/day/`
- One Parquet file per extraction date
- DuckDB for analytics, Parquet for storage
- Created on: 2026-02-10
