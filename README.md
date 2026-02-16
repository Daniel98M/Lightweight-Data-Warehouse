# 🏢 DataCore - Lightweight Data Warehouse

> Lightweight Data Warehouse using DuckDB, Parquet, and Python

A production-ready data warehouse solution for Business Intelligence built with best practices in data engineering. Features Hive-style partitioning, dimensional modeling, and seamless integration with DuckDB for fast analytics.

[![Python](https://img.shields.io/badge/python-3.11--3.14-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1+-yellow.svg)](https://duckdb.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![GitHub](https://img.shields.io/github/stars/Daniel98M/Lightweight-Data-Warehouse?style=social)](https://github.com/Daniel98M/Lightweight-Data-Warehouse)

---

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Usage](#-usage)
- [Suggested Project Structure](#-suggested-project-structure)
- [Development](#-development)

---

## ✨ Features

- **🚀 Fast Analytics**: DuckDB's columnar engine for sub-second queries
- **📦 Hive Partitioning**: Automatic date-based partitioning (year/month/day)
- **💾 Parquet Storage**: Efficient compression and portable format
- **🔌 Pluggable Architecture**: Abstract database interface for easy migration
- **📊 Dimensional Modeling**: Star schema design for business intelligence
- **🔄 Incremental Loads**: Support for both snapshot and delta loads
- **🎯 Type Safety**: Full type hints and abstract base classes

---

## 🏗️ Architecture

### Data Flow
```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  CSV/Excel  │────▶│  RAW Layer   │────▶│   DuckDB    │
│   Source    │     │   (Parquet)  │     │  Analytics  │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                           │ Hive Partitioning
                           ▼
              year=2025/month=02/day=11/
                    case_history_*.parquet
```

### Storage Strategy

**Hybrid Approach**: Parquet for persistence + DuckDB for analytics

- ✅ **Portability**: Parquet files work with any tool (Spark, Pandas, Power BI)
- ✅ **Performance**: DuckDB indexes and query optimization
- ✅ **Reliability**: Easy backups and disaster recovery
- ✅ **Scalability**: Add more partitions as data grows

### Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database Engine** | DuckDB 1.1+ | In-process analytics |
| **Storage Format** | Apache Parquet | Columnar storage |
| **Language** | Python 3.11-3.14 | ETL and orchestration |
| **Data Processing** | Pandas 2.0+ | Data transformation |
| **Partitioning** | Hive-style | Date-based organization |

---

## 📦 Installation

### Prerequisites

- **Python 3.11, 3.12, 3.13, or 3.14**
- pip package manager

### Install Package

**Development Mode**
```bash
# Clone repository
git clone https://github.com/Daniel98M/Lightweight-Data-Warehouse.git
cd Lightweight-Data-Warehouse

# Verify Python version
python --version

# Install
pip install -e .
```

**From requirements.txt**
```bash
pip install -r requirements.txt
```

### Verify Installation
```bash
python -c "import datacore; print(datacore.__version__)"
# Output: 0.1.0
```

---

## 🚀 Quick Start

### 1. Load Your First Dataset
```python
from datacore import ParquetLoader
from datetime import datetime

# Initialize loader
loader = ParquetLoader(base_path:Path = Path("data/raw/case_history"))

# Load CSV with automatic Hive partitioning
parquet_path = loader.load_from_csv(
    'path/to/casos.csv',
    extraction_date=datetime(2025, 2, 11)
)

# Load Excel
parquet_path = loader.load_from_excel(
    'path/to/casos.xlsx',
    extraction_date=datetime.now()
)
```

### 2. Query Your Data
```python
from datacore import DuckDBAdapter

# Query with Hive partitioning
db = DuckDBAdapter(
    db_path:str = "dwh.duckdb",
    memory_limit:str = "4GB",
    threads:int = 4
)

df = db.fetch_df("""
    SELECT *
    FROM read_parquet('data/raw/case_history/**/*.parquet',
                        hive_partitioning = true)
    WHERE year = 2025 AND month = 2
    LIMIT 10
""")

print(f'Total cases: {len(df):,}')
```

---

## 💻 Usage

### Loading Data
```python
from datacore import ParquetLoader
from datetime import datetime

loader = ParquetLoader(base_path:Path = Path("data/raw/example"))

# Load CSV with specific date
parquet_path = loader.load_from_csv(
    'downloads/casos_20250211.csv',
    extraction_date=datetime(2025, 2, 11)
)
# Output:
# 📥 Loading CSV: casos_20250211.csv
# ✓ Loaded 1,500 rows, 30 columns
# ✓ Saved to: data/raw/casos_rrhh/year=2025/month=02/day=11/casos_rrhh_20250211.parquet
#   File size: 0.45 MB
#   Compression: ZSTD

# Load Excel
parquet_path = loader.load_from_excel(
    'downloads/casos.xlsx',
    sheet_name='Sheet1',
    extraction_date=datetime.now()
)

# View all loaded files
summary = loader.get_data_summary()
print(summary)
#  year  month  day                     filename  size_mb           modified_at
#  2025     02   11  casos_rrhh_20250211.parquet     0.45  2025-02-11 10:30:15
#  2025     02   04  casos_rrhh_20250204.parquet     0.43  2025-02-04 09:15:22
```

---

### Querying Data with DuckDB

#### Connect with the database
```python
from datacore import DuckDBAdapter

# Create an Instance of the duckdb database
db = DuckDBAdapter(
    db_path:str = "dwh.duckdb",
    memory_limit:str = "4GB",
    threads:int = 4
)
```

#### Basic Queries
```python

df = db.fetch_df("""
    SELECT *
    FROM read_parquet('data/raw/example/**/*.parquet',
                        hive_partitioning = true)
""")

print(f'Total cases: {len(df):,}'))
```

#### Partition Pruning (Fast Queries)
```python
# Only reads files from February 2025 (very fast!)
df_feb = db.fetch_df("""
    SELECT *
    FROM read_parquet('data/raw/example/**/*.parquet',
                        hive_partitioning = true)
    WHERE year = 2025 AND month = 2
""")
print(f"February cases: {len(df_feb):,}")

# Specific day
df_day = db.fetch_df("""
    SELECT *
    FROM read_parquet('data/raw/example/**/*.parquet',
                        hive_partitioning = true)
    WHERE year = 2025 AND month = 2 AND day = 11
""")
print(f"Cases on Feb 11: {len(df_day):,}")
```

#### Aggregations
```python
# Cases by country
summary = db.fetch_df("""
    SELECT 
        COUNTRY,
        STATUS,
        COUNT(*) as case_count,
        AVG(days_to_resolve) as avg_resolution_days
    FROM read_parquet('data/raw/example/**/*.parquet',
                        hive_partitioning = true)
    WHERE year = 2025 AND month = 2
    GROUP BY COUNTRY, STATUS
    ORDER BY case_count DESC
""")
print(summary)
```

#### Partition Statistics
```python
stats = db.fetch_df("""
    SELECT 
        year,
        month,
        day,
        COUNT(*) as case_count,
        COUNT(DISTINCT CASE_ID) as unique_cases
    FROM read_parquet('data/raw/example/**/*.parquet',
                        hive_partitioning = true)
    GROUP BY year, month, day
    ORDER BY year DESC, month DESC, day DESC
""")
print(stats)
```

---

### Working with Tables

#### Create Table from Parquet
```python
from datacore import DuckDBAdapter
from pathlib import Path

# Create an Instance of the duckdb database
db = DuckDBAdapter(
    db_path:str = "dwh.duckdb",
    memory_limit:str = "4GB",
    threads:int = 4
)

# Create table from specific Parquet file
db.create_table_from_parquet(
    parquet_path=Path('data/raw/case_history/year=2025/month=02/day=11/case_history_20250211.parquet'),
    table_name='cases_february',
    if_exists='replace'
)

# Now query the table directly
df = db.fetch_df("SELECT * FROM cases_february WHERE STATUS = 'Open'")
print(f"Open cases: {len(df)}")
```

#### Create Table from DataFrame
```python
import pandas as pd
from datacore import DuckDBAdapter

# Create an Instance of the duckdb database
db = DuckDBAdapter(
    db_path:str = "dwh.duckdb",
    memory_limit:str = "4GB",
    threads:int = 4
)

# Transform data
df = pd.DataFrame({
    'case_id': ['C001', 'C002', 'C003'],
    'country': ['Mexico', 'USA', 'Colombia'],
    'status': ['Open', 'Resolved', 'Open']
})

# Save to DuckDB table
db.create_table_from_df(df, 'temp_cases', if_exists='replace')

# Query it
result = db.fetch_df("SELECT country, COUNT(*) as count FROM temp_cases GROUP BY country")
print(result)
```

#### Export to Parquet
```python
from datacore import DuckDBAdapter
from pathlib import Path

# Create an Instance of the duck's database
db = DuckDBAdapter(
    db_path:str = "dwh.duckdb",
    memory_limit:str = "4GB",
    threads:int = 4
)

# Complex query and export results
db.export_to_parquet(
    query="""
        SELECT 
            year,
            month,
            COUNTRY,
            TYPE,
            COUNT(*) as case_count,
            AVG(days_to_resolve) as avg_days
        FROM read_parquet('data/raw/case_history/**/*.parquet',
                            hive_partitioning = true)
        WHERE year = 2025
        GROUP BY year, month, COUNTRY, TYPE
    """,
    output_path=Path('data/warehouse/monthly_summary.parquet')
)
print("✓ Summary exported to data/warehouse/monthly_summary.parquet")
```

---

## 📁 Suggested Project Structure
```
Lightweight-Data-Warehouse/
├── src/
│   └── datacore/                   # Main package
│       ├── __init__.py             # Package exports
│       ├── config/                 # Configuration & database
│       │   ├── __init__.py
│       │   ├── database_config.py  # Database factory
│       │   ├── database_interface.py  # Abstract base class
│       │   ├── duckdb_adapter.py   # DuckDB implementation
│       │   └── file_utils.py       # File system utilities
│       ├── extraction/             # Data loading
│       │   ├── __init__.py
│       │   └── parquet_loader.py   # CSV/Excel → Parquet
│       └── transformation/         # Data processing (future)
│           └── __init__.py
│
├── data/                           # Data storage (gitignored)
│   ├── raw/                        # Raw Parquet files
│   │   └── case_history/
│   │       └── year=YYYY/month=MM/day=DD/
│   ├── staging/                    # Cleaned data
│   └── warehouse/                  # Dimensional model
│
├── scripts/                        # Utility scripts
│
├── sql/                            # SQL queries
│   └── queries/                    # Business queries
│
├── notebooks/                      # Jupyter notebooks (optional)
│   └── exploratory_analysis.ipynb
│
├── tests/                          # Unit tests (future)
├── backups/                        # Data backups
├── logs/                           # Application logs
│
├── pyproject.toml                  # Package configuration
├── requirements.txt                # Dependencies
├── README.md                       # This file
├── .gitignore                      # Git exclusions
└── datacore.duckdb                 # DuckDB database file
```

---

## 🧪 Development

### Setup Development Environment
```bash
# Clone repository
git clone https://github.com/Daniel98M/Lightweight-Data-Warehouse.git
cd Lightweight-Data-Warehouse

# Install package
pip install -e .
```

---

## 🗺️ Roadmap

### ✅ Phase 1: Core Infrastructure (Completed)
- [x] Project structure with Hive partitioning
- [x] Abstract database interface
- [x] DuckDB adapter implementation
- [x] CSV/Excel to Parquet loader
- [x] Direct DuckDB querying API
- [x] Package setup (pyproject.toml)

---

## 📚 Links

- **Repository**: [github.com/Daniel98M/Lightweight-Data-Warehouse](https://github.com/Daniel98M/Lightweight-Data-Warehouse)

---

## 🤝 Contributing

Contributions are welcome! This is a learning project focused on best practices.

### How to Contribute

1. Fork the repository: [Lightweight-Data-Warehouse](https://github.com/Daniel98M/Lightweight-Data-Warehouse)
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guide
- Add type hints to all functions
- Write docstrings for public APIs
- Include examples in docstrings
- Update README for new features

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Daniel Martínez**
- 🐙 GitHub: [@Daniel98M](https://github.com/Daniel98M)
- 📂 Project: [Lightweight-Data-Warehouse](https://github.com/Daniel98M/Lightweight-Data-Warehouse)
- 💼 LinkedIn: [Tu Perfil](www.linkedin.com/in/carlosmaza98)

*Built as part of my Data Engineering portfolio to showcase modern data warehouse practices with DuckDB and Parquet.*

---

## 🙏 Acknowledgments

- [DuckDB](https://duckdb.org/) - Amazing in-process analytical database
- [Apache Parquet](https://parquet.apache.org/) - Efficient columnar storage
- [Kimball Group](https://www.kimballgroup.com/) - Dimensional modeling methodology

---

## 📚 Additional Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Parquet Format Specification](https://parquet.apache.org/docs/)
- [Hive Partitioning Guide](https://duckdb.org/docs/data/partitioning/hive_partitioning.html)
- [Data Warehouse Design](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)

---

**Built with ❤️ for HR Analytics**