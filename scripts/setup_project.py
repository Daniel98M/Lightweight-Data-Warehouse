# scripts/setup_project.py
"""
Project structure initialization script
Creates all necessary folders for the Data Warehouse

This script creates a production-ready folder structure with:
- Hierarchical partitioning (year/month/day) for raw data
- Source code organization in src/
- Separate folders for staging, warehouse, and backups
"""

import os
from pathlib import Path
from datetime import datetime

def create_project_structure():
    """Create the complete folder structure for the project"""
    
    base_path = Path.cwd()
    
    # Base folders
    base_folders = [
        # Data layers - warehouse structure
        "data/staging",
        "data/warehouse",
        
        # Source code - organized by function
        "src/config",
        "src/extraction",
        "src/transformation",
        "src/reporting",

        # Test modules
        "tests"
        
        # SQL queries
        "sql/queries",
        
        # Executable scripts
        "scripts",
        
        # Backups - for weekly snapshots
        "backups",
        
        # Logs - for monitoring and debugging
        "logs"
    ]
    
    print("=" * 60)
    print("DATA WAREHOUSE - PROJECT SETUP")
    print("=" * 60)
    print("\nCreating base folder structure...")
    
    for folder in base_folders:
        folder_path = base_path / folder
        folder_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created: {folder}")
    
    # Create example partitioned structure for raw data (current year/month)
    current_date = datetime.now()
    raw_example_path = (
        base_path / "data" / "raw" / "case_history" / 
        str(current_date.year) / 
        f"{current_date.month:02d}" / 
        f"{current_date.day:02d}"
    )
    raw_example_path.mkdir(parents=True, exist_ok=True)
    print(f"\n✓ Created raw data structure example:")
    print(f"  {raw_example_path.relative_to(base_path)}")
    
    # Create __init__.py files to make src a proper Python package
    init_files = [
        "src/__init__.py",
        "src/config/__init__.py",
        "src/extraction/__init__.py",
        "src/transformation/__init__.py",
        "src/reporting/__init__.py"
    ]
    
    print("\nCreating Python package files...")
    for init_file in init_files:
        init_path = base_path / init_file
        init_path.touch(exist_ok=True)
        print(f"✓ Created: {init_file}")
    
    # Create a .gitignore file (best practice)
    gitignore_content = """# DuckDB database files
*.duckdb
*.duckdb.wal

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.venv/

# Data files (don't commit large data files)
data/raw/**/*.parquet
data/raw/**/*.csv
data/staging/*.parquet
data/warehouse/*.parquet
backups/*.parquet

# Logs
logs/*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
"""
    
    gitignore_path = base_path / ".gitignore"
    if not gitignore_path.exists():
        gitignore_path.write_text(gitignore_content, encoding="utf-8")
        print("\n✓ Created: .gitignore")
    
    # Create a README.md template
    readme_content = f"""# RRHH Data Warehouse

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
│   └── setup_project.py        # This setup script
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
.dwhenv\Scripts\activate
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
- Created on: {current_date.strftime('%Y-%m-%d')}
"""
    
    readme_path = base_path / "README.md"
    if not readme_path.exists():
        readme_path.write_text(readme_content, encoding="utf-8")
        print("✓ Created: README.md")
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ PROJECT STRUCTURE CREATED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n📁 Base path: {base_path}")
    print(f"📅 Example raw data path created for: {current_date.strftime('%Y-%m-%d')}")
    print("\n📋 Next steps:")
    print("  1. Run: pip install duckdb pandas pyarrow")
    print("  2. Run: python scripts/test_connection.py")
    print("  3. Start building your data warehouse!")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    create_project_structure()