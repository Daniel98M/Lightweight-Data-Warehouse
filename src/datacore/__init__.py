# src/datacore/__init__.py
"""
DataCore - Lightweight Data Warehouse
Modern HR analytics using DuckDB and Parquet
"""

__version__ = "0.1.0"

# Import main classes for easy access
from datacore.config.duckdb_adapter import DuckDBAdapter
from datacore.extraction.parquet_loader import ParquetLoader

__all__ = [
    "DuckDBAdapter",
    "ParquetLoader",
]