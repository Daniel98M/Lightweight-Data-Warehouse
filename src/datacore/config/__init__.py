# src/datacore/config/__init__.py
"""
Configuration and database connection management
"""

from datacore.config.database_interface import DatabaseInterface
from datacore.config.duckdb_adapter import DuckDBAdapter

__all__ = [
    "DatabaseInterface",
    "DuckDBAdapter",
]