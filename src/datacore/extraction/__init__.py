# src/datacore/extraction/__init__.py
"""
Data extraction and loading utilities
"""

from datacore.extraction.parquet_loader import ParquetLoader

__all__ = [
    "ParquetLoader",
]