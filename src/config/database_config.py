# src/config/database_config.py
"""
Database configuration and factory
Provides easy access to database connections following our interface
"""

from pathlib import Path
from typing import Optional

from config.database_interface import DatabaseInterface
from config.duckdb_adapter import DuckDBAdapter

# Configuration constants
DB_PATH = "dwh.duckdb"
RAW_DATA_PATH = Path("data/raw/case_history")
STAGING_PATH = Path("data/staging")
WAREHOUSE_PATH = Path("data/warehouse")
BACKUP_PATH = Path("backups")

# Database configuration
DEFAULT_MEMORY_LIMIT = "4GB"
DEFAULT_THREADS = 4

def get_database(
    db_type: str = "duckdb",
    db_path: str = DB_PATH,
    **kwargs
) -> DatabaseInterface:
    """
    Factory function to get a database adapter
    
    Args:
        db_type: Type of database ('duckdb', 'postgres', etc.)
        db_path: Path to database file (for file-based DBs)
        **kwargs: Additional database-specific parameters
        
    Returns:
        Database adapter implementing DatabaseInterface
        
    Example:
        >>> db = get_database('duckdb')
        >>> with db:
        ...     df = db.fetch_df("SELECT * FROM cases")
    """
    if db_type.lower() == "duckdb":
        return DuckDBAdapter(
            db_path=db_path,
            memory_limit=kwargs.get('memory_limit', DEFAULT_MEMORY_LIMIT),
            threads=kwargs.get('threads', DEFAULT_THREADS)
        )
    # Future: Add PostgreSQL, MySQL, Snowflake adapters here
    # elif db_type.lower() == "postgres":
    #     return PostgreSQLAdapter(...)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def get_default_database() -> DatabaseInterface:
    """
    Get the default database connection for the project
    
    Returns:
        Configured database adapter
    """
    return get_database('duckdb', DB_PATH)


# Path helper functions (keep existing ones)
from datetime import datetime

def get_raw_data_path(date: datetime) -> Path:
    """Get the path for raw data based on date"""
    return RAW_DATA_PATH / str(date.year) / f"{date.month:02d}" / f"{date.day:02d}"

def get_raw_file_path(date: datetime) -> Path:
    """Get the full file path for a raw parquet file"""
    folder = get_raw_data_path(date)
    filename = f"casos_rrhh_{date.strftime('%Y%m%d')}.parquet"
    return folder / filename