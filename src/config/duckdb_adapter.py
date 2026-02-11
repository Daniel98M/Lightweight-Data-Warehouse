# src/config/duckdb_adapter.py
"""
DuckDB implementation of the database interface
"""

import duckdb
import pandas as pd
from pathlib import Path
from typing import Any, List, Optional

from config.database_interface import DatabaseInterface

class DuckDBAdapter(DatabaseInterface):
    """
    DuckDB-specific implementation of the database interface
    
    This adapter wraps DuckDB functionality to conform to our
    standard database interface, making it easy to swap databases later.
    """
    
    def __init__(
        self, 
        db_path: str = "rrhh.duckdb",
        memory_limit: str = "4GB",
        threads: int = 4
    ):
        """
        Initialize DuckDB adapter
        
        Args:
            db_path: Path to DuckDB database file
            memory_limit: Memory limit for DuckDB (e.g., '4GB', '8GB')
            threads: Number of threads for parallel processing
        """
        self.db_path = Path(db_path)
        self.memory_limit = memory_limit
        self.threads = threads
        self.conn = None
    
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Establish connection to DuckDB"""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path))
            
            # Configure DuckDB settings
            self.conn.execute(f"SET memory_limit='{self.memory_limit}'")
            self.conn.execute(f"SET threads={self.threads}")
            
            print(f"✓ Connected to DuckDB: {self.db_path}")
            print(f"  Memory limit: {self.memory_limit}")
            print(f"  Threads: {self.threads}")
        
        return self.conn
    
    def close(self) -> None:
        """Close DuckDB connection"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            print("✓ DuckDB connection closed")
    
    def execute(self, query: str, params: Optional[List] = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query"""
        if self.conn is None:
            self.connect()
        
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)
    
    def execute_many(self, query: str, params_list: List[List]) -> None:
        """Execute query multiple times with different parameters"""
        if self.conn is None:
            self.connect()
        
        for params in params_list:
            self.conn.execute(query, params)
    
    def fetch_one(self, query: str, params: Optional[List] = None) -> Optional[tuple]:
        """Execute query and fetch a single row"""
        result = self.execute(query, params)
        return result.fetchone()
    
    def fetch_all(self, query: str, params: Optional[List] = None) -> List[tuple]:
        """Execute query and fetch all rows"""
        result = self.execute(query, params)
        return result.fetchall()
    
    def fetch_df(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        result = self.execute(query, params)
        return result.df()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = ?
        """
        result = self.fetch_one(query, [table_name])
        return result[0] > 0 if result else False
    
    def create_table_from_df(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'fail'
    ) -> None:
        """Create a table from a DataFrame"""
        if self.conn is None:
            self.connect()
        
        # Check if table exists
        exists = self.table_exists(table_name)
        
        if exists and if_exists == 'fail':
            raise ValueError(f"Table '{table_name}' already exists")
        elif exists and if_exists == 'replace':
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table from DataFrame
        if if_exists == 'append' and exists:
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        else:
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        print(f"✓ Table '{table_name}' created from DataFrame ({len(df)} rows)")
    
    def create_table_from_parquet(
        self, 
        parquet_path: Path, 
        table_name: str,
        if_exists: str = 'fail'
    ) -> None:
        """Create a table from a Parquet file"""
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
        
        # Check if table exists
        exists = self.table_exists(table_name)
        
        if exists and if_exists == 'fail':
            raise ValueError(f"Table '{table_name}' already exists")
        elif exists and if_exists == 'replace':
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table from Parquet
        if if_exists == 'append' and exists:
            query = f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_path}')"
        else:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
        
        self.execute(query)
        
        # Get row count
        count = self.fetch_one(f"SELECT COUNT(*) FROM {table_name}")[0]
        print(f"✓ Table '{table_name}' created from Parquet ({count} rows)")
    
    def export_to_parquet(
        self, 
        query: str, 
        output_path: Path,
        params: Optional[List] = None
    ) -> None:
        """Execute query and export results to Parquet"""
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export to Parquet
        export_query = f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        self.execute(export_query, params)
        
        print(f"✓ Data exported to: {output_path}")
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get schema information for a table"""
        query = f"DESCRIBE {table_name}"
        return self.fetch_df(query)
    
    def commit(self) -> None:
        """Commit transaction (DuckDB auto-commits by default)"""
        if self.conn is not None:
            self.conn.commit()
    
    def rollback(self) -> None:
        """Rollback transaction"""
        if self.conn is not None:
            self.conn.rollback()
    
    def vacuum(self) -> None:
        """Optimize database file size (DuckDB specific)"""
        if self.conn is None:
            self.connect()
        self.conn.execute("CHECKPOINT")
        print("✓ Database optimized (CHECKPOINT)")