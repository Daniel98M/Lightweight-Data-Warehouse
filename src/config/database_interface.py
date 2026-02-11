# src/config/database_interface.py
"""
Abstract database interface
Defines the contract that any database adapter must implement
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional
from pathlib import Path
import pandas as pd

class DatabaseInterface(ABC):
    """
    Abstract base class for database operations
    
    This interface defines the contract that any database implementation
    must follow. This allows easy switching between DuckDB, PostgreSQL,
    Snowflake, or any other database system.
    """
    
    @abstractmethod
    def connect(self) -> Any:
        """
        Establish connection to the database
        
        Returns:
            Connection object specific to the database implementation
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the database connection"""
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a SQL query
        
        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
            
        Returns:
            Query result object
        """
        pass
    
    @abstractmethod
    def execute_many(self, query: str, params_list: List[Dict]) -> None:
        """
        Execute the same query multiple times with different parameters
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
        """
        pass
    
    @abstractmethod
    def fetch_one(self, query: str, params: Optional[Dict] = None) -> Optional[tuple]:
        """
        Execute query and fetch a single row
        
        Args:
            query: SQL query string
            params: Optional parameters
            
        Returns:
            Single row as tuple or None
        """
        pass
    
    @abstractmethod
    def fetch_all(self, query: str, params: Optional[Dict] = None) -> List[tuple]:
        """
        Execute query and fetch all rows
        
        Args:
            query: SQL query string
            params: Optional parameters
            
        Returns:
            List of rows as tuples
        """
        pass
    
    @abstractmethod
    def fetch_df(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame
        
        Args:
            query: SQL query string
            params: Optional parameters
            
        Returns:
            Query results as DataFrame
        """
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
        """
        pass
    
    @abstractmethod
    def create_table_from_df(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'fail'
    ) -> None:
        """
        Create a table from a pandas DataFrame
        
        Args:
            df: Source DataFrame
            table_name: Name for the new table
            if_exists: Action if table exists ('fail', 'replace', 'append')
        """
        pass
    
    @abstractmethod
    def create_table_from_parquet(
        self, 
        parquet_path: Path, 
        table_name: str,
        if_exists: str = 'fail'
    ) -> None:
        """
        Create a table from a Parquet file
        
        Args:
            parquet_path: Path to Parquet file
            table_name: Name for the new table
            if_exists: Action if table exists ('fail', 'replace', 'append')
        """
        pass
    
    @abstractmethod
    def export_to_parquet(
        self, 
        query: str, 
        output_path: Path,
        params: Optional[Dict] = None
    ) -> None:
        """
        Execute query and export results to Parquet
        
        Args:
            query: SQL query string
            output_path: Path for output Parquet file
            params: Optional parameters
        """
        pass
    
    @abstractmethod
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with column names, types, and constraints
        """
        pass
    
    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction"""
        pass
    
    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction"""
        pass
    
    def __enter__(self):
        """Context manager entry"""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type is not None:
            self.rollback()
        self.close()