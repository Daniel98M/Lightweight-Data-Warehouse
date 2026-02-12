# src/transformation/duckdb_reader.py
"""
Read Parquet files using DuckDB with Hive partition optimization
Demonstrates DuckDB's powerful Parquet reading capabilities
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime
from typing import Optional, List
from config.database_config import get_default_database, RAW_DATA_PATH

class DuckDBReader:
    """
    Read and query Parquet files using DuckDB
    Leverages DuckDB's native Hive partitioning support
    """
    
    def __init__(self):
        """Initialize DuckDB reader"""
        self.db = get_default_database()
        self.raw_path = RAW_DATA_PATH
    
    def read_all_cases(self) -> pd.DataFrame:
        """
        Read ALL cases from all partitions
        DuckDB automatically handles Hive partitioning
        
        Returns:
            DataFrame with all cases
            
        Example:
            >>> reader = DuckDBReader()
            >>> df = reader.read_all_cases()
            >>> print(f"Total cases: {len(df)}")
        """
        with self.db as conn:
            query = f"""
                SELECT * 
                FROM read_parquet('{self.raw_path}/**/*.parquet', 
                                  hive_partitioning = true)
            """
            
            print("📊 Reading all cases from Hive partitions...")
            df = conn.fetch_df(query)
            print(f"✓ Loaded {len(df):,} rows")
            
            return df
    
    def read_cases_by_date(
        self, 
        year: int, 
        month: Optional[int] = None,
        day: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read cases filtered by date partition
        Uses partition pruning for performance
        
        Args:
            year: Year to filter
            month: Optional month to filter
            day: Optional day to filter
            
        Returns:
            Filtered DataFrame
            
        Example:
            >>> # Get all cases from January 2025
            >>> df = reader.read_cases_by_date(year=2025, month=1)
            
            >>> # Get cases from specific day
            >>> df = reader.read_cases_by_date(year=2025, month=2, day=11)
        """
        with self.db as conn:
            # Build WHERE clause based on provided filters
            where_clauses = [f"year = {year}"]
            
            if month is not None:
                where_clauses.append(f"month = {month}")
            
            if day is not None:
                where_clauses.append(f"day = {day}")
            
            where_clause = " AND ".join(where_clauses)
            
            query = f"""
                SELECT * 
                FROM read_parquet('{self.raw_path}/**/*.parquet', 
                                  hive_partitioning = true)
                WHERE {where_clause}
            """
            
            print(f"📊 Reading cases: {where_clause}")
            df = conn.fetch_df(query)
            print(f"✓ Loaded {len(df):,} rows")
            
            return df
    
    def read_cases_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Read cases within a date range
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            Filtered DataFrame
            
        Example:
            >>> from datetime import datetime
            >>> df = reader.read_cases_by_date_range(
            ...     start_date=datetime(2025, 1, 1),
            ...     end_date=datetime(2025, 1, 31)
            ... )
        """
        with self.db as conn:
            query = f"""
                SELECT * 
                FROM read_parquet('{self.raw_path}/**/*.parquet', 
                                  hive_partitioning = true)
                WHERE year >= {start_date.year} 
                  AND year <= {end_date.year}
                  AND month >= {start_date.month}
                  AND month <= {end_date.month}
            """
            
            print(f"📊 Reading cases from {start_date.date()} to {end_date.date()}")
            df = conn.fetch_df(query)
            print(f"✓ Loaded {len(df):,} rows")
            
            return df
    
    def get_partition_stats(self) -> pd.DataFrame:
        """
        Get statistics per partition (year/month/day)
        
        Returns:
            DataFrame with count of cases per partition
            
        Example:
            >>> stats = reader.get_partition_stats()
            >>> print(stats)
        """
        with self.db as conn:
            query = f"""
                SELECT 
                    year,
                    month,
                    day,
                    COUNT(*) as case_count,
                    COUNT(DISTINCT CASE_ID) as unique_cases
                FROM read_parquet('{self.raw_path}/**/*.parquet', 
                                  hive_partitioning = true)
                GROUP BY year, month, day
                ORDER BY year DESC, month DESC, day DESC
            """
            
            print("📊 Calculating partition statistics...")
            df = conn.fetch_df(query)
            print(f"✓ Found {len(df)} partitions")
            
            return df
    
    def query_cases(self, sql_filter: str) -> pd.DataFrame:
        """
        Execute custom SQL query on all cases
        
        Args:
            sql_filter: SQL WHERE clause or full query
            
        Returns:
            Query results as DataFrame
            
        Example:
            >>> # Simple filter
            >>> df = reader.query_cases("COUNTRY = 'Mexico'")
            
            >>> # Complex query
            >>> df = reader.query_cases('''
            ...     STATUS = 'Resolved' 
            ...     AND year = 2025 
            ...     AND month = 1
            ... ''')
        """
        with self.db as conn:
            # Check if it's a full query or just a WHERE clause
            if sql_filter.strip().upper().startswith('SELECT'):
                query = sql_filter
            else:
                query = f"""
                    SELECT * 
                    FROM read_parquet('{self.raw_path}/**/*.parquet', 
                                      hive_partitioning = true)
                    WHERE {sql_filter}
                """
            
            print(f"📊 Executing custom query...")
            df = conn.fetch_df(query)
            print(f"✓ Returned {len(df):,} rows")
            
            return df