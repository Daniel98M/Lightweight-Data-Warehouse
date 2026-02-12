# scripts/query_cases.py
"""
Script to query cases from Parquet using DuckDB
Demonstrates Hive partitioning benefits
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transformation.duckdb_reader import DuckDBReader
from datetime import datetime

def main():
    reader = DuckDBReader()
    
    print("=" * 60)
    print("🔍 QUERY cases - DUCKDB + HIVE PARTITIONS")
    print("=" * 60)
    
    # Example 1: Get partition statistics
    print("\n📊 Partition Statistics:")
    print("-" * 60)
    stats = reader.get_partition_stats()
    print(stats.to_string(index=False))
    
    # Example 2: Read all cases
    print("\n\n📊 Reading all cases:")
    print("-" * 60)
    df_all = reader.read_all_cases()
    print(f"Total rows: {len(df_all):,}")
    print(f"Columns: {list(df_all.columns)}")
    
    # Example 3: Read by specific month
    print("\n\n📊 Cases from February 2025:")
    print("-" * 60)
    df_feb = reader.read_cases_by_date(year=2025, month=2)
    print(f"Rows: {len(df_feb):,}")
    
    # Example 4: Custom query
    print("\n\n📊 Custom Query Example (Resolved cases):")
    print("-" * 60)
    df_resolved = reader.query_cases("STATUS = 'Resolved'")
    print(f"Resolved cases: {len(df_resolved):,}")
    
    print("\n" + "=" * 60)
    print("✅ QUERY EXAMPLES COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
    main()