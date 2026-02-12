# scripts/load_cases.py
"""
Script to load cases from CSV/Excel into Parquet (RAW layer)
Usage: python scripts/load_cases.py <file_path> [--date YYYY-MM-DD]
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import argparse
from datetime import datetime
from extraction.parquet_loader import ParquetLoader

def main():
    parser = argparse.ArgumentParser(
        description='Load cases from CSV/Excel into Parquet with Hive partitioning'
    )
    parser.add_argument(
        'file_path',
        type=str,
        help='Path to CSV or Excel file'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Extraction date in YYYY-MM-DD format (default: today)',
        default=None
    )
    
    args = parser.parse_args()
    
    # Parse extraction date
    if args.date:
        extraction_date = datetime.strptime(args.date, '%Y-%m-%d')
    else:
        extraction_date = datetime.now()
    
    # Initialize loader
    loader = ParquetLoader()
    
    # Determine file type and load
    file_path = Path(args.file_path)
    
    if not file_path.exists():
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    
    print("=" * 60)
    print("📦 LOADING cases TO PARQUET")
    print("=" * 60)
    print(f"Source file: {file_path.name}")
    print(f"Extraction date: {extraction_date.date()}")
    print("=" * 60)
    
    try:
        if file_path.suffix.lower() == '.csv':
            output_path = loader.load_from_csv(file_path, extraction_date)
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            output_path = loader.load_from_excel(file_path, extraction_date)
        else:
            print(f"❌ Error: Unsupported file type: {file_path.suffix}")
            sys.exit(1)
        
        print("\n" + "=" * 60)
        print("✅ LOAD COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"Output: {output_path}")
        print("\nNext steps:")
        print("  1. Run: python scripts/query_cases.py")
        print("  2. View summary: python scripts/view_summary.py")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error during load: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()