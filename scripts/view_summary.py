# scripts/view_summary.py
"""
View summary of all Parquet files in RAW layer
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from extraction.parquet_loader import ParquetLoader

def main():
    loader = ParquetLoader()
    
    print("=" * 60)
    print("📋 RAW LAYER SUMMARY")
    print("=" * 60)
    
    summary = loader.get_data_summary()
    
    if summary.empty:
        print("\n⚠️  No data files found in RAW layer")
        print("\nTo load data, run:")
        print("  python scripts/load_casos.py <file_path>")
    else:
        print(f"\n📊 Found {len(summary)} file(s):\n")
        print(summary.to_string(index=False))
        
        total_size = summary['size_mb'].sum()
        print(f"\n💾 Total size: {total_size:.2f} MB")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()