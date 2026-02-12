# src/extraction/parquet_loader.py
"""
Load CSV/Excel files into Parquet with Hive-style partitioning
Handles the RAW layer of the data warehouse
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime
from typing import Union, Optional
from config.database_config import RAW_DATA_PATH
from config.file_utils import ensure_path_exists

class ParquetLoader:
    """
    Handles loading source data (CSV/Excel) into Parquet format
    with Hive-style partitioning (year=YYYY/month=MM/day=DD/)
    """
    
    def __init__(self, base_path: Path = RAW_DATA_PATH):
        """
        Initialize ParquetLoader
        
        Args:
            base_path: Base path for raw data storage
        """
        self.base_path = base_path
    
    def _get_hive_partition_path(self, extraction_date: datetime) -> Path:
        """
        Generate Hive-style partition path
        
        Args:
            extraction_date: Date of data extraction
            
        Returns:
            Path with Hive partitioning: year=YYYY/month=MM/day=DD/
            
        Example:
            >>> _get_hive_partition_path(datetime(2025, 2, 11))
            Path('data/raw/case_history/year=2025/month=02/day=11')
        """
        return (
            self.base_path / 
            f"year={extraction_date.year}" / 
            f"month={extraction_date.month:02d}" / 
            f"day={extraction_date.day:02d}"
        )
    
    def load_from_csv(
        self, 
        csv_path: Union[str, Path],
        extraction_date: Optional[datetime] = None,
        **pandas_kwargs
    ) -> Path:
        """
        Load CSV into Parquet with Hive partitioning
        
        Args:
            csv_path: Path to source CSV file
            extraction_date: Date of extraction (defaults to today)
            **pandas_kwargs: Additional arguments for pd.read_csv()
            
        Returns:
            Path to created Parquet file
            
        Example:
            >>> loader = ParquetLoader()
            >>> parquet_path = loader.load_from_csv(
            ...     'downloads/casos_20250211.csv',
            ...     extraction_date=datetime(2025, 2, 11)
            ... )
        """
        csv_path = Path(csv_path)
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Default to today if no date provided
        if extraction_date is None:
            extraction_date = datetime.now()
        
        print(f"📥 Loading CSV: {csv_path.name}")
        
        # Read CSV
        df = pd.read_csv(csv_path, **pandas_kwargs)
        
        print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        # Save to Parquet with Hive partitioning
        return self._save_to_parquet(df, extraction_date)
    
    def load_from_excel(
        self,
        excel_path: Union[str, Path],
        extraction_date: Optional[datetime] = None,
        sheet_name: Union[str, int] = 0,
        **pandas_kwargs
    ) -> Path:
        """
        Load Excel into Parquet with Hive partitioning
        
        Args:
            excel_path: Path to source Excel file
            extraction_date: Date of extraction (defaults to today)
            sheet_name: Sheet name or index to read
            **pandas_kwargs: Additional arguments for pd.read_excel()
            
        Returns:
            Path to created Parquet file
        """
        excel_path = Path(excel_path)
        
        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
        
        # Default to today if no date provided
        if extraction_date is None:
            extraction_date = datetime.now()
        
        print(f"📥 Loading Excel: {excel_path.name}")
        
        # Read Excel
        df = pd.read_excel(excel_path, sheet_name=sheet_name, **pandas_kwargs)
        
        print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        # Save to Parquet with Hive partitioning
        return self._save_to_parquet(df, extraction_date)
    
    def _save_to_parquet(
        self, 
        df: pd.DataFrame, 
        extraction_date: datetime
    ) -> Path:
        """
        Save DataFrame to Parquet with Hive partitioning
        
        Args:
            df: DataFrame to save
            extraction_date: Date for partitioning
            
        Returns:
            Path to saved Parquet file
        """
        # Generate Hive partition path
        partition_path = self._get_hive_partition_path(extraction_date)
        ensure_path_exists(partition_path)
        
        # Generate filename
        filename = f"case_history_{extraction_date.strftime('%Y%m%d')}.parquet"
        output_path = partition_path / filename
        
        # Save with compression
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='zstd',  # Better compression than snappy
            index=False
        )
        
        # Get file size
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        print(f"✓ Saved to: {output_path.relative_to(Path.cwd())}")
        print(f"  File size: {file_size_mb:.2f} MB")
        print(f"  Compression: ZSTD")
        
        return output_path
    
    def get_data_summary(self) -> pd.DataFrame:
        """
        Get summary of all Parquet files in raw layer
        
        Returns:
            DataFrame with file information
        """
        files = list(self.base_path.rglob("*.parquet"))
        
        if not files:
            print("⚠️  No Parquet files found in raw layer")
            return pd.DataFrame()
        
        summary = []
        for file_path in sorted(files):
            # Extract partition values from path
            parts = file_path.parts
            year = month = day = None
            
            for part in parts:
                if part.startswith('year='):
                    year = part.split('=')[1]
                elif part.startswith('month='):
                    month = part.split('=')[1]
                elif part.startswith('day='):
                    day = part.split('=')[1]
            
            # Get file stats
            size_mb = file_path.stat().st_size / (1024 * 1024)
            modified = datetime.fromtimestamp(file_path.stat().st_mtime)
            
            summary.append({
                'year': year,
                'month': month,
                'day': day,
                'filename': file_path.name,
                'size_mb': round(size_mb, 2),
                'modified_at': modified,
                'full_path': str(file_path)
            })
        
        return pd.DataFrame(summary)