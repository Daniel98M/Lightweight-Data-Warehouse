# src/config/file_utils.py
"""
File system utilities for data warehouse
Handles path creation and file management with hierarchical partitioning
"""

from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

def ensure_path_exists(path: Path) -> Path:
    """
    Create path if it doesn't exist
    
    Args:
        path: Path to create
        
    Returns:
        The path object
        
    Example:
        >>> ensure_path_exists(Path("data/raw/case_history/2025/01/15"))
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_latest_raw_file() -> Optional[Path]:
    """
    Find the most recent raw parquet file
    
    Returns:
        Path to latest file or None if no files exist
        
    Example:
        >>> latest = get_latest_raw_file()
        >>> print(latest)
        data/raw/case_history/2025/01/15/case_history_20250115.parquet
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config.database_config import RAW_DATA_PATH
    
    parquet_files = list(RAW_DATA_PATH.rglob("*.parquet"))
    
    if not parquet_files:
        return None
    
    # Sort by modification time (most recent first)
    return max(parquet_files, key=lambda p: p.stat().st_mtime)


def list_raw_files_in_range(start_date: datetime, end_date: datetime) -> List[Path]:
    """
    List all raw files between two dates
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        List of paths to parquet files that exist in the range
        
    Example:
        >>> from datetime import datetime
        >>> files = list_raw_files_in_range(
        ...     datetime(2025, 1, 1),
        ...     datetime(2025, 1, 31)
        ... )
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config.database_config import get_raw_file_path
    
    files = []
    current = start_date
    
    while current <= end_date:
        file_path = get_raw_file_path(current)
        
        if file_path.exists():
            files.append(file_path)
        
        current += timedelta(days=1)
    
    return files


def get_file_size_mb(file_path: Path) -> float:
    """
    Get file size in megabytes
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB
    """
    if not file_path.exists():
        return 0.0
    
    size_bytes = file_path.stat().st_size
    return size_bytes / (1024 * 1024)


def list_all_raw_files() -> List[Path]:
    """
    List all raw parquet files in chronological order
    
    Returns:
        Sorted list of all raw parquet files
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config.database_config import RAW_DATA_PATH
    
    parquet_files = list(RAW_DATA_PATH.rglob("*.parquet"))
    
    # Sort by file path (which follows chronological order due to naming)
    return sorted(parquet_files)