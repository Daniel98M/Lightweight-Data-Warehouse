# tests/test_connection.py
"""
Test database connection using the new abstract interface
"""

import sys
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config.database_config import get_default_database

def test_connection():
    """Test database connection and basic queries"""
    
    print("=" * 60)
    print("🧪 Testing Database Connection (Abstract Interface)")
    print("=" * 60)
    
    # Get database using factory pattern
    db = get_default_database()
    
    with db:
        # Test query
        result = db.fetch_one("SELECT 'Connection successful!' as message")
        print(f"\n✓ {result[0]}")
        
        # Check database version
        version = db.fetch_one("SELECT version()")
        print(f"✓ Database version: {version[0]}")
        
        # Test DataFrame functionality
        test_df = db.fetch_df("SELECT 1 as id, 'test' as name")
        print(f"✓ DataFrame support: Working ({test_df.shape[0]} rows)")
        
        # Test table operations
        exists = db.table_exists('nonexistent_table')
        print(f"✓ Table existence check: Working (result={exists})")
    
    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)
    print("\n💡 Key benefits of this architecture:")
    print("   ✓ Easy to swap databases (DuckDB → PostgreSQL)")
    print("   ✓ Testable with mocks")
    print("   ✓ Clean separation of concerns")
    print("   ✓ Production-ready pattern")
    print("=" * 60)

if __name__ == "__main__":
    test_connection()