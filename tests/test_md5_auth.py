#!/usr/bin/env python3

"""
Test MD5 authentication with the PostgreSQL wire protocol server
"""

import psycopg2
import sys

def test_md5_auth():
    print("🔐 Testing MD5 Authentication")
    print("=" * 40)
    
    try:
        # Test connection with MD5 authentication
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="username1",
            password="password1",
            database="test",
            sslmode="disable"
        )
        
        print("✅ Connection successful!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 'MD5 auth working' as status")
        result = cursor.fetchone()
        print(f"✅ Query result: {result[0]}")
        
        cursor.close()
        conn.close()
        
        print("✅ MD5 authentication test passed!")
        return True
        
    except Exception as e:
        print(f"❌ MD5 authentication test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_md5_auth()
    sys.exit(0 if success else 1)