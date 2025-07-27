#!/usr/bin/env python3
import psycopg2
from datetime import datetime

# Test queries with CURRENT_DATE and CURRENT_TIME
test_queries = [
    # Test CURRENT_DATE as identifier
    "SELECT * FROM loggedtagvalues WHERE tag_name = 'TestTag' AND timestamp >= CURRENT_DATE",
    
    # Test CURRENT_DATE with function syntax
    "SELECT * FROM loggedtagvalues WHERE tag_name = 'TestTag' AND timestamp >= CURRENT_DATE()",
    
    # Test CURRENT_TIME
    "SELECT * FROM loggedtagvalues WHERE tag_name = 'TestTag' AND timestamp <= CURRENT_TIME",
    
    # Test with loggedalarms
    "SELECT * FROM loggedalarms WHERE timestamp >= CURRENT_DATE",
    
    # Test in BETWEEN clause
    "SELECT * FROM loggedtagvalues WHERE tag_name = 'TestTag' AND timestamp BETWEEN CURRENT_DATE AND CURRENT_TIME",
    
    # Test CURRENT_TIMESTAMP
    "SELECT * FROM loggedtagvalues WHERE tag_name = 'TestTag' AND timestamp >= CURRENT_TIMESTAMP",
    
    # Test NOW()
    "SELECT * FROM loggedtagvalues WHERE tag_name = 'TestTag' AND timestamp >= NOW()",
]

try:
    # Connect to the server
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="username1",
        password="password1",
        database="postgres"
    )
    
    print("Connected successfully!")
    
    for query in test_queries:
        print(f"\nTesting query: {query}")
        try:
            cur = conn.cursor()
            cur.execute(query)
            # Try to fetch results (may fail if backend is not available)
            try:
                results = cur.fetchall()
                print(f"✓ Query executed successfully, returned {len(results)} rows")
            except Exception as e:
                print(f"✓ Query parsed successfully (backend error: {e})")
            cur.close()
        except Exception as e:
            print(f"✗ Query failed: {e}")
    
    conn.close()
    
except Exception as e:
    print(f"Connection failed: {e}")