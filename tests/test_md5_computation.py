#!/usr/bin/env python3

"""
Test MD5 computation to verify our PostgreSQL MD5 authentication implementation.
This script computes the PostgreSQL MD5 hash manually to compare with our Rust implementation.
"""

import hashlib

def compute_postgres_md5_hash(username, password, salt):
    """
    Compute PostgreSQL MD5 authentication hash.
    PostgreSQL MD5 authentication: MD5(MD5(password + username) + salt)
    """
    # Step 1: MD5(password + username)
    inner_input = password.encode('utf-8') + username.encode('utf-8')
    inner_hash = hashlib.md5(inner_input).hexdigest()
    
    # Step 2: MD5(inner_hex + salt)
    final_input = inner_hash.encode('utf-8') + salt
    final_hash = hashlib.md5(final_input).hexdigest()
    
    # PostgreSQL prefixes the result with "md5"
    return f"md5{final_hash}"

def test_md5_computation():
    """Test cases for MD5 computation"""
    
    print("🔐 Testing PostgreSQL MD5 Authentication Hash Computation")
    print("=" * 60)
    
    test_cases = [
        {
            "username": "username1",
            "password": "password1",
            "salt": bytes([0x12, 0x34, 0x56, 0x78]),
            "description": "Basic test case"
        },
        {
            "username": "grafana", 
            "password": "password1",
            "salt": bytes([0x12, 0x34, 0x56, 0x78]),
            "description": "Grafana user test"
        },
        {
            "username": "testuser",
            "password": "password1", 
            "salt": bytes([0xAB, 0xCD, 0xEF, 0x12]),
            "description": "Different salt test"
        }
    ]
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n🔍 Test Case {i}: {test['description']}")
        print(f"   Username: {test['username']}")
        print(f"   Password: {test['password']}")
        print(f"   Salt: {test['salt'].hex()}")
        
        result = compute_postgres_md5_hash(test['username'], test['password'], test['salt'])
        print(f"   Result: {result}")
        
        # Show the intermediate steps
        inner_input = test['password'].encode('utf-8') + test['username'].encode('utf-8')
        inner_hash = hashlib.md5(inner_input).hexdigest()
        print(f"   Step 1 (MD5(password+username)): {inner_hash}")
        
        final_input = inner_hash.encode('utf-8') + test['salt']
        final_hash = hashlib.md5(final_input).hexdigest()
        print(f"   Step 2 (MD5(step1+salt)): {final_hash}")

if __name__ == "__main__":
    test_md5_computation()
    
    print(f"\n💡 To manually test with psql:")
    print(f"   1. Connect to the server")
    print(f"   2. The server will send an MD5 auth request with a salt")
    print(f"   3. psql will compute the hash using the formula above")
    print(f"   4. Our server will verify it matches")