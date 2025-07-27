#!/usr/bin/env python3

"""
Test empty query handling
"""

import socket
import struct

def send_postgres_query(host, port, query):
    """Send a PostgreSQL simple query and print the response"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    
    # Build Query message: 'Q' + length + query + null
    query_bytes = query.encode('utf-8') + b'\0'
    length = 4 + len(query_bytes)
    message = b'Q' + struct.pack('>I', length) + query_bytes
    
    print(f"Sending query: '{query}'")
    sock.send(message)
    
    # Read response
    response = sock.recv(1024)
    print(f"Response length: {len(response)} bytes")
    
    # Parse response types
    pos = 0
    while pos < len(response):
        msg_type = chr(response[pos])
        length = struct.unpack('>I', response[pos+1:pos+5])[0]
        
        if msg_type == 'T':
            print("  RowDescription message received")
        elif msg_type == 'C':
            # Extract command tag
            tag = response[pos+5:pos+length+1].rstrip(b'\0').decode('utf-8')
            print(f"  CommandComplete: {tag}")
        elif msg_type == 'Z':
            status = chr(response[pos+5])
            print(f"  ReadyForQuery: status={status}")
        elif msg_type == 'E':
            print("  Error message received")
        else:
            print(f"  Message type: {msg_type}")
        
        pos += 1 + length
    
    sock.close()
    print()

if __name__ == "__main__":
    print("⚪ Testing Empty Query Handling")
    print("===============================")
    print()
    
    # Note: This requires a running server with startup message handling
    # For demonstration purposes only
    
    queries = [
        "",          # Empty string
        ";",         # Just semicolon
        "   ;   ",   # Whitespace and semicolon
        ";;;",       # Multiple semicolons
        "\n;\n",     # Newlines and semicolon
    ]
    
    print("Example PostgreSQL wire protocol messages for empty queries:")
    print()
    
    for query in queries:
        print(f"Query: '{repr(query)}'")
        print("Expected response:")
        print("  - RowDescription with 0 fields")
        print("  - CommandComplete: SELECT 0")
        print("  - ReadyForQuery: I (idle)")
        print()