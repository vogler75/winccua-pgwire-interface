#!/usr/bin/env python3
"""
Test script that continuously connects and disconnects from the PostgreSQL server
to verify proper session cleanup on disconnect.
"""

import psycopg2
import time
import sys
import argparse
import random
import socket
from datetime import datetime

def test_connection_loop(host='localhost', port=5432, user='username1', password='password1', 
                        database='winccua', delay=1.0, verbose=False, kill_chance=0.0):
    """
    Continuously connect and disconnect from the PostgreSQL server.
    
    Args:
        host: Server hostname
        port: Server port
        user: Username for authentication
        password: Password for authentication
        database: Database name
        delay: Delay between connections in seconds
        verbose: Print detailed connection info
        kill_chance: Probability (0.0-1.0) of abruptly closing connections
    """
    connection_count = 0
    error_count = 0
    
    print(f"Starting connection loop test to {host}:{port}")
    print(f"User: {user}, Database: {database}")
    print(f"Delay between connections: {delay}s")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            try:
                # Connect to the server
                start_time = time.time()
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database,
                    connect_timeout=5
                )
                connect_time = time.time() - start_time
                
                connection_count += 1
                
                if verbose:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                          f"Connection #{connection_count} established in {connect_time:.3f}s")
                
                # Optionally execute a simple query
                #cursor = conn.cursor()
                #cursor.execute("SELECT 1")
                #result = cursor.fetchone()
                
                #if verbose and result:
                    print(f"  Query result: {result[0]}")
                
                #cursor.close()

                # Wait before next connection
                time.sleep(delay)

                
                # Simulate abrupt disconnection some percentage of the time
                if kill_chance > 0 and random.random() < kill_chance:
                    if verbose:
                        print(f"  Simulating abrupt disconnection (kill -9)")
                    # Abruptly close the underlying socket to simulate SIGKILL
                    try:
                        conn.get_backend_pid()  # This forces the connection to be active
                        # Get the underlying socket and close it without proper shutdown
                        sock = conn.get_dsn_parameters()
                        conn.close()  # This will cause an abrupt close
                    except:
                        pass  # Connection might already be closed
                else:
                    # Normal graceful close
                    conn.close()
                
                if connection_count % 10 == 0 and not verbose:
                    print(f"Completed {connection_count} connections (errors: {error_count})")
                
            except psycopg2.Error as e:
                error_count += 1
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"Connection error #{error_count}: {e}")
            
            except Exception as e:
                error_count += 1
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"Unexpected error #{error_count}: {e}")
                        
    except KeyboardInterrupt:
        print(f"\n\nTest completed:")
        print(f"  Total connections: {connection_count}")
        print(f"  Total errors: {error_count}")
        print(f"  Success rate: {(connection_count/(connection_count+error_count)*100):.1f}%")

def main():
    parser = argparse.ArgumentParser(description='Test PostgreSQL connection/disconnection loop')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--user', default='username1', help='Username (default: username1)')
    parser.add_argument('--password', default='password1', help='Password (default: password1)')
    parser.add_argument('--database', default='winccua', help='Database name (default: winccua)')
    parser.add_argument('--delay', type=float, default=1.0, 
                        help='Delay between connections in seconds (default: 1.0)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                        help='Print detailed connection information')
    parser.add_argument('--kill-chance', type=float, default=0.0, metavar='PROB',
                        help='Probability (0.0-1.0) of abruptly closing connections to test cleanup (default: 0.0)')
    
    args = parser.parse_args()
    
    test_connection_loop(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        delay=args.delay,
        verbose=args.verbose,
        kill_chance=args.kill_chance
    )

if __name__ == '__main__':
    main()