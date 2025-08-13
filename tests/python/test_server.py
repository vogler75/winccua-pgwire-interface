#!/usr/bin/env python3
"""
WinCC UA PostgreSQL Wire Protocol Server Test Suite
Reads SQL queries from queries.txt and executes them against the server.
"""

import sys
import argparse
import time
import os
import traceback
from typing import List, Tuple, Optional

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("❌ Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from colorama import init, Fore, Style
    init(autoreset=True)
    COLOR_SUPPORT = True
except ImportError:
    COLOR_SUPPORT = False
    class Fore:
        RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = ""
    class Style:
        DIM = NORMAL = BRIGHT = RESET_ALL = ""

try:
    from tabulate import tabulate
    TABULATE_SUPPORT = True
except ImportError:
    TABULATE_SUPPORT = False

class Colors:
    """Color constants for output formatting"""
    if COLOR_SUPPORT:
        SUCCESS = Fore.GREEN
        ERROR = Fore.RED
        WARNING = Fore.YELLOW
        INFO = Fore.CYAN
        QUERY = Fore.MAGENTA
        RESULT = Fore.BLUE
        HEADER = Fore.WHITE + Style.BRIGHT
        DIM = Style.DIM
        RESET = Style.RESET_ALL
    else:
        SUCCESS = ERROR = WARNING = INFO = QUERY = RESULT = HEADER = DIM = RESET = ""

class QueryTester:
    """Test suite for WinCC UA PostgreSQL Wire Protocol Server"""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 user: str = "testuser", password: str = "password1",
                 database: str = "winccua", sslmode: str = "disable",
                 timeout: int = 30, verbose: bool = False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.timeout = timeout
        self.verbose = verbose
        self.connection = None
    
    def connect(self) -> bool:
        """Connect to the PostgreSQL server"""
        try:
            conn_params = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'password': self.password,
                'database': self.database,
                'sslmode': self.sslmode,
                'connect_timeout': self.timeout
            }
            
            print(f"{Colors.INFO}🔌 Connecting to WinCC UA PostgreSQL server...{Colors.RESET}")
            print(f"{Colors.DIM}   Host: {self.host}:{self.port}{Colors.RESET}")
            print(f"{Colors.DIM}   User: {self.user}{Colors.RESET}")
            print(f"{Colors.DIM}   Database: {self.database}{Colors.RESET}")
            
            self.connection = psycopg2.connect(**conn_params)
            self.connection.set_session(autocommit=True)
            
            print(f"{Colors.SUCCESS}✅ Connected successfully!{Colors.RESET}\n")
            return True
            
        except psycopg2.Error as e:
            print(f"{Colors.ERROR}❌ Connection failed: {e}{Colors.RESET}")
            return False
        except Exception as e:
            print(f"{Colors.ERROR}❌ Unexpected error during connection: {e}{Colors.RESET}")
            return False
    
    def disconnect(self):
        """Disconnect from the server"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print(f"\n{Colors.INFO}🔌 Disconnected from server{Colors.RESET}")
    
    def load_queries(self, queries_file: str) -> List[Tuple[int, str, str]]:
        """Load queries from queries.txt file"""
        if not os.path.exists(queries_file):
            print(f"{Colors.ERROR}❌ File not found: {queries_file}{Colors.RESET}")
            return []
        
        queries = []
        try:
            with open(queries_file, 'r') as f:
                content = f.read()
            
            # Parse queries from the file
            lines = content.split('\n')
            query_num = 1
            i = 0
            
            while i < len(lines):
                line = lines[i].strip()
                
                # Skip empty lines and separators
                if not line or line.startswith('-- ==='):
                    i += 1
                    continue
                
                # Check for description comment
                description = None
                if line.startswith('--'):
                    description = line[2:].strip()
                    i += 1
                    if i >= len(lines):
                        continue
                    line = lines[i].strip()
                
                # Collect SQL query
                if line and not line.startswith('--'):
                    query_lines = []
                    
                    while i < len(lines):
                        current_line = lines[i]
                        
                        # Stop at separator
                        if current_line.strip().startswith('-- ==='):
                            break
                        
                        # Skip inline comments but keep the query structure
                        if not current_line.strip().startswith('--'):
                            query_lines.append(current_line)
                        
                        # Check if query ends with semicolon
                        if current_line.strip().endswith(';'):
                            i += 1
                            break
                        
                        i += 1
                    
                    if query_lines:
                        query = '\n'.join(query_lines).strip()
                        
                        # Generate description if not provided
                        if not description:
                            description = f"Query {query_num}"
                        
                        queries.append((query_num, description, query))
                        query_num += 1
                else:
                    i += 1
            
            print(f"{Colors.SUCCESS}📄 Loaded {len(queries)} queries from {queries_file}{Colors.RESET}\n")
            return queries
            
        except Exception as e:
            print(f"{Colors.ERROR}❌ Error reading queries file: {e}{Colors.RESET}")
            return []
    
    def execute_query(self, query_num: int, description: str, query: str) -> None:
        """Execute a single query and display results"""
        print(f"{Colors.HEADER}{'='*80}{Colors.RESET}")
        print(f"{Colors.HEADER}Query {query_num}: {description}{Colors.RESET}")
        print(f"{Colors.HEADER}{'='*80}{Colors.RESET}")
        
        if self.verbose:
            print(f"{Colors.QUERY}SQL:{Colors.RESET}")
            # Display first few lines of query
            query_lines = query.strip().split('\n')
            for line in query_lines[:5]:
                print(f"{Colors.DIM}  {line}{Colors.RESET}")
            if len(query_lines) > 5:
                print(f"{Colors.DIM}  ...{Colors.RESET}")
            print()
        
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SET statement_timeout = %s", (self.timeout * 1000,))
            
            # Execute query and measure time
            start_time = time.time()
            cursor.execute(query)
            duration = time.time() - start_time
            
            # Fetch results
            rows = cursor.fetchall()
            row_count = len(rows)
            
            # Display execution info
            print(f"{Colors.SUCCESS}✅ Query executed successfully{Colors.RESET}")
            print(f"{Colors.INFO}⏱️  Execution time: {duration:.3f} seconds{Colors.RESET}")
            print(f"{Colors.INFO}📊 Total rows returned: {row_count}{Colors.RESET}")
            
            # Display results (max 10 rows)
            if rows:
                display_limit = 10
                display_rows = rows[:display_limit]
                
                print(f"\n{Colors.RESULT}Results (showing {len(display_rows)} of {row_count} rows):{Colors.RESET}")
                
                if TABULATE_SUPPORT:
                    # Format with tabulate
                    headers = list(display_rows[0].keys())
                    table_data = []
                    for row in display_rows:
                        formatted_row = []
                        for key in headers:
                            value = row[key]
                            if value is None:
                                formatted_row.append("NULL")
                            elif isinstance(value, str) and len(str(value)) > 50:
                                formatted_row.append(str(value)[:47] + "...")
                            else:
                                formatted_row.append(str(value))
                        table_data.append(formatted_row)
                    
                    table = tabulate(table_data, headers=headers, tablefmt="grid", maxcolwidths=50)
                    print(table)
                else:
                    # Simple display without tabulate
                    for i, row in enumerate(display_rows, 1):
                        print(f"\nRow {i}:")
                        for key, value in row.items():
                            if value is None:
                                value = "NULL"
                            elif isinstance(value, str) and len(str(value)) > 100:
                                value = str(value)[:97] + "..."
                            print(f"  {key}: {value}")
                
                if row_count > display_limit:
                    print(f"\n{Colors.DIM}... and {row_count - display_limit} more rows{Colors.RESET}")
            else:
                print(f"{Colors.WARNING}No results returned{Colors.RESET}")
            
            cursor.close()
            print()
            
        except psycopg2.Error as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            print(f"{Colors.ERROR}❌ Query failed after {duration:.3f}s: {e}{Colors.RESET}\n")
            if self.verbose:
                print(f"{Colors.DIM}{traceback.format_exc()}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}❌ Unexpected error: {e}{Colors.RESET}\n")
            if self.verbose:
                print(f"{Colors.DIM}{traceback.format_exc()}{Colors.RESET}")
    
    def run_tests(self, queries_file: str, query_filter: Optional[int] = None) -> None:
        """Run all queries from the file or a specific query"""
        if not self.connect():
            return
        
        try:
            queries = self.load_queries(queries_file)
            
            if not queries:
                print(f"{Colors.ERROR}No queries to execute{Colors.RESET}")
                return
            
            # Filter specific query if requested
            if query_filter:
                queries = [q for q in queries if q[0] == query_filter]
                if not queries:
                    print(f"{Colors.ERROR}Query #{query_filter} not found{Colors.RESET}")
                    return
            
            print(f"{Colors.HEADER}🚀 Executing {len(queries)} queries...{Colors.RESET}\n")
            
            # Execute each query
            total_start = time.time()
            successful = 0
            failed = 0
            
            for query_num, description, query in queries:
                try:
                    self.execute_query(query_num, description, query)
                    successful += 1
                except KeyboardInterrupt:
                    print(f"\n{Colors.WARNING}⚠️  Execution interrupted by user{Colors.RESET}")
                    break
                except Exception:
                    failed += 1
                
                # Small delay between queries
                time.sleep(0.1)
            
            # Print summary
            total_duration = time.time() - total_start
            print(f"{Colors.HEADER}{'='*80}{Colors.RESET}")
            print(f"{Colors.HEADER}📊 SUMMARY{Colors.RESET}")
            print(f"{Colors.HEADER}{'='*80}{Colors.RESET}")
            print(f"{Colors.SUCCESS}✅ Successful: {successful}{Colors.RESET}")
            print(f"{Colors.ERROR}❌ Failed: {failed}{Colors.RESET}")
            print(f"{Colors.INFO}⏱️  Total time: {total_duration:.3f} seconds{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.ERROR}❌ Test execution error: {e}{Colors.RESET}")
            if self.verbose:
                print(traceback.format_exc())
        finally:
            self.disconnect()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Test WinCC UA PostgreSQL Wire Protocol Server with queries from queries.txt",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"), 
                       help="Server host (default: localhost, env: PGHOST)")
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")), 
                       help="Server port (default: 5432, env: PGPORT)")
    parser.add_argument("--user", default=os.getenv("PGUSER", "testuser"), 
                       help="Username (default: testuser, env: PGUSER)")
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", "password1"), 
                       help="Password (default: password1, env: PGPASSWORD)")
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "winccua"), 
                       help="Database name (default: winccua, env: PGDATABASE)")
    parser.add_argument("--ssl-mode", default=os.getenv("PGSSLMODE", "disable"), 
                       choices=["disable", "require", "verify-ca", "verify-full"],
                       help="SSL mode (default: disable, env: PGSSLMODE)")
    parser.add_argument("--timeout", type=int, default=30, 
                       help="Query timeout in seconds (default: 30)")
    parser.add_argument("--verbose", action="store_true", 
                       help="Enable verbose output")
    parser.add_argument("--query-only", type=int, 
                       help="Run only a specific query by number")
    parser.add_argument("--queries-file", default="queries.txt",
                       help="Path to queries file (default: queries.txt)")
    
    args = parser.parse_args()
    
    # Get full path to queries file
    if not os.path.isabs(args.queries_file):
        args.queries_file = os.path.join(os.path.dirname(__file__), args.queries_file)
    
    # Create test suite
    tester = QueryTester(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        sslmode=args.ssl_mode,
        timeout=args.timeout,
        verbose=args.verbose
    )
    
    # Run tests
    tester.run_tests(args.queries_file, args.query_only)

if __name__ == "__main__":
    main()