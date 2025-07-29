#!/usr/bin/env python3
"""
WinCC UA PostgreSQL Wire Protocol Server Test Suite

This script tests the server with various SQL queries including:
- Tag list browsing and filtering
- Tag value queries
- Historical data queries with aggregations
- Active and logged alarms
- Information schema queries
- System queries

Requirements:
    pip install psycopg2-binary colorama tabulate

Usage:
    python test_server.py [options]
    
    Options:
        --host HOST       Server host (default: localhost, env: PGHOST)
        --port PORT       Server port (default: 5432, env: PGPORT)
        --user USER       Username (default: testuser, env: PGUSER)
        --password PASS   Password (default: password1, env: PGPASSWORD)
        --database DB     Database name (default: winccua, env: PGDATABASE)
        --ssl-mode MODE   SSL mode: disable, require, verify-ca, verify-full (default: disable, env: PGSSLMODE)
        --timeout SEC     Query timeout in seconds (default: 30)
        --verbose         Enable verbose output
        --query-only NUM  Run only a specific query by number
        --no-color        Disable colored output
        --loop NUM        Loop test execution NUM times (0 = infinite loop, default: 1)
"""

import sys
import argparse
import time
import traceback
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import sql
except ImportError:
    print("❌ Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from colorama import init, Fore, Back, Style
    init(autoreset=True)
    COLOR_SUPPORT = True
except ImportError:
    print("⚠️  Warning: colorama not found. Install with: pip install colorama for colored output")
    COLOR_SUPPORT = False
    # Dummy color classes if colorama is not available
    class Fore:
        RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = ""
    class Back:
        RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = ""
    class Style:
        DIM = NORMAL = BRIGHT = RESET_ALL = ""

try:
    from tabulate import tabulate
    TABULATE_SUPPORT = True
except ImportError:
    print("⚠️  Warning: tabulate not found. Install with: pip install tabulate for better table formatting")
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

class TestResult:
    """Represents the result of a test query"""
    def __init__(self, query_num: int, description: str, query: str, 
                 success: bool, duration: float, row_count: int = 0, 
                 error: str = None, data: List[Dict] = None):
        self.query_num = query_num
        self.description = description
        self.query = query
        self.success = success
        self.duration = duration
        self.row_count = row_count
        self.error = error
        self.data = data or []

class WinCCTestSuite:
    """Test suite for WinCC UA PostgreSQL Wire Protocol Server"""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 user: str = "testuser", password: str = "password1",
                 database: str = "winccua", sslmode: str = "disable",
                 timeout: int = 30, verbose: bool = False, no_color: bool = False,
                 loop_count: int = 1):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.timeout = timeout
        self.verbose = verbose
        self.no_color = no_color
        self.loop_count = loop_count
        self.connection = None
        self.results: List[TestResult] = []
        
        # Disable colors if requested
        if no_color:
            global COLOR_SUPPORT
            COLOR_SUPPORT = False
            Colors.SUCCESS = Colors.ERROR = Colors.WARNING = ""
            Colors.INFO = Colors.QUERY = Colors.RESULT = Colors.HEADER = Colors.DIM = Colors.RESET = ""
    
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
            print(f"{Colors.DIM}   SSL Mode: {self.sslmode}{Colors.RESET}")
            
            self.connection = psycopg2.connect(**conn_params)
            self.connection.set_session(autocommit=True)
            
            print(f"{Colors.SUCCESS}✅ Connected successfully!{Colors.RESET}")
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
            print(f"{Colors.INFO}🔌 Disconnected from server{Colors.RESET}")
    
    def execute_query(self, query_num: int, description: str, query: str, 
                     log_row_count: bool = False) -> TestResult:
        """Execute a single test query"""
        print(f"\n{Colors.HEADER}{'='*60}{Colors.RESET}")
        print(f"{Colors.HEADER}Test {query_num}: {description}{Colors.RESET}")
        print(f"{Colors.HEADER}{'='*60}{Colors.RESET}")
        
        if self.verbose:
            print(f"{Colors.QUERY}📝 Query:{Colors.RESET}")
            print(f"{Colors.DIM}{query.strip()}{Colors.RESET}")
            print()
        
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SET statement_timeout = %s", (self.timeout * 1000,))
            
            start_time = time.time()
            cursor.execute(query)
            end_time = time.time()
            
            duration = end_time - start_time
            rows = cursor.fetchall()
            row_count = len(rows)
            
            # Convert RealDictRow to regular dict for JSON serialization
            data = [dict(row) for row in rows]
            
            result = TestResult(
                query_num=query_num,
                description=description,
                query=query,
                success=True,
                duration=duration,
                row_count=row_count,
                data=data
            )
            
            print(f"{Colors.SUCCESS}✅ Query executed successfully{Colors.RESET}")
            print(f"{Colors.INFO}⏱️  Duration: {duration:.3f} seconds{Colors.RESET}")
            print(f"{Colors.INFO}📊 Rows returned: {row_count}{Colors.RESET}")
            
            # Special logging for loggedtagvalues queries
            if log_row_count or 'loggedtagvalues' in query.lower():
                print(f"{Colors.WARNING}📈 LoggedTagValues query - Row count: {row_count}{Colors.RESET}")
            
            # Display results based on size and type
            self._display_results(data, query_num)
            
            cursor.close()
            return result
            
        except psycopg2.Error as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            error_msg = str(e).strip()
            
            result = TestResult(
                query_num=query_num,
                description=description,
                query=query,
                success=False,
                duration=duration,
                error=error_msg
            )
            
            print(f"{Colors.ERROR}❌ Query failed: {error_msg}{Colors.RESET}")
            if self.verbose:
                print(f"{Colors.DIM}{traceback.format_exc()}{Colors.RESET}")
            
            return result
        except Exception as e:
            duration = time.time() - start_time if 'start_time' in locals() else 0
            error_msg = f"Unexpected error: {str(e)}"
            
            result = TestResult(
                query_num=query_num,
                description=description,
                query=query,
                success=False,
                duration=duration,
                error=error_msg
            )
            
            print(f"{Colors.ERROR}❌ {error_msg}{Colors.RESET}")
            if self.verbose:
                print(f"{Colors.DIM}{traceback.format_exc()}{Colors.RESET}")
            
            return result
    
    def _display_results(self, data: List[Dict], query_num: int):
        """Display query results in a formatted way"""
        if not data:
            print(f"{Colors.WARNING}📭 No results returned{Colors.RESET}")
            return
        
        # Limit display for large result sets
        display_limit = 10
        total_rows = len(data)
        display_data = data[:display_limit]
        
        if TABULATE_SUPPORT and data:
            print(f"\n{Colors.RESULT}📋 Results (showing {len(display_data)} of {total_rows} rows):{Colors.RESET}")
            
            # Format the table
            headers = list(display_data[0].keys())
            rows = []
            for row in display_data:
                formatted_row = []
                for key in headers:
                    value = row[key]
                    if value is None:
                        formatted_row.append("NULL")
                    elif isinstance(value, (int, float)):
                        formatted_row.append(str(value))
                    elif isinstance(value, datetime):
                        formatted_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        # Truncate long strings
                        str_value = str(value)
                        if len(str_value) > 50:
                            str_value = str_value[:47] + "..."
                        formatted_row.append(str_value)
                rows.append(formatted_row)
            
            table = tabulate(rows, headers=headers, tablefmt="grid", maxcolwidths=50)
            print(f"{Colors.DIM}{table}{Colors.RESET}")
        else:
            # Fallback display without tabulate
            print(f"\n{Colors.RESULT}📋 Results (showing {len(display_data)} of {total_rows} rows):{Colors.RESET}")
            for i, row in enumerate(display_data, 1):
                print(f"{Colors.DIM}Row {i}:{Colors.RESET}")
                for key, value in row.items():
                    if isinstance(value, datetime):
                        value = value.strftime("%Y-%m-%d %H:%M:%S")
                    elif value is None:
                        value = "NULL"
                    print(f"  {key}: {value}")
                print()
        
        if total_rows > display_limit:
            print(f"{Colors.DIM}... and {total_rows - display_limit} more rows{Colors.RESET}")
    
    def get_test_queries(self) -> List[tuple]:
        """Return list of test queries with descriptions"""
        return [
            (1, "Tag List - Basic Browse with Limit", 
             """select tag_name, display_name, object_type, data_type from taglist 
                where display_name like '%' order by tag_name desc limit 10;"""),
            
            (2, "Tag List - Order by Object Type", 
             """select * from taglist 
                order by object_type asc
                limit 10;"""),
            
            (3, "Tag List - Filter by Display Name Pattern", 
             """select * from taglist 
                where display_name like '%::%PV%'
                order by tag_name asc
                limit 10;"""),
            
            (4, "Tag List - Group by Object Type", 
             """select object_type, count(*) 
                from taglist where display_name like '%::%PV%' 
                group by object_type order by object_type;"""),
            
            (5, "Tag Values - Sum Numeric Values", 
             """select sum(numeric_value) from tagvalues 
                where tag_name like '%::%HMI_Tag_%' ;"""),
            
            (6, "Logged Tag Values - Recent Aggregations", 
             """select count(*), min(numeric_value), max(numeric_value), avg(numeric_value) 
                from loggedtagvalues 
                where timestamp > CURRENT_TIMESTAMP - INTERVAL '10 minutes'
                and tag_name like '%::HMI_Tag_%:LoggingTag_1' 
                and quality = 'GOOD_CASCADE'
                limit 100000;"""),
            
            (7, "Logged Tag Values - Time Range Query", 
             """select tag_name as metric, timestamp, numeric_value
                from loggedtagvalues 
                where timestamp > '2025-07-27T14:00:00Z' and timestamp < '2025-07-27T14:02:00Z' 
                and tag_name like '%::HMI_Tag_%:LoggingTag_1' 
                and quality = 'GOOD_CASCADE'
                order by timestamp desc;"""),
            
            (8, "Logged Tag Values - Time Bucketing", 
             """SELECT 
                    tag_name as metric,
                    to_timestamp(floor(extract(epoch from timestamp) / 600) * 600) AS time_bucket,
                    MIN(numeric_value) AS min_value,
                    MAX(numeric_value) AS max_value,
                    AVG(numeric_value) AS avg_value,
                    COUNT(*) AS sample_count
                FROM loggedtagvalues 
                WHERE timestamp > '2025-07-28T14:00:00Z' 
                AND timestamp < '2025-07-28T15:00:00Z'
                    AND tag_name LIKE '%::HMI_Tag_%:LoggingTag_1' 
                    AND quality = 'GOOD_CASCADE' 
                GROUP BY tag_name, time_bucket
                ORDER BY tag_name, time_bucket
                LIMIT 100000;"""),
            
            (9, "Tag List - HMI Pattern Search", 
             """SELECT * FROM taglist WHERE tag_name LIKE '%::HMI_%:%';"""),
            
            (10, "Simple Test Query", 
             """select 1;"""),
            
            (11, "Active Alarms - Filter by Priority", 
             """select * from activealarms where priority > 10 and alarm_group_id = 0;"""),
            
            #(12, "Logged Alarms - All Records", 
            # """select * from loggedalarms where raise_time between current_time - interval '12 hours' and CURRENT_TIMESTAMP;"""),
            
            (13, "System Version Query", 
             """SELECT version();"""),
            
            (14, "Information Schema - Tables", 
             """SELECT 
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type,
                    self_referencing_column_name,
                    reference_generation,
                    user_defined_type_catalog,
                    user_defined_type_schema,
                    user_defined_type_name,
                    is_insertable_into,
                    is_typed,
                    commit_action
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;"""),
            
            (15, "Information Schema - Columns", 
             """SELECT * FROM information_schema.columns
                ORDER BY table_name, ordinal_position;"""),
            
            (16, "Tag Values - HMI Tag Pattern with Double Colon", 
             """select * from tagvalues where tag_name like '%::%HMI_%' order by tag_name;""")
        ]
    
    def run_tests(self, query_filter: Optional[int] = None) -> bool:
        """Run all test queries or a specific query with optional looping"""
        if not self.connect():
            return False
        
        try:
            queries = self.get_test_queries()
            
            if query_filter:
                queries = [q for q in queries if q[0] == query_filter]
                if not queries:
                    print(f"{Colors.ERROR}❌ Query {query_filter} not found{Colors.RESET}")
                    return False
            
            # Determine loop parameters
            is_infinite = self.loop_count == 0
            loop_iterations = self.loop_count if not is_infinite else 1
            
            print(f"\n{Colors.HEADER}🚀 Starting WinCC UA PostgreSQL Server Test Suite{Colors.RESET}")
            print(f"{Colors.INFO}📋 Running {len(queries)} test queries...{Colors.RESET}")
            
            if is_infinite:
                print(f"{Colors.WARNING}🔄 Looping infinitely (Press Ctrl+C to stop){Colors.RESET}")
            elif self.loop_count > 1:
                print(f"{Colors.INFO}🔄 Looping {self.loop_count} times{Colors.RESET}")
            
            iteration = 0
            all_results_successful = True
            
            try:
                while is_infinite or iteration < loop_iterations:
                    iteration += 1
                    
                    if self.loop_count > 1 or is_infinite:
                        print(f"\n{Colors.HEADER}{'='*80}{Colors.RESET}")
                        print(f"{Colors.HEADER}🔄 LOOP ITERATION {iteration}{'' if not is_infinite else ' (Press Ctrl+C to stop)'}{Colors.RESET}")
                        print(f"{Colors.HEADER}{'='*80}{Colors.RESET}")
                    
                    # Clear results for each iteration (except the last one to keep summary)
                    if iteration > 1:
                        self.results.clear()
                    
                    iteration_successful = True
                    for query_num, description, query in queries:
                        # Check if this is a loggedtagvalues query for special logging
                        is_logged_query = 'loggedtagvalues' in query.lower()
                        result = self.execute_query(query_num, description, query, log_row_count=is_logged_query)
                        self.results.append(result)
                        
                        if not result.success:
                            iteration_successful = False
                        
                        # Small delay between queries
                        time.sleep(0.1)
                    
                    if not iteration_successful:
                        all_results_successful = False
                    
                    # Print iteration summary for multi-loop runs
                    if self.loop_count > 1 or is_infinite:
                        successful_count = sum(1 for r in self.results if r.success)
                        total_count = len(self.results)
                        iteration_duration = sum(r.duration for r in self.results)
                        
                        status_color = Colors.SUCCESS if iteration_successful else Colors.ERROR
                        status_text = "✅ PASSED" if iteration_successful else "❌ FAILED"
                        
                        print(f"\n{status_color}🔄 Iteration {iteration} {status_text}: {successful_count}/{total_count} queries successful in {iteration_duration:.3f}s{Colors.RESET}")
                        
                        # Add delay between iterations (except for single runs)
                        if (is_infinite or iteration < loop_iterations) and not is_infinite:
                            print(f"{Colors.DIM}💤 Waiting 1 second before next iteration...{Colors.RESET}")
                            time.sleep(1)
                    
                    # For infinite loop, we don't break automatically
                    if not is_infinite and iteration >= loop_iterations:
                        break
                        
            except KeyboardInterrupt:
                print(f"\n{Colors.WARNING}⚠️  Loop interrupted by user after {iteration} iteration(s){Colors.RESET}")
                if not self.results:
                    return False
            
            # Print final summary
            if self.loop_count > 1 or is_infinite:
                print(f"\n{Colors.HEADER}{'='*80}{Colors.RESET}")
                print(f"{Colors.HEADER}🏁 FINAL SUMMARY - {iteration} iteration(s) completed{Colors.RESET}")
                print(f"{Colors.HEADER}{'='*80}{Colors.RESET}")
            
            self._print_summary()
            return all_results_successful
            
        except KeyboardInterrupt:
            print(f"\n{Colors.WARNING}⚠️  Test suite interrupted by user{Colors.RESET}")
            return False
        except Exception as e:
            print(f"\n{Colors.ERROR}❌ Unexpected error during test execution: {e}{Colors.RESET}")
            if self.verbose:
                print(f"{Colors.DIM}{traceback.format_exc()}{Colors.RESET}")
            return False
        finally:
            self.disconnect()
    
    def _print_summary(self):
        """Print test execution summary"""
        print(f"\n{Colors.HEADER}{'='*60}{Colors.RESET}")
        print(f"{Colors.HEADER}📊 TEST SUMMARY{Colors.RESET}")
        print(f"{Colors.HEADER}{'='*60}{Colors.RESET}")
        
        total_tests = len(self.results)
        successful_tests = sum(1 for r in self.results if r.success)
        failed_tests = total_tests - successful_tests
        total_duration = sum(r.duration for r in self.results)
        
        print(f"{Colors.INFO}📋 Total tests: {total_tests}{Colors.RESET}")
        print(f"{Colors.SUCCESS}✅ Successful: {successful_tests}{Colors.RESET}")
        print(f"{Colors.ERROR}❌ Failed: {failed_tests}{Colors.RESET}")
        print(f"{Colors.INFO}⏱️  Total duration: {total_duration:.3f} seconds{Colors.RESET}")
        
        if failed_tests > 0:
            print(f"\n{Colors.ERROR}❌ Failed Tests:{Colors.RESET}")
            for result in self.results:
                if not result.success:
                    print(f"{Colors.ERROR}  Test {result.query_num}: {result.description}{Colors.RESET}")
                    print(f"{Colors.DIM}    Error: {result.error}{Colors.RESET}")
        
        # LoggedTagValues summary
        logged_queries = [r for r in self.results if 'loggedtagvalues' in r.query.lower() and r.success]
        if logged_queries:
            print(f"\n{Colors.WARNING}📈 LoggedTagValues Query Summary:{Colors.RESET}")
            for result in logged_queries:
                print(f"{Colors.WARNING}  Test {result.query_num}: {result.row_count} rows in {result.duration:.3f}s{Colors.RESET}")
        
        success_rate = (successful_tests / total_tests) * 100 if total_tests > 0 else 0
        if success_rate == 100:
            print(f"\n{Colors.SUCCESS}🎉 All tests passed! Success rate: {success_rate:.1f}%{Colors.RESET}")
        elif success_rate >= 80:
            print(f"\n{Colors.WARNING}⚠️  Most tests passed. Success rate: {success_rate:.1f}%{Colors.RESET}")
        else:
            print(f"\n{Colors.ERROR}💥 Many tests failed. Success rate: {success_rate:.1f}%{Colors.RESET}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Test WinCC UA PostgreSQL Wire Protocol Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_server.py                                    # Run all tests with defaults
  python test_server.py --host 192.168.1.100 --port 5433  # Custom host/port
  python test_server.py --ssl-mode require                 # Enable TLS
  python test_server.py --query-only 6                     # Run only query #6
  python test_server.py --verbose --no-color               # Verbose output without colors
  python test_server.py --loop 10                          # Run all tests 10 times
  python test_server.py --loop 0                           # Run tests infinitely (Ctrl+C to stop)
  python test_server.py --query-only 6 --loop 0            # Run specific query infinitely

Environment Variables:
  export PGHOST=192.168.1.100                              # Set default host
  export PGPORT=5433                                        # Set default port
  export PGUSER=myuser                                      # Set default username
  export PGPASSWORD=mypass                                  # Set default password
  export PGDATABASE=mydatabase                              # Set default database
  export PGSSLMODE=require                                  # Set default SSL mode
  python test_server.py                                    # Uses env vars as defaults
        """
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
    parser.add_argument("--timeout", type=int, default=30, help="Query timeout in seconds (default: 30)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--query-only", type=int, help="Run only a specific query by number")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")
    parser.add_argument("--loop", type=int, default=1, help="Loop test execution N times (0 = infinite loop, default: 1)")
    
    args = parser.parse_args()
    
    # Create test suite
    test_suite = WinCCTestSuite(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        sslmode=args.ssl_mode,
        timeout=args.timeout,
        verbose=args.verbose,
        no_color=args.no_color,
        loop_count=args.loop
    )
    
    # Run tests
    success = test_suite.run_tests(args.query_only)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()