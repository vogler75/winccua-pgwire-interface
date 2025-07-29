#!/bin/bash
#
# Parallel Test Runner for WinCC UA PostgreSQL Wire Protocol Server
#
# This script starts multiple instances of test_server.py in parallel,
# each running in the background with output logged to separate files.
#
# Usage:
#   ./run_parallel_tests.sh [options]
#
# Options:
#   -n, --instances NUM    Number of parallel test instances (default: 10)
#   -l, --loop NUM         Loop count per instance (0 = infinite, default: 1)
#   -q, --query NUM        Run only specific query number (optional)
#   -h, --host HOST        Server host (default: localhost)
#   -p, --port PORT        Server port (default: 5432)
#   --ssl                  Enable SSL mode (require)
#   --timeout SEC          Query timeout in seconds (default: 30)
#   --log-dir DIR          Directory for log files (default: ./test_logs)
#   --help                 Show this help message
#
# Examples:
#   ./run_parallel_tests.sh                                    # 10 instances, run once each
#   ./run_parallel_tests.sh -n 5 -l 10                         # 5 instances, 10 loops each
#   ./run_parallel_tests.sh -n 20 -l 0 -q 6                    # 20 instances, infinite loops, query 6 only
#   ./run_parallel_tests.sh --host 192.168.1.100 --port 5433  # Custom host/port
#   ./run_parallel_tests.sh --ssl --timeout 60                 # With SSL and longer timeout
#

set -euo pipefail

# Default values (with environment variable support)
INSTANCES=10
LOOP_COUNT=1
QUERY_NUM=""
HOST="${PGHOST:-localhost}"
PORT="${PGPORT:-5432}"
USER="${PGUSER:-testuser}"
PASSWORD="${PGPASSWORD:-password1}"
DATABASE="${PGDATABASE:-winccua}"
SSL_MODE="${PGSSLMODE:-disable}"
TIMEOUT=30
LOG_DIR="./test_logs"
VERBOSE=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Function to show help
show_help() {
    cat << EOF
Parallel Test Runner for WinCC UA PostgreSQL Wire Protocol Server

This script starts multiple instances of test_server.py in parallel,
each running in the background with output logged to separate files.

Usage: $0 [options]

Options:
  -n, --instances NUM    Number of parallel test instances (default: 10)
  -l, --loop NUM         Loop count per instance (0 = infinite, default: 1)
  -q, --query NUM        Run only specific query number (optional)
  -h, --host HOST        Server host (default: localhost, env: PGHOST)
  -p, --port PORT        Server port (default: 5432, env: PGPORT)
  -u, --user USER        Username (default: testuser, env: PGUSER)
  -w, --password PASS    Password (default: password1, env: PGPASSWORD)
  -d, --database DB      Database name (default: winccua, env: PGDATABASE)
      --ssl              Enable SSL mode (require, env: PGSSLMODE)
      --timeout SEC      Query timeout in seconds (default: 30)
      --log-dir DIR      Directory for log files (default: ./test_logs)
      --verbose          Enable verbose output for test instances
      --help             Show this help message

Examples:
  $0                                        # 10 instances, run once each
  $0 -n 5 -l 10                             # 5 instances, 10 loops each
  $0 -n 20 -l 0 -q 6                        # 20 instances, infinite loops, query 6 only
  $0 --host 192.168.1.100 --port 5433      # Custom host/port
  $0 --ssl --timeout 60                     # With SSL and longer timeout
  
Environment Variables:
  export PGHOST=192.168.1.100              # Set default host
  export PGPORT=5433                        # Set default port
  export PGUSER=myuser                      # Set default username
  export PGPASSWORD=mypass                  # Set default password
  export PGDATABASE=mydatabase              # Set default database
  export PGSSLMODE=require                  # Set default SSL mode
  $0 -n 20 -l 0                            # Uses env vars as defaults

Log files will be created in: $LOG_DIR/
- test_instance_N.log    - Individual test output
- parallel_tests.log     - This script's output
- pids.txt              - Process IDs for cleanup

To stop all background processes:
  kill \$(cat $LOG_DIR/pids.txt)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--instances)
            INSTANCES="$2"
            shift 2
            ;;
        -l|--loop)
            LOOP_COUNT="$2"
            shift 2
            ;;
        -q|--query)
            QUERY_NUM="$2"
            shift 2
            ;;
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -u|--user)
            USER="$2"
            shift 2
            ;;
        -w|--password)
            PASSWORD="$2"
            shift 2
            ;;
        -d|--database)
            DATABASE="$2"
            shift 2
            ;;
        --ssl)
            SSL_MODE="require"
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE="--verbose"
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            print_color $RED "❌ Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

# Validate inputs
if ! [[ "$INSTANCES" =~ ^[0-9]+$ ]] || [ "$INSTANCES" -lt 1 ]; then
    print_color $RED "❌ Error: Number of instances must be a positive integer"
    exit 1
fi

if ! [[ "$LOOP_COUNT" =~ ^[0-9]+$ ]]; then
    print_color $RED "❌ Error: Loop count must be a non-negative integer"
    exit 1
fi

if [ -n "$QUERY_NUM" ] && ! [[ "$QUERY_NUM" =~ ^[0-9]+$ ]]; then
    print_color $RED "❌ Error: Query number must be a positive integer"
    exit 1
fi

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    print_color $RED "❌ Error: Port must be between 1 and 65535"
    exit 1
fi

# Check if test_server.py exists
if [ ! -f "test_server.py" ]; then
    print_color $RED "❌ Error: test_server.py not found in current directory"
    print_color $YELLOW "Please run this script from the tests/ directory"
    exit 1
fi

# Create log directory
mkdir -p "$LOG_DIR"

# Clear previous PID file
> "$LOG_DIR/pids.txt"

# Build test command
TEST_CMD="python3 test_server.py --host $HOST --port $PORT --user $USER --password $PASSWORD --database $DATABASE --ssl-mode $SSL_MODE --timeout $TIMEOUT --no-color --loop $LOOP_COUNT"

if [ -n "$QUERY_NUM" ]; then
    TEST_CMD="$TEST_CMD --query-only $QUERY_NUM"
fi

if [ -n "$VERBOSE" ]; then
    TEST_CMD="$TEST_CMD $VERBOSE"
fi

# Print configuration
print_color $WHITE "🚀 WinCC UA PostgreSQL Server - Parallel Test Runner"
print_color $WHITE "=" "$(printf '=%.0s' {1..60})"
print_color $CYAN "📋 Configuration:"
print_color $BLUE "   • Test instances: $INSTANCES"
print_color $BLUE "   • Loop count per instance: $LOOP_COUNT $([ "$LOOP_COUNT" = "0" ] && echo "(infinite)")"
print_color $BLUE "   • Server: $HOST:$PORT (User: $USER, DB: $DATABASE, SSL: $SSL_MODE)"
print_color $BLUE "   • Timeout: ${TIMEOUT}s"
print_color $BLUE "   • Log directory: $LOG_DIR"
if [ -n "$QUERY_NUM" ]; then
    print_color $BLUE "   • Query filter: #$QUERY_NUM only"
fi
print_color $WHITE "=" "$(printf '=%.0s' {1..60})"

# Start parallel test instances
print_color $GREEN "🏁 Starting $INSTANCES parallel test instances..."

pids=()
start_time=$(date +%s)

for i in $(seq 1 $INSTANCES); do
    log_file="$LOG_DIR/test_instance_$i.log"
    
    # Start test instance in background
    (
        echo "=== Test Instance $i Started at $(date) ==="
        echo "Command: $TEST_CMD"
        echo "Log file: $log_file"
        echo "PID: $$"
        echo "========================================"
        echo
        
        $TEST_CMD 2>&1
        
        echo
        echo "========================================"
        echo "=== Test Instance $i Finished at $(date) ==="
    ) > "$log_file" 2>&1 &
    
    pid=$!
    pids+=($pid)
    echo $pid >> "$LOG_DIR/pids.txt"
    
    print_color $CYAN "   Instance $i: PID $pid -> $log_file"
    
    # Small delay to avoid overwhelming the server
    sleep 0.1
done

print_color $GREEN "✅ All $INSTANCES instances started successfully!"
print_color $YELLOW "📝 Log files: $LOG_DIR/test_instance_*.log"
print_color $YELLOW "📋 Process IDs saved to: $LOG_DIR/pids.txt"

# If infinite loop, show monitoring info
if [ "$LOOP_COUNT" = "0" ]; then
    print_color $MAGENTA "🔄 Running infinite loops. To stop all processes:"
    print_color $WHITE "   kill \$(cat $LOG_DIR/pids.txt)"
    print_color $MAGENTA "📊 Monitor progress with:"
    print_color $WHITE "   tail -f $LOG_DIR/test_instance_*.log"
else
    print_color $CYAN "⏳ Waiting for all instances to complete..."
    
    # Wait for all background processes
    wait_count=0
    total_pids=${#pids[@]}
    
    while [ $wait_count -lt $total_pids ]; do
        wait_count=0
        for pid in "${pids[@]}"; do
            if ! kill -0 $pid 2>/dev/null; then
                ((wait_count++))
            fi
        done
        
        if [ $wait_count -lt $total_pids ]; then
            running=$((total_pids - wait_count))
            print_color $BLUE "   Still running: $running/$total_pids instances..."
            sleep 5
        fi
    done
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    print_color $GREEN "🎉 All instances completed!"
    print_color $GREEN "⏱️  Total duration: ${duration}s"
fi

# Generate summary
print_color $WHITE "📊 SUMMARY REPORT"
print_color $WHITE "=" "$(printf '=%.0s' {1..40})"

success_count=0
total_tests=0

for i in $(seq 1 $INSTANCES); do
    log_file="$LOG_DIR/test_instance_$i.log"
    if [ -f "$log_file" ]; then
        # Extract success information from log
        if grep -q "All tests passed" "$log_file" 2>/dev/null; then
            print_color $GREEN "   Instance $i: ✅ SUCCESS"
            ((success_count++))
        elif grep -q "TEST SUMMARY" "$log_file" 2>/dev/null; then
            print_color $YELLOW "   Instance $i: ⚠️  PARTIAL"
        else
            print_color $RED "   Instance $i: ❌ FAILED/INCOMPLETE"
        fi
        ((total_tests++))
    fi
done

print_color $WHITE "=" "$(printf '=%.0s' {1..40})"
success_rate=$(( success_count * 100 / total_tests ))
print_color $CYAN "📈 Success Rate: $success_count/$total_tests ($success_rate%)"

if [ $success_rate -eq 100 ]; then
    print_color $GREEN "🎉 Perfect! All instances passed!"
elif [ $success_rate -ge 80 ]; then
    print_color $YELLOW "👍 Good! Most instances passed."
else
    print_color $RED "⚠️  Warning: Many instances failed."
fi

print_color $CYAN "📁 Detailed logs available in: $LOG_DIR/"

# Clean up PID file for completed runs
if [ "$LOOP_COUNT" != "0" ]; then
    rm -f "$LOG_DIR/pids.txt"
fi