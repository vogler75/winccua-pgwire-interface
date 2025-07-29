# Parallel Testing Scripts

This directory contains scripts for running parallel stress tests against the WinCC UA PostgreSQL Wire Protocol Server.

## Environment Variables

All scripts support PostgreSQL environment variables for connection settings:

- `PGHOST` - Server hostname (default: localhost)
- `PGPORT` - Server port (default: 5432)
- `PGUSER` - Username (default: testuser)
- `PGPASSWORD` - Password (default: password1)
- `PGDATABASE` - Database name (default: winccua)
- `PGSSLMODE` - SSL mode (default: disable)

### Quick Setup

```bash
# Use the provided environment script
source test_env.sh

# Or set manually
export PGHOST=192.168.1.100
export PGPORT=5433
export PGUSER=myuser
export PGPASSWORD=mypass
export PGSSLMODE=require

# Then run tests without connection parameters
python test_server.py
./run_parallel_tests.sh
```

## Scripts Overview

### 1. `test_server.py` - Main Test Script
The core test script that runs individual test suites against the WinCC UA PostgreSQL server. Supports looping, specific query filtering, and environment variable configuration.

**Usage:**
```bash
python test_server.py [options]
```

**Key Options:**
- `--loop NUM` - Loop test execution (0 = infinite, default: 1)
- `--query-only NUM` - Run only specific query number
- `--host, --port, --user, --password` - Connection parameters (or use env vars)

### 2. `run_parallel_tests.sh` - Parallel Test Runner
The comprehensive script that starts multiple test instances in parallel with full configuration options.

**Usage:**
```bash
./run_parallel_tests.sh [options]
```

**Key Options:**
- `-n, --instances NUM` - Number of parallel instances (default: 10)
- `-l, --loop NUM` - Loop count per instance (0 = infinite, default: 1)
- `-q, --query NUM` - Run only specific query number
- `-h, --host HOST` - Server host (default: localhost, env: PGHOST)
- `-p, --port PORT` - Server port (default: 5432, env: PGPORT)
- `-u, --user USER` - Username (default: testuser, env: PGUSER)
- `-w, --password PASS` - Password (default: password1, env: PGPASSWORD)
- `-d, --database DB` - Database name (default: winccua, env: PGDATABASE)
- `--ssl` - Enable SSL mode (env: PGSSLMODE)
- `--log-dir DIR` - Directory for log files (default: ./test_logs)

**Examples:**
```bash
# Basic: 10 instances, run once each
./run_parallel_tests.sh

# Stress test: 10 instances with infinite loops
./run_parallel_tests.sh -l 0

# Heavy load: 20 instances, 5 loops each
./run_parallel_tests.sh -n 20 -l 5

# Targeted test: 5 instances running only query #6 infinitely
./run_parallel_tests.sh -n 5 -l 0 -q 6

# Custom server
./run_parallel_tests.sh --host 192.168.1.100 --port 5433 --ssl
```

### 3. Environment Configuration Scripts

**`test_env.sh`** - Ready-to-use environment configuration
```bash
source test_env.sh  # Sets default localhost connection
```

**`test_env_template.sh`** - Customizable environment template
```bash
cp test_env_template.sh my_env.sh
# Edit my_env.sh with your settings
source my_env.sh
```

## Log Files

All test output is logged to separate files in the `test_logs/` directory:

- `test_instance_N.log` - Individual test instance output
- `pids.txt` - Process IDs of running instances (for cleanup)

## Typical Workflow

1. **Set up environment:**
   ```bash
   source test_env.sh
   # or customize your own
   cp test_env_template.sh my_env.sh
   # edit my_env.sh
   source my_env.sh
   ```

2. **Start parallel testing:**
   ```bash
   # Basic stress test: 10 instances with infinite loops
   ./run_parallel_tests.sh -l 0
   
   # Or finite run: 10 instances, 5 loops each
   ./run_parallel_tests.sh -n 10 -l 5
   ```

3. **Monitor progress:**
   ```bash
   # Watch all test instances
   tail -f test_logs/test_instance_*.log
   
   # Check running processes
   ps aux | grep test_server.py
   ```

4. **Stop when done:**
   ```bash
   # Stop all running test instances
   kill $(cat test_logs/pids.txt)
   ```

## Advanced Usage

### Long-running Stability Test
```bash
# Run 5 instances for 100 loops each (finite but long)
./run_parallel_tests.sh -n 5 -l 100
```

### Connection Stress Test
```bash
# 50 concurrent connections doing single test runs
./run_parallel_tests.sh -n 50 -l 1
```

### Query-specific Load Test
```bash
# 20 instances hammering the logged tag values query
./run_parallel_tests.sh -n 20 -l 0 -q 6
```

### Custom Configuration
```bash
# Test against remote server with SSL and longer timeouts
./run_parallel_tests.sh \
  --host 192.168.1.100 \
  --port 5433 \
  --ssl \
  --timeout 60 \
  -n 15 \
  -l 10
```

## Monitoring and Debugging

### View Live Logs
```bash
# Watch all instances
tail -f test_logs/test_instance_*.log

# Watch specific instance
tail -f test_logs/test_instance_1.log
```

### Check Running Processes
```bash
# List running test processes
ps aux | grep test_server.py

# Count running instances
cat test_logs/pids.txt | wc -l
```

### Stop All Tests
```bash
# Graceful stop using PID file
kill $(cat test_logs/pids.txt)

# Nuclear option - kill all test processes
pkill -f test_server.py
```

## Tips

1. **Start Small**: Begin with fewer instances to ensure the server handles the load
2. **Monitor Resources**: Watch CPU, memory, and network usage on both client and server
3. **Log Rotation**: Clean up old logs periodically to save disk space
4. **Network Testing**: Use different client machines for true network load testing
5. **Query Patterns**: Mix different query types to simulate realistic usage

## Troubleshooting

### Common Issues

- **Connection Refused**: Server not running or wrong host/port
- **Too Many Connections**: Reduce instance count or check server connection limits
- **High CPU Usage**: Normal for stress testing, but monitor server performance
- **Log Directory Full**: Clean up old test logs regularly

### Performance Tuning

- Adjust the delay between starting instances (currently 0.1s)
- Modify query timeout based on expected response times
- Use `--query-only` to focus on specific problematic queries
- Consider running tests from multiple client machines for distributed load