# WinCC UA PostgreSQL Wire Protocol Server

A PostgreSQL wire protocol server that acts as a proxy to a WinCC UA GraphQL backend, allowing SQL clients to query industrial automation data.

## Features

- **Authentication**: Username/password authentication via GraphQL login
- **Virtual Tables**: 
  - `TagValues` - Current tag values
  - `LoggedTagValues` - Historical tag data with timestamp filtering
  - `ActiveAlarms` - Current active alarms  
  - `LoggedAlarms` - Historical alarm data
- **SQL Support**: SELECT queries with WHERE clauses, filtering, and LIKE patterns with wildcards
- **GraphQL Integration**: Translates SQL queries to GraphQL calls

## Quick Start

### Prerequisites

- Rust 1.70+ 
- Access to a WinCC UA GraphQL server

### Installation

```bash
git clone <repository>
cd winccua-pgwire-protocol
cargo build --release
```

### Usage

```bash
# Set GraphQL server URL
export GRAPHQL_HTTP_URL="http://your-wincc-server/graphql"

# Start the server
cargo run -- --bind-addr 127.0.0.1:5432

# Or specify GraphQL URL directly
cargo run -- --graphql-url "http://your-server/graphql" --bind-addr 127.0.0.1:5432
```

**Testing with psql**:
```bash
# Connect with psql (SSL disabled for simplicity) 
# You will be prompted for password
psql -h localhost -p 5433 -U username1 --set=sslmode=disable

# Or use PGPASSWORD environment variable
PGPASSWORD=password1 psql -h localhost -p 5433 -U username1 --set=sslmode=disable

# Run queries after authentication
SELECT version();
SELECT * FROM tagvalues WHERE tag_name = 'HMI_Tag_1';
SELECT name, priority FROM activealarms WHERE priority >= 10;
```

#### Debug Mode and Testing

```
source setenv.sh
RUST_LOG=debug cargo run -- --debug --bind-addr 127.0.0.1:5433
```

The debug mode shows detailed connection information including:
- 🔌 Connection establishment and client addresses
- 📊 Raw bytes received (to identify protocol types)
- 🐘 PostgreSQL wire protocol detection
- 🔐 Password authentication flow
- 📥 Query processing and GraphQL calls
- 📤 Response generation

## Virtual Table Schemas

### TagValues
```sql
CREATE TABLE tagvalues (
    tag_name TEXT,
    timestamp TIMESTAMP,
    timestamp_ms BIGINT,
    numeric_value NUMERIC,
    string_value TEXT
);
```

### LoggedTagValues
```sql  
CREATE TABLE loggedtagvalues (
    tag_name TEXT,
    timestamp TIMESTAMP,
    timestamp_ms BIGINT,
    numeric_value NUMERIC,
    string_value TEXT
);
```

### ActiveAlarms
```sql
CREATE TABLE activealarms (
    name TEXT,
    instance_id INTEGER,
    alarm_group_id INTEGER,
    raise_time TIMESTAMP,
    acknowledgment_time TIMESTAMP,
    clear_time TIMESTAMP,
    reset_time TIMESTAMP,
    modification_time TIMESTAMP,
    state TEXT,
    priority INTEGER,
    event_text TEXT,
    info_text TEXT,
    origin TEXT,
    area TEXT,
    value TEXT,
    host_name TEXT,
    user_name TEXT
);
```

### LoggedAlarms
```sql
CREATE TABLE loggedalarms (
    name TEXT,
    instance_id INTEGER,
    alarm_group_id INTEGER,
    raise_time TIMESTAMP,
    acknowledgment_time TIMESTAMP,
    clear_time TIMESTAMP,
    reset_time TIMESTAMP,
    modification_time TIMESTAMP,
    state TEXT,
    priority INTEGER,
    event_text TEXT,
    info_text TEXT,
    origin TEXT,
    area TEXT,
    value TEXT,
    host_name TEXT,
    user_name TEXT,
    duration TEXT
);
```

## Example Queries

```sql
-- Get current values for specific tags
SELECT * FROM tagvalues WHERE tag_name IN ('Temp_01', 'Pressure_02');

-- Get historical data with time range
SELECT * FROM loggedtagvalues 
WHERE tag_name = 'Temperature_01' 
AND timestamp >= '2023-01-01T00:00:00Z'
AND timestamp <= '2023-01-01T23:59:59Z'
ORDER BY timestamp DESC
LIMIT 100;

-- Find tags with LIKE pattern (uses GraphQL browse)
SELECT * FROM tagvalues WHERE tag_name LIKE 'Temp%';

-- LIKE patterns with wildcards for LoggedTagValues (note the second % is important)
SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'HMI_Tag_%:%';

-- Get active alarms
SELECT name, priority, event_text, raise_time 
FROM activealarms 
WHERE priority >= 10;
```

## LIKE Pattern Support

The server supports SQL LIKE patterns with wildcards (`%` and `_`) for tag_name filtering:

- **TagValues**: Standard LIKE patterns work normally
  ```sql
  SELECT * FROM tagvalues WHERE tag_name LIKE 'Temp%';
  SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI_Tag_1_';
  ```

- **LoggedTagValues**: When using LIKE patterns, ensure proper format for logging tag names
  ```sql
  -- ✅ Correct: Include both wildcards for proper browse filtering
  SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'HMI_Tag_%:%';
  
  -- ❌ Incorrect: Missing second % may not return expected results
  SELECT * FROM loggedtagvalues WHERE tag_name LIKE 'HMI_Tag_%';
  ```

**Note**: LIKE patterns trigger GraphQL browse queries with `objectTypeFilters="LOGGINGTAG"` for LoggedTagValues to ensure only logging-enabled tags are returned.

## Configuration

### Environment Variables

- `GRAPHQL_HTTP_URL` - GraphQL server endpoint
- `RUST_LOG` - Logging level (debug, info, warn, error)

### Command Line Options

```
Options:
  --bind-addr <BIND_ADDR>        Address to bind the server [default: 127.0.0.1:5432]
  --graphql-url <GRAPHQL_URL>    GraphQL server URL
  --debug                        Enable debug logging
  -h, --help                     Print help
```

📋 **Future Enhancements:**
- SSL/TLS support

## Architecture

```
PostgreSQL Client
       ↓ (SQL queries)
PostgreSQL Wire Protocol Server
       ↓ (Parse SQL) 
Query Translator
       ↓ (GraphQL queries)
WinCC Unified GraphQL Server
       ↓ (Industrial data)
WinCC Unified System
```

## License

GNU GENERAL PUBLIC LICENSE Version 3