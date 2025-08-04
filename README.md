# WinCC Unified PostgreSQL Wire Protocol Server

A PostgreSQL wire protocol server that acts as a proxy to a WinCC Unified GraphQL backend, allowing SQL clients to query industrial automation data.

## Features

- **Authentication**: Username/password authentication via GraphQL login
- **Virtual Tables**: 
  - `TagValues` - Current tag values
  - `LoggedTagValues` - Historical tag data with timestamp filtering
  - `ActiveAlarms` - Current active alarms  
  - `LoggedAlarms` - Historical alarm data
  - `TagList` - List of available tags (uses GraphQL browse query)
- **SQL Support**: SELECT queries with WHERE clauses, filtering, and LIKE patterns with wildcards
- **GraphQL Integration**: Translates SQL queries to GraphQL calls

## Quick Start

### Prerequisites

- Rust 1.70+ 
- Access to a WinCC Unified GraphQL server

### Installation

```bash
git clone <repository>
cd winccua-pgwire-protocol
cargo build --release
```

### Usage

```bash
# Start the server with GraphQL URL as argument
cargo run -- --graphql-url "http://your-wincc-server/graphql" --bind-addr 127.0.0.1:5432

# Alternative port (if 5432 is already in use)
cargo run -- --graphql-url "http://your-wincc-server/graphql" --bind-addr 127.0.0.1:5433
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

## TLS/SSL Encryption

The server supports TLS encryption for secure connections.

### Generate TLS Certificates

**Quick Setup (Development Only)**:
```bash
# Run the auto-generation script
./generate_certs.sh

# Or generate manually:
# 1. Generate private key
openssl genrsa -out server.key 2048

# 2. Generate self-signed certificate (valid for 365 days)
openssl req -new -x509 -key server.key -out server.crt -days 365 \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# 3. Optional: Create CA certificate for client cert testing
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 365 \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=TestCA"
```

### TLS Server Usage

```bash
# Basic TLS setup
cargo run -- --graphql-url "http://your-wincc-server/graphql" \
  --bind-addr 127.0.0.1:5432 \
  --tls-enabled \
  --tls-cert server.crt \
  --tls-key server.key

# TLS with client certificate verification
cargo run -- --graphql-url "http://your-wincc-server/graphql" \
  --bind-addr 127.0.0.1:5432 \
  --tls-enabled \
  --tls-cert server.crt \
  --tls-key server.key \
  --tls-ca-cert ca.crt \
  --tls-require-client-cert
```

### TLS Client Connection

```bash
# Connect with TLS (may need to skip certificate verification for self-signed certs)
psql "host=localhost port=5432 dbname=winccua user=testuser sslmode=require"

# Connection string format
postgresql://testuser:password@localhost:5432/winccua?sslmode=require
```

**Note**: For production use, obtain certificates from a trusted Certificate Authority. Self-signed certificates are only suitable for development and testing.

#### Debug Mode and Testing

```bash
# Run with debug logging enabled
cargo run -- --graphql-url "http://your-wincc-server/graphql" --debug --bind-addr 127.0.0.1:5433

# Or with environment variable for more detailed Rust logging
RUST_LOG=debug cargo run -- --graphql-url "http://your-wincc-server/graphql" --debug --bind-addr 127.0.0.1:5433
```

The debug mode shows detailed connection information including:
- 🔌 Connection establishment and client addresses
- 📊 Raw bytes received (to identify protocol types)
- 🐘 PostgreSQL wire protocol detection
- 🔐 Password authentication flow
- 📥 Query processing and GraphQL calls
- 📤 Response generation

### SQL Query Logging with Table Format

The `--log-sql` option enables enhanced SQL query logging with formatted table output:

```bash
# Enable SQL logging with maximum 50 rows displayed per query
cargo run -- --graphql-url "http://your-wincc-server/graphql" --log-sql 50

# Enable SQL logging with maximum 10 rows displayed per query
cargo run -- --graphql-url "http://your-wincc-server/graphql" --log-sql 10
```

When enabled, the server will log each SQL query and its results in a nicely formatted table showing:
- **Query Header**: The SQL query, execution time, and total row count
- **Column Information**: Column names (first row) and PostgreSQL data types (second row)
- **Data Rows**: Up to the specified number of result rows
- **Truncation Notice**: If results exceed the row limit

Example output:
```
┌─ SQL Query Result ─────────────────────────────────────────
│ Query: SELECT * FROM tagvalues WHERE tag_name LIKE 'HMI%'
│ Execution time: 45ms
│ Rows returned: 3
└────────────────────────────────────────────────────────────
┌────────────────┬─────────────────────┬──────────────┐
│ tag_name       │ timestamp           │ numeric_value│
│ text           │ timestamp           │ double       │
├────────────────┼─────────────────────┼──────────────┤
│ HMI_Tag_1      │ 2024-01-15 10:30:45 │ 123.456      │
│ HMI_Tag_2      │ 2024-01-15 10:30:46 │ 789.012      │
│ HMI_Tag_3      │ 2024-01-15 10:30:47 │ NULL         │
└────────────────┴─────────────────────┴──────────────┘
```

This feature is particularly useful for:
- **Development and Testing**: Verify query results without connecting a separate SQL client
- **Debugging**: Understand data structure and query performance
- **Monitoring**: Track query patterns and result sizes in production logs

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

### TagList
```sql
CREATE TABLE taglist (
    tag_name TEXT,
    display_name TEXT,
    object_type TEXT,
    data_type TEXT
);
```

## Advanced SQL Queries with DataFusion

This server leverages **Apache DataFusion** as an in-memory query engine to provide powerful SQL capabilities on top of the data fetched from the GraphQL API. This allows for complex queries, including aggregations, `GROUP BY`, `ORDER BY`, and advanced filtering directly on the live industrial data.

The query process is as follows:
1.  The SQL query is parsed.
2.  A request is sent to the GraphQL API to fetch the relevant raw data.
3.  This data is loaded into an in-memory table managed by DataFusion.
4.  The original SQL query is executed against this in-memory table, enabling the full power of SQL.

### DataFusion Example Queries

Here are some examples of complex queries that are now supported for the `taglist`, `tagvalues`, and `loggedtagvalues` tables:

```sql
-- Find all tags where the display name contains '::PV'
select * from taglist where display_name like '%PV%';

-- Count tags by their object type
select object_type, count(*) from taglist where display_name like '%PV%' group by object_type;

-- Calculate the sum of numeric values for a group of tags
select sum(numeric_value) from tagvalues where tag_name like '%HMI_Tag_%' ;

-- Filter logged values by timestamp and quality
select * from loggedtagvalues where timestamp > '2025-07-27T14:00:00Z' and tag_name like '%::HMI_Tag_%:LoggingTag_1' and quality = 'GOOD_CASCADE';

-- Get aggregate values (min, max, avg) for a specific tag over a time range
select tag_name, min(numeric_value), max(numeric_value), avg(numeric_value) 
from loggedtagvalues where timestamp > '2025-07-27T14:00:00Z' and tag_name like '%::HMI_Tag_%:LoggingTag_1' and quality = 'GOOD_CASCADE' 
group by tag_name;
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

-- LIKE patterns with wildcards for LoggedTagValues (note the second part ":%" is important)
SELECT * FROM loggedtagvalues WHERE tag_name LIKE '%::HMI_Tag_%:%';

-- Get active alarms
SELECT name, priority, event_text, raise_time 
FROM activealarms 
WHERE priority >= 10;

-- List all available tags (uses GraphQL browse query)
SELECT * FROM taglist;

-- Filter tags by pattern
SELECT * FROM taglist WHERE tag_name LIKE '%::HMI_%';
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

### Command Line Options

```
Options:
  --bind-addr <BIND_ADDR>              Address to bind the server [default: 127.0.0.1:5432]
  --graphql-url <GRAPHQL_URL>          GraphQL server URL (required)
  --debug                              Enable debug logging
  --tls-enabled                        Enable TLS/SSL support
  --tls-cert <TLS_CERT>                Path to TLS certificate file (PEM format)
  --tls-key <TLS_KEY>                  Path to TLS private key file (PEM format)
  --tls-ca-cert <TLS_CA_CERT>          Path to CA certificate for client verification (optional)
  --tls-require-client-cert            Require client certificates for authentication
  --session-extension-interval <SEC>   Session extension interval in seconds [default: 600]
  --keep-alive-interval <SEC>          Keep-alive interval in seconds [default: 30]
  --log-sql <ROWS>                     Enable SQL query logging with table format (e.g., --log-sql 100)
  --quiet-connections                  Suppress connection and authentication log messages
  -h, --help                           Print help
```

### Environment Variables (Optional)

- `RUST_LOG` - Logging level (debug, info, warn, error) - for detailed Rust internal logging

📋 **Features:**
- ✅ SSL/TLS encryption support with optional client certificate verification

## Architecture

```
PostgreSQL Client
       ↓ (SQL queries via PostgreSQL wire protocol)
PostgreSQL Wire Protocol Server
       ↓ (SQL parsing via DataFusion's sqlparser)
SQL Query Handler
       ↓ (Parse query structure and filters)
Query Translator  
       ↓ (GraphQL queries with parsed filters)
WinCC Unified GraphQL Server
       ↓ (Raw industrial data)
DataFusion In-Memory Processing
       ↓ (Load data into Arrow RecordBatch)
       ↓ (Execute original SQL on in-memory data)
PostgreSQL Wire Protocol Response
       ↓ (Formatted results)
PostgreSQL Client
```

### Data Flow Explanation

1. **PostgreSQL Client** sends SQL queries using the standard PostgreSQL wire protocol
2. **PostgreSQL Wire Protocol Server** handles authentication and connection management  
3. **SQL Query Handler** uses DataFusion's sqlparser to parse all incoming SQL queries
4. **Query Translator** extracts filters and parameters from parsed SQL, then converts to GraphQL requests
5. **WinCC Unified GraphQL Server** returns raw industrial data based on the filters
6. **DataFusion In-Memory Processing** (for all queries):
   - Loads raw data into Arrow RecordBatch tables
   - Executes the original SQL query (with all features: GROUP BY, JOINs, aggregations, etc.)
   - Returns processed results
7. **PostgreSQL Wire Protocol Response** formats results and sends back to client

This architecture provides full SQL analytical capabilities on industrial data while maintaining compatibility with standard PostgreSQL clients. **All queries use DataFusion** for consistent SQL feature support.

## License

GNU GENERAL PUBLIC LICENSE Version 3