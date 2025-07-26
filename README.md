# WinCC UA PostgreSQL Wire Protocol Server

A PostgreSQL wire protocol server that acts as a proxy to a WinCC UA GraphQL backend, allowing SQL clients to query industrial automation data.

## Features

- **Authentication**: Username/password authentication via GraphQL login
- **Virtual Tables**: 
  - `TagValues` - Current tag values
  - `LoggedTagValues` - Historical tag data with timestamp filtering
  - `ActiveAlarms` - Current active alarms  
  - `LoggedAlarms` - Historical alarm data
- **SQL Support**: SELECT queries with WHERE clauses, filtering, and LIKE patterns
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

### Connecting

#### ✅ PostgreSQL Client Compatibility

**Current Status**: The server implements **PostgreSQL wire protocol** compatible with psql, DBeaver, pgAdmin, and other SQL clients.

**Supported Features**:

1. **SSL Negotiation**: Properly handles and rejects SSL requests with standard PostgreSQL protocol
2. **Password Authentication**: Full PostgreSQL cleartext password authentication flow
3. **GraphQL Integration**: Username/password authentication via WinCC UA GraphQL backend
4. **Simple Query Protocol**: Full support for SELECT queries with proper result formatting
5. **Parameter Status**: Sends required PostgreSQL session parameters
6. **Error Handling**: Proper PostgreSQL error message formatting

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

#### Alternative: Testing with Simple TCP Protocol

For debugging or testing purposes, you can still use the simple TCP protocol:

```bash
# Connect with netcat for low-level testing
nc localhost 5433

# Send authentication (username:password)
operator:secret123

# Send SQL queries
SELECT * FROM tagvalues WHERE tag_name = 'Temperature_01';
```

#### Debug Mode and Testing

```bash
# Run with comprehensive debug logging
./test_debug.sh

# Test PostgreSQL password authentication
./test_password_auth.sh

# Test psql connectivity
./test_psql.sh

# Test authentication fixes
./test_auth_fix.sh

# Test SSL handling
./test_ssl.sh

# Or manually:
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
    timestamp TIMESTAMPTZ,
    numeric_value NUMERIC,
    string_value TEXT
);
```

### LoggedTagValues
```sql  
CREATE TABLE loggedtagvalues (
    tag_name TEXT,
    timestamp TIMESTAMPTZ, 
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
    raise_time TIMESTAMPTZ,
    acknowledgment_time TIMESTAMPTZ,
    clear_time TIMESTAMPTZ,
    reset_time TIMESTAMPTZ,
    modification_time TIMESTAMPTZ,
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
Same as ActiveAlarms plus:
```sql
duration TEXT
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

-- Get active alarms
SELECT name, priority, event_text, raise_time 
FROM activealarms 
WHERE priority >= 10;
```

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

## Development Status

This is currently a **working implementation** with:

✅ **Completed:**
- Basic project structure and dependencies  
- GraphQL client with authentication (including error code handling)
- SQL query parsing and validation
- **PostgreSQL Wire Protocol Support** (compatible with psql, DBeaver, pgAdmin)
- Authentication flow with proper error handling
- SSL connection handling for PostgreSQL clients
- **All 4 Virtual Table Implementations:**
  - `TagValues` - Current tag values with filtering
  - `LoggedTagValues` - Historical data with timestamp filtering  
  - `ActiveAlarms` - Current alarms with filter strings
  - `LoggedAlarms` - Historical alarms with time ranges
- Full SQL-to-GraphQL translation
- WHERE clauses, IN operators, LIKE patterns, ORDER BY, LIMIT
- LIKE pattern support via GraphQL browse
- Comprehensive debug logging
- PostgreSQL result formatting and error handling

🔄 **Ready for Production Use:**
- Connect with any PostgreSQL client
- Full query support for all virtual tables
- Proper PostgreSQL protocol compliance

📋 **Future Enhancements:**
- Extended Query Protocol (prepared statements)
- Advanced SQL features (JOINs, aggregations)
- Connection pooling
- Performance optimizations
- SSL/TLS support
- Comprehensive testing

## Architecture

```
PostgreSQL Client
       ↓ (SQL queries)
PostgreSQL Wire Protocol Server
       ↓ (Parse SQL) 
Query Translator
       ↓ (GraphQL queries)
WinCC UA GraphQL Server
       ↓ (Industrial data)
WinCC UA System
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes  
4. Add tests
5. Submit a pull request

## License

[Your License Here]