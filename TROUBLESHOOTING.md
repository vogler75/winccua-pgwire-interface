# Troubleshooting Guide

## PostgreSQL Client Connection Issues

### Problem: SSL Connection Errors

**Symptom**: `psql` or other clients show SSL-related errors:
```
psql: error: connection to server at "localhost", port 5432 failed: 
server sent an error response during SSL exchange
```

**Root Cause**: PostgreSQL clients try to establish SSL connections by default, but our server doesn't support SSL.

**Debug Information**: When SSL is requested, you'll see logs like:
```
🔌 New connection established from 127.0.0.1:54321
📊 Received 8 bytes from 127.0.0.1:54321
🔍 Raw bytes: [00, 00, 00, 08, 04, d2, 16, 2f]
🔒 SSL connection request detected from 127.0.0.1:54321!
📝 Rejecting SSL request - SSL not supported
✅ Sent SSL rejection ('N') to 127.0.0.1:54321
📖 Waiting for startup message after SSL rejection from 127.0.0.1:54321
```

**Solutions**:

1. **Disable SSL in client** (recommended):
   ```bash
   psql -h localhost -p 5433 -U username -d database --set=sslmode=disable
   ```

2. **Use `--set=sslmode=prefer`** (falls back to non-SSL):
   ```bash
   psql -h localhost -p 5433 -U username --set=sslmode=prefer
   ```

### Problem: DBeaver Disconnects Immediately

**Symptom**: DBeaver fails to connect and shows "Connection closed" or timeout errors.

**Root Cause**: DBeaver expects the full PostgreSQL wire protocol, but the current server implements a simple TCP protocol for testing.

**Debug Information**: When DBeaver connects, you'll see logs like:
```
🔌 New connection established from 127.0.0.1:54321
📊 Received 8 bytes from 127.0.0.1:54321
🔍 Raw bytes: [00, 00, 00, 20, 00, 03, 00, 00]
🐘 PostgreSQL wire protocol detected from 127.0.0.1:54321!
📤 Sending PostgreSQL error response to 127.0.0.1:54321
```

**Solutions**:

1. **Use netcat for testing** (recommended for now):
   ```bash
   nc localhost 5433
   # Send: username:password
   # Send: SELECT * FROM tagvalues;
   ```

2. **Wait for PostgreSQL wire protocol implementation** (planned future feature)

3. **Use a simple HTTP client** to test GraphQL directly:
   ```bash
   curl -X POST http://DESKTOP-KHLB071:4000/graphql \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer YOUR_TOKEN" \
     -d '{"query": "{ tagValues(names: [\"TestTag\"]) { name value { timestamp value } } }"}'
   ```

### Protocol Detection Details

The server identifies PostgreSQL wire protocol by:
- **Length field**: First 4 bytes indicate message length
- **Version field**: Next 4 bytes contain protocol version
- **Common values**:
  - `196608` (0x00030000) = PostgreSQL 3.0 protocol
  - `80877103` (0x04d2162f) = SSL request
  - `80877102` (0x04d2162e) = Cancel request

## Authentication Issues

### Problem: GraphQL Authentication Fails

**Debug Information**:
```
🔑 Authentication attempt: user='admin' from 127.0.0.1:54321
❌ Authentication failed for user 'admin' from 127.0.0.1:54321: Login failed: Incorrect credentials provided
```

**Solutions**:
1. Verify credentials with direct GraphQL test
2. Check if WinCC UA server is running
3. Verify network connectivity to GraphQL endpoint

### Problem: GraphQL Connection Validation Fails

**Debug Information**:
```
⚠️ GraphQL connection validation failed: GraphQL server returned status: 400 - Bad Request
```

**Solutions**:
1. Check if GraphQL server is running:
   ```bash
   curl -X POST http://DESKTOP-KHLB071:4000/graphql \
     -H "Content-Type: application/json" \
     -d '{"query": "{ __schema { queryType { name } } }"}'
   ```

2. Verify URL in setenv.sh is correct
3. Check firewall/network connectivity

## Debug Logging Levels

### Enable Maximum Debug Information
```bash
RUST_LOG=trace cargo run -- --debug
```

### Log Level Descriptions
- **ERROR**: Authentication failures, connection errors
- **WARN**: Protocol mismatches, validation issues  
- **INFO**: Connection events, query results
- **DEBUG**: Detailed processing steps, GraphQL calls
- **TRACE**: Raw byte inspection, low-level details

### Useful Debug Commands

```bash
# Test GraphQL endpoint directly
curl -v http://DESKTOP-KHLB071:4000/graphql

# Monitor network connections
netstat -an | grep 5433

# Test with different clients
echo "admin:password" | nc localhost 5433
```

## Common Error Messages

| Message | Meaning | Solution |
|---------|---------|----------|
| `PostgreSQL wire protocol detected` | SQL client connected | Use netcat or wait for wire protocol |
| `Authentication failed` | Wrong credentials | Check username/password |
| `GraphQL server returned status: 400` | GraphQL endpoint issue | Verify server and URL |
| `Connection closed immediately` | Client disconnected | Check logs for protocol detection |
| `Only SELECT queries are supported` | Unsupported SQL | Use SELECT statements only |

## Development Status

✅ **Working**: GraphQL integration, authentication, simple TCP protocol  
🔄 **In Progress**: Virtual table implementations  
📋 **Planned**: Full PostgreSQL wire protocol, advanced SQL features  

For latest status, see the todo list in the code or run with `--debug` flag.