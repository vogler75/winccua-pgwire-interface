# TLS/SSL Setup Guide

This document explains how to enable TLS encryption for the WinCC UA PostgreSQL Wire Protocol Server.

## Overview

The server now supports TLS/SSL encryption for secure communication between PostgreSQL clients and the server. This implementation follows the PostgreSQL SSL protocol negotiation standard.

## Certificate Requirements

You need the following certificate files in PEM format:

1. **Server Certificate** (`server.crt`): The TLS certificate for your server
2. **Private Key** (`server.key`): The private key corresponding to the server certificate
3. **CA Certificate** (`ca.crt`, optional): Certificate Authority certificate for client certificate verification

## Quick Setup with Self-Signed Certificates (Development Only)

For development and testing purposes, you can create self-signed certificates:

```bash
# Generate private key
openssl genrsa -out server.key 2048

# Generate self-signed certificate (valid for 365 days)
openssl req -new -x509 -key server.key -out server.crt -days 365 \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# Optional: Create CA certificate for client cert testing
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 365 \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=TestCA"
```

## Command Line Arguments

### Basic TLS Setup

```bash
# Enable TLS with server certificate and key
cargo run -- \
  --graphql-url http://your-wincc-server:4000/graphql \
  --bind-addr 127.0.0.1:5432 \
  --tls-enabled \
  --tls-cert server.crt \
  --tls-key server.key
```

### TLS with Client Certificate Verification

```bash
# Enable TLS with client certificate verification
cargo run -- \
  --graphql-url http://your-wincc-server:4000/graphql \
  --bind-addr 127.0.0.1:5432 \
  --tls-enabled \
  --tls-cert server.crt \
  --tls-key server.key \
  --tls-ca-cert ca.crt \
  --tls-require-client-cert
```

## TLS Command Line Options

- `--tls-enabled`: Enable TLS/SSL support
- `--tls-cert <path>`: Path to server certificate file (PEM format)
- `--tls-key <path>`: Path to server private key file (PEM format)
- `--tls-ca-cert <path>`: Path to CA certificate for client verification (optional)
- `--tls-require-client-cert`: Require and verify client certificates

## Client Configuration

### psql

```bash
# Connect with TLS (verify server certificate)
psql "host=localhost port=5432 dbname=winccua user=testuser sslmode=require"

# Connect with TLS but skip certificate verification (for self-signed certs)
psql "host=localhost port=5432 dbname=winccua user=testuser sslmode=require sslcert=client.crt sslkey=client.key sslrootcert=ca.crt"
```

### Connection Strings

```
# Basic TLS connection
postgresql://testuser:password@localhost:5432/winccua?sslmode=require

# TLS with client certificate
postgresql://testuser:password@localhost:5432/winccua?sslmode=require&sslcert=client.crt&sslkey=client.key&sslrootcert=ca.crt
```

## SSL Modes

The server supports the following PostgreSQL SSL modes:

- **sslmode=disable**: No SSL connection (client falls back to unencrypted)
- **sslmode=require**: SSL connection required, but certificate not verified
- **sslmode=verify-ca**: SSL connection with CA certificate verification
- **sslmode=verify-full**: SSL connection with full certificate verification

## Protocol Flow

1. Client connects to server
2. Client sends SSL request (if TLS is desired)
3. Server responds:
   - `'S'` if TLS is enabled and configured
   - `'N'` if TLS is not available
4. If TLS accepted, client and server perform TLS handshake
5. After successful handshake, PostgreSQL protocol continues over encrypted connection

## Security Considerations

### Production Environment

- Use certificates issued by a trusted Certificate Authority
- Ensure private keys are properly secured (correct file permissions)
- Consider using client certificate authentication for additional security
- Regularly update certificates before expiration

### Development Environment

- Self-signed certificates are acceptable for development
- Be aware that clients may need to disable certificate verification
- Use `sslmode=require` instead of `verify-ca` or `verify-full` for self-signed certs

## Troubleshooting

### Common Issues

1. **"TLS certificate path is required"**
   - Ensure `--tls-cert` and `--tls-key` are provided when `--tls-enabled` is used

2. **"Failed to load certificate"**
   - Verify certificate file exists and is in PEM format
   - Check file permissions

3. **"TLS handshake failed"**
   - Verify certificate and key match
   - Check that certificate is valid and not expired
   - Ensure client is configured properly

4. **Client connection refused**
   - Verify server is running with TLS enabled
   - Check if client supports the TLS version used by rustls

### Debug Logging

Enable debug logging to see detailed TLS negotiation:

```bash
RUST_LOG=debug cargo run -- --tls-enabled --tls-cert server.crt --tls-key server.key --graphql-url http://localhost:4000/graphql
```

## Examples

### Complete Development Setup

```bash
# 1. Generate certificates
openssl genrsa -out server.key 2048
openssl req -new -x509 -key server.key -out server.crt -days 365 -subj "/CN=localhost"

# 2. Start server with TLS
cargo run -- \
  --graphql-url http://localhost:4000/graphql \
  --bind-addr 127.0.0.1:5432 \
  --tls-enabled \
  --tls-cert server.crt \
  --tls-key server.key \
  --debug

# 3. Connect with psql
psql "host=localhost port=5432 dbname=winccua user=testuser sslmode=require"
```

## Integration with Grafana

Grafana PostgreSQL data source can be configured to use TLS:

```json
{
  "sslmode": "require",
  "sslcert": "path/to/client.crt",
  "sslkey": "path/to/client.key",
  "sslrootcert": "path/to/ca.crt"
}
```

Note: The exact configuration depends on your Grafana version and setup.