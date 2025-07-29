#!/bin/bash

# Certificate Generation Script for WinCC UA PostgreSQL Wire Protocol Server
# This script generates self-signed certificates for development and testing purposes.
# WARNING: Do NOT use self-signed certificates in production environments!

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CERT_DIR="."
SERVER_CERT="server.crt"
SERVER_KEY="server.key"
CA_CERT="ca.crt"
CA_KEY="ca.key"
CLIENT_CERT="client.crt"
CLIENT_KEY="client.key"
CLIENT_P12="client.p12"
DAYS=365
COUNTRY="US"
STATE="State"
CITY="City"
ORG="WinCC-UA-Dev"
CN_SERVER="localhost"
CN_CA="WinCC-UA-CA"
CN_CLIENT="client"

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Generate TLS certificates for WinCC UA PostgreSQL Wire Protocol Server"
    echo ""
    echo "Options:"
    echo "  -d, --dir DIR           Directory to store certificates (default: current directory)"
    echo "  --days DAYS            Certificate validity in days (default: 365)"
    echo "  --server-cn CN         Server certificate Common Name (default: localhost)"
    echo "  --ca-cn CN             CA certificate Common Name (default: WinCC-UA-CA)"
    echo "  --client-cn CN         Client certificate Common Name (default: client)"
    echo "  --with-client          Generate client certificates for mutual TLS"
    echo "  --country CODE         Country code (default: US)"
    echo "  --state STATE          State/Province (default: State)"
    echo "  --city CITY            City/Locality (default: City)"
    echo "  --org ORG              Organization (default: WinCC-UA-Dev)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Generate basic server certificates"
    echo "  $0 --with-client                     # Generate server and client certificates"
    echo "  $0 --dir ./certs --days 30           # Generate certificates in ./certs/ valid for 30 days"
    echo "  $0 --server-cn myserver.com          # Generate certificate for specific domain"
}

# Parse command line arguments
GENERATE_CLIENT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dir)
            CERT_DIR="$2"
            shift 2
            ;;
        --days)
            DAYS="$2"
            shift 2
            ;;
        --server-cn)
            CN_SERVER="$2"
            shift 2
            ;;
        --ca-cn)
            CN_CA="$2"
            shift 2
            ;;
        --client-cn)
            CN_CLIENT="$2"
            shift 2
            ;;
        --with-client)
            GENERATE_CLIENT=true
            shift
            ;;
        --country)
            COUNTRY="$2"
            shift 2
            ;;
        --state)
            STATE="$2"
            shift 2
            ;;
        --city)
            CITY="$2"
            shift 2
            ;;
        --org)
            ORG="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Check if OpenSSL is available
if ! command -v openssl &> /dev/null; then
    print_error "OpenSSL is not installed or not in PATH"
    echo "Please install OpenSSL to generate certificates:"
    echo "  Ubuntu/Debian: sudo apt install openssl"
    echo "  macOS: brew install openssl"
    echo "  Windows: Download from https://slproweb.com/products/Win32OpenSSL.html"
    exit 1
fi

# Create certificate directory if it doesn't exist
if [[ "$CERT_DIR" != "." ]] && [[ ! -d "$CERT_DIR" ]]; then
    print_info "Creating certificate directory: $CERT_DIR"
    mkdir -p "$CERT_DIR"
fi

# Change to certificate directory
cd "$CERT_DIR"

print_info "🔒 Generating TLS certificates for WinCC UA PostgreSQL Wire Protocol Server"
print_warning "These are self-signed certificates for DEVELOPMENT/TESTING only!"
print_warning "DO NOT use these certificates in production environments!"
echo ""

# Build subject strings
SERVER_SUBJECT="/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$CN_SERVER"
CA_SUBJECT="/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$CN_CA"
CLIENT_SUBJECT="/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$CN_CLIENT"

print_info "Certificate configuration:"
echo "  📁 Directory: $(pwd)"
echo "  📅 Validity: $DAYS days"
echo "  🌐 Server CN: $CN_SERVER"
echo "  🏢 Organization: $ORG"
echo "  📍 Location: $CITY, $STATE, $COUNTRY"
if [[ "$GENERATE_CLIENT" == true ]]; then
    echo "  👤 Client CN: $CN_CLIENT"
fi
echo ""

# 1. Generate CA private key and certificate (needed for client cert verification)
print_info "Generating CA private key and certificate..."
openssl genrsa -out "$CA_KEY" 2048 2>/dev/null
openssl req -new -x509 -key "$CA_KEY" -out "$CA_CERT" -days "$DAYS" \
    -subj "$CA_SUBJECT" 2>/dev/null
print_success "CA certificate generated: $CA_CERT"

# 2. Generate server private key
print_info "Generating server private key..."
openssl genrsa -out "$SERVER_KEY" 2048 2>/dev/null
print_success "Server private key generated: $SERVER_KEY"

# 3. Generate server certificate signed by CA
print_info "Generating server certificate..."
openssl req -new -key "$SERVER_KEY" -out server.csr \
    -subj "$SERVER_SUBJECT" 2>/dev/null

# Create a config file for server certificate with SAN
cat > server.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C=$COUNTRY
ST=$STATE
L=$CITY
O=$ORG
CN=$CN_SERVER

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = $CN_SERVER
DNS.2 = localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl x509 -req -in server.csr -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
    -out "$SERVER_CERT" -days "$DAYS" -extensions v3_req -extfile server.conf 2>/dev/null

# Clean up temporary files
rm -f server.csr server.conf

print_success "Server certificate generated: $SERVER_CERT"

# 4. Generate client certificates if requested
if [[ "$GENERATE_CLIENT" == true ]]; then
    print_info "Generating client private key..."
    openssl genrsa -out "$CLIENT_KEY" 2048 2>/dev/null
    print_success "Client private key generated: $CLIENT_KEY"

    print_info "Generating client certificate..."
    openssl req -new -key "$CLIENT_KEY" -out client.csr \
        -subj "$CLIENT_SUBJECT" 2>/dev/null

    # Create a config file for client certificate
    cat > client.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C=$COUNTRY
ST=$STATE
L=$CITY
O=$ORG
CN=$CN_CLIENT

[v3_req]
keyUsage = keyEncipherment, dataEncipherment, digitalSignature
extendedKeyUsage = clientAuth
EOF

    openssl x509 -req -in client.csr -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
        -out "$CLIENT_CERT" -days "$DAYS" -extensions v3_req -extfile client.conf 2>/dev/null

    # Generate PKCS#12 file for easier client certificate installation
    print_info "Generating client certificate bundle (PKCS#12)..."
    openssl pkcs12 -export -out "$CLIENT_P12" -inkey "$CLIENT_KEY" -in "$CLIENT_CERT" \
        -certfile "$CA_CERT" -passout pass: 2>/dev/null

    # Clean up temporary files
    rm -f client.csr client.conf

    print_success "Client certificate generated: $CLIENT_CERT"
    print_success "Client certificate bundle (PKCS#12) generated: $CLIENT_P12"
fi

echo ""
print_success "🎉 Certificate generation completed successfully!"
echo ""

# Display generated files
print_info "Generated files:"
echo "  📜 CA Certificate: $CA_CERT"
echo "  🔑 CA Private Key: $CA_KEY"
echo "  📜 Server Certificate: $SERVER_CERT"
echo "  🔑 Server Private Key: $SERVER_KEY"

if [[ "$GENERATE_CLIENT" == true ]]; then
    echo "  📜 Client Certificate: $CLIENT_CERT"
    echo "  🔑 Client Private Key: $CLIENT_KEY"
    echo "  📦 Client Bundle (PKCS#12): $CLIENT_P12"
fi

# Set appropriate file permissions
chmod 600 "$SERVER_KEY" "$CA_KEY"
if [[ "$GENERATE_CLIENT" == true ]]; then
    chmod 600 "$CLIENT_KEY"
fi

echo ""
print_info "🔒 File permissions set (private keys are readable only by owner)"
echo ""

# Show usage examples
print_info "Usage examples:"
echo ""
echo "Start server with TLS (basic):"
echo "  cargo run -- --graphql-url http://localhost:4000/graphql \\"
echo "    --tls-enabled --tls-cert $SERVER_CERT --tls-key $SERVER_KEY"
echo ""

if [[ "$GENERATE_CLIENT" == true ]]; then
    echo "Start server with client certificate verification:"
    echo "  cargo run -- --graphql-url http://localhost:4000/graphql \\"
    echo "    --tls-enabled --tls-cert $SERVER_CERT --tls-key $SERVER_KEY \\"
    echo "    --tls-ca-cert $CA_CERT --tls-require-client-cert"
    echo ""
fi

echo "Connect with psql (TLS enabled):"
echo "  psql \"host=localhost port=5432 dbname=winccua user=testuser sslmode=require\""
echo ""

if [[ "$GENERATE_CLIENT" == true ]]; then
    echo "Connect with psql (client certificate):"
    echo "  psql \"host=localhost port=5432 dbname=winccua user=testuser \\"
    echo "    sslmode=require sslcert=$CLIENT_CERT sslkey=$CLIENT_KEY sslrootcert=$CA_CERT\""
    echo ""
fi

print_warning "⚠️  Remember:"
echo "  • These are self-signed certificates for development only"
echo "  • For production, use certificates from a trusted Certificate Authority"
echo "  • Keep private key files secure and never commit them to version control"
echo "  • Consider using .gitignore to exclude *.key and *.p12 files"

echo ""
print_success "🚀 Ready to start your TLS-enabled WinCC UA PostgreSQL server!"