#!/bin/bash
#
# Test Environment Configuration Template
#
# Copy this file and customize for your environment:
#   cp test_env_template.sh my_test_env.sh
#   # Edit my_test_env.sh with your settings
#   source my_test_env.sh
#

# =============================================================================
# WinCC UA PostgreSQL Server Connection Settings
# =============================================================================

# Server connection
export PGHOST="your-server-host"          # e.g., "192.168.1.100" or "localhost"
export PGPORT="5432"                      # PostgreSQL port

# Authentication
export PGUSER="your-username"             # e.g., "testuser", "admin"
export PGPASSWORD="your-password"         # e.g., "password1", "secretpass"

# Database
export PGDATABASE="winccua"               # Database name (usually "winccua")

# SSL/TLS mode
export PGSSLMODE="disable"                # Options: disable, require, verify-ca, verify-full

# =============================================================================
# Example Configurations
# =============================================================================

# Local development (default)
# export PGHOST="localhost"
# export PGPORT="5432"
# export PGUSER="testuser"
# export PGPASSWORD="password1"
# export PGDATABASE="winccua"
# export PGSSLMODE="disable"

# Remote server with SSL
# export PGHOST="192.168.1.100"
# export PGPORT="5433"
# export PGUSER="winccuser"
# export PGPASSWORD="secure123"
# export PGDATABASE="production"
# export PGSSLMODE="require"

# Docker environment
# export PGHOST="127.0.0.1"
# export PGPORT="15432"
# export PGUSER="postgres"
# export PGPASSWORD="docker123"
# export PGDATABASE="winccua"
# export PGSSLMODE="disable"

# =============================================================================
# Confirmation Output
# =============================================================================

# Color output for confirmation
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}✅ Test environment variables set:${NC}"
echo -e "${BLUE}   PGHOST=$PGHOST${NC}"
echo -e "${BLUE}   PGPORT=$PGPORT${NC}"
echo -e "${BLUE}   PGUSER=$PGUSER${NC}"
echo -e "${BLUE}   PGPASSWORD=$(echo $PGPASSWORD | sed 's/./*/g')${NC}"  # Hide password
echo -e "${BLUE}   PGDATABASE=$PGDATABASE${NC}"
echo -e "${BLUE}   PGSSLMODE=$PGSSLMODE${NC}"
echo ""
echo -e "${GREEN}Usage:${NC}"
echo -e "${BLUE}   python test_server.py           ${YELLOW}# Single test run${NC}"
echo -e "${BLUE}   ./run_parallel_tests.sh         ${YELLOW}# 10 parallel instances${NC}"
echo -e "${BLUE}   ./stress_test.sh                ${YELLOW}# Infinite stress test${NC}"
echo -e "${BLUE}   ./monitor_tests.sh              ${YELLOW}# Monitor running tests${NC}"