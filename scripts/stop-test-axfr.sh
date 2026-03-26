#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="blastdns-test-axfr"

if docker rm -f "$CONTAINER_NAME" 2>/dev/null; then
    echo "Stopped AXFR test server"
else
    echo "No AXFR test server running"
fi
