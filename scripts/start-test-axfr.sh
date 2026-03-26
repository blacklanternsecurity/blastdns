#!/usr/bin/env bash
set -euo pipefail

# Start a BIND9 authoritative DNS server in Docker for AXFR testing.
# Listens on 127.0.0.1:5354 (TCP+UDP).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="blastdns-test-axfr"
PORT="${AXFR_PORT:-5354}"

# Stop any existing container
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "Starting BIND9 AXFR test server on port $PORT..."

docker run -d \
    --name "$CONTAINER_NAME" \
    -p "127.0.0.1:${PORT}:53/tcp" \
    -p "127.0.0.1:${PORT}:53/udp" \
    -v "${SCRIPT_DIR}/test-zone/named.conf:/etc/bind/named.conf:ro" \
    -v "${SCRIPT_DIR}/test-zone/db.zonetransfer.test:/etc/bind/db.zonetransfer.test:ro" \
    internetsystemsconsortium/bind9:9.20

# Wait for it to be ready
for i in $(seq 1 10); do
    if docker exec "$CONTAINER_NAME" rndc status >/dev/null 2>&1; then
        echo "BIND9 AXFR test server ready on 127.0.0.1:$PORT"
        exit 0
    fi
    sleep 0.5
done

# Fallback: just check if container is running
if docker ps --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "$CONTAINER_NAME"; then
    sleep 1
    echo "BIND9 AXFR test server started on 127.0.0.1:$PORT"
else
    echo "ERROR: BIND9 container failed to start"
    docker logs "$CONTAINER_NAME" 2>&1 | tail -20
    exit 1
fi
