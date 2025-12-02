#!/usr/bin/env bash
set -euo pipefail

# Start dnsmasq on port 5353 with high ulimits for testing
# Requires root.

if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root"
    exit 1
fi

PORT="${DNS_PORT:-5353}"
UPSTREAM="${DNS_UPSTREAM:-1.1.1.1}"
PID_FILE="${DNS_PID_FILE:-/tmp/dnsmasq.pid}"

echo "Starting dnsmasq on port $PORT with upstream $UPSTREAM..."

# Set system-wide kernel limits
sysctl -w fs.file-max=2097152 >/dev/null
sysctl -w net.core.somaxconn=65535 >/dev/null
sysctl -w net.ipv4.ip_local_port_range="1024 65535" >/dev/null

# Kill any existing dnsmasq on this port
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Killing existing dnsmasq process $OLD_PID..."
        kill "$OLD_PID" || true
        sleep 1
    fi
    rm -f "$PID_FILE"
fi

# Start dnsmasq
prlimit --nofile=1048576 --nproc=65536 \
    dnsmasq \
        --no-daemon \
        --no-hosts \
        --no-resolv \
        --port="$PORT" \
        --server="$UPSTREAM" \
        --dns-forward-max=10000 \
        --address=/bench.local/127.0.0.1 &

DNSMASQ_PID=$!
echo "$DNSMASQ_PID" > "$PID_FILE"

sleep 1

if ! kill -0 "$DNSMASQ_PID" 2>/dev/null; then
    echo "ERROR: dnsmasq failed to start"
    exit 1
fi

echo "dnsmasq started successfully with PID $DNSMASQ_PID"
echo "Listening on 127.0.0.1:$PORT and [::1]:$PORT"
echo "PID file: $PID_FILE"
echo ""
echo "To stop: kill \$(cat $PID_FILE)"
