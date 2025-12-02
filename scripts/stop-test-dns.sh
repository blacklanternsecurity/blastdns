#!/usr/bin/env bash
set -euo pipefail

# Stop the test dnsmasq server

PID_FILE="${DNS_PID_FILE:-/tmp/dnsmasq.pid}"

if [[ ! -f "$PID_FILE" ]]; then
    echo "No PID file found at $PID_FILE"
    exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "Process $PID is not running"
    rm -f "$PID_FILE"
    exit 0
fi

echo "Stopping dnsmasq process $PID..."
kill "$PID"
rm -f "$PID_FILE"
echo "dnsmasq stopped"
