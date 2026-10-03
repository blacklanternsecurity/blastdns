#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
./stop-test-axfr.sh
sudo ./stop-test-dns.sh
