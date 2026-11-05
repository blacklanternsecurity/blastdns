#!/usr/bin/env bash
# CI fixtures for the DNS tests: dnsmasq on 5353 and BIND9 AXFR on 5354.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
command -v dnsmasq >/dev/null || { sudo apt-get update && sudo apt-get install -y dnsmasq; }
sudo ./start-test-dns.sh
./start-test-axfr.sh
