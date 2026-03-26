"""Integration tests for zone transfer (AXFR) support.

Requires the BIND9 test server: ./scripts/start-test-axfr.sh
The server runs on 127.0.0.1:5354 with the zonetransfer.test zone.
"""

import pytest

from blastdns import zone_transfer, ConfigurationError, ResolverError, BlastDNSError

AXFR_SERVER = "127.0.0.1:5354"
AXFR_ZONE = "zonetransfer.test"


@pytest.mark.asyncio
async def test_zone_transfer_success():
    """Full AXFR should return all zone records with every record type."""
    records = await zone_transfer(AXFR_SERVER, AXFR_ZONE)

    # Build lookup structures
    record_types = {rtype for _, rtype, _ in records}
    by_type = {}
    for name, rtype, rdata in records:
        by_type.setdefault(rtype, []).append((name, rdata))

    # --- Verify every record type is present ---

    # SOA (appears at start and end of AXFR)
    assert "SOA" in record_types
    soa_records = by_type["SOA"]
    assert len(soa_records) == 2, "AXFR should have SOA at start and end"
    assert "ns1.zonetransfer.test." in soa_records[0][1]

    # NS
    assert "NS" in record_types
    ns_names = {rdata for _, rdata in by_type["NS"]}
    assert "ns1.zonetransfer.test." in ns_names

    # A
    assert "A" in record_types
    a_records = {(name, rdata) for name, rdata in by_type["A"]}
    assert ("www.zonetransfer.test.", "127.0.0.3") in a_records
    assert ("mail.zonetransfer.test.", "127.0.0.2") in a_records
    assert ("ftp.zonetransfer.test.", "127.0.0.4") in a_records

    # AAAA
    assert "AAAA" in record_types
    aaaa_records = {(name, rdata) for name, rdata in by_type["AAAA"]}
    assert ("ipv6.zonetransfer.test.", "::1") in aaaa_records

    # MX
    assert "MX" in record_types
    mx_data = {rdata for _, rdata in by_type["MX"]}
    assert any("mail.zonetransfer.test." in d for d in mx_data)

    # CNAME
    assert "CNAME" in record_types
    cname_records = {(name, rdata) for name, rdata in by_type["CNAME"]}
    assert ("api.zonetransfer.test.", "www.zonetransfer.test.") in cname_records

    # TXT
    assert "TXT" in record_types
    txt_data = {rdata for _, rdata in by_type["TXT"]}
    assert any("spf1" in d for d in txt_data)

    # SRV
    assert "SRV" in record_types
    srv_data = {rdata for _, rdata in by_type["SRV"]}
    assert any("ldap.zonetransfer.test." in d for d in srv_data)

    # PTR
    assert "PTR" in record_types
    ptr_records = {(name, rdata) for name, rdata in by_type["PTR"]}
    assert ("1.0.0.zonetransfer.test.", "ns1.zonetransfer.test.") in ptr_records

    # NSEC
    assert "NSEC" in record_types
    assert len(by_type["NSEC"]) == 2
    nsec_data = {rdata for _, rdata in by_type["NSEC"]}
    assert any("api.zonetransfer.test." in d for d in nsec_data)
    assert any("ftp.zonetransfer.test." in d for d in nsec_data)


@pytest.mark.asyncio
async def test_zone_transfer_invalid_nameserver():
    """Invalid nameserver should raise ConfigurationError."""
    with pytest.raises(ConfigurationError):
        await zone_transfer("not-an-ip", "example.com")


@pytest.mark.asyncio
async def test_zone_transfer_nonexistent_zone():
    """Zone transfer for a non-existent zone should return empty results."""
    records = await zone_transfer(AXFR_SERVER, "nonexistent.zone", timeout_secs=3.0)
    assert len(records) == 0


@pytest.mark.asyncio
async def test_zone_transfer_timeout():
    """Zone transfer against a non-routable IP should timeout."""
    with pytest.raises((ResolverError, BlastDNSError)):
        await zone_transfer("192.0.2.1:53", "example.com", timeout_secs=2.0)
