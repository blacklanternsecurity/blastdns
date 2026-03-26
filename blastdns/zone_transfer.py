"""Zone transfer (AXFR) support."""

from . import _native  # type: ignore


async def zone_transfer(nameserver, zone, timeout_secs=6.0):
    """Perform an AXFR (full zone transfer) against a specific nameserver.

    Zone transfers are TCP-based operations that download all records from a DNS zone.

    Args:
        nameserver: The nameserver to query (IP or IP:port string, e.g., "1.2.3.4" or "1.2.3.4:53")
        zone: The zone name to transfer (e.g., "example.com")
        timeout_secs: Connection/query timeout in seconds (default: 6.0)

    Returns:
        list[tuple[str, str, str]]: List of (name, record_type, rdata) tuples.

    Raises:
        ConfigurationError: If the nameserver address or zone name is invalid.
        ResolverError: If the zone transfer fails (timeout, connection refused, etc.).

    Example:
        records = await zone_transfer("ns1.example.com:53", "example.com")
        for name, rtype, rdata in records:
            print(f"{name} {rtype} {rdata}")
    """
    return await _native.zone_transfer_py(nameserver, zone, timeout_secs)
