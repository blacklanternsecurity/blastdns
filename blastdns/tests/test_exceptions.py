import pytest

from blastdns import (
    Client,
    MockClient,
    BlastDNSError,
    ConfigurationError,
    NoResolversError,
    ResolverError,
)


class TestExceptionHierarchy:
    """Verify the exception class hierarchy."""

    def test_configuration_error_is_blastdns_error(self):
        assert issubclass(ConfigurationError, BlastDNSError)

    def test_no_resolvers_error_is_configuration_error(self):
        assert issubclass(NoResolversError, ConfigurationError)
        assert issubclass(NoResolversError, BlastDNSError)

    def test_resolver_error_is_blastdns_error(self):
        assert issubclass(ResolverError, BlastDNSError)

    def test_blastdns_error_is_exception(self):
        assert issubclass(BlastDNSError, Exception)


class TestConfigurationErrors:
    """Errors raised during client construction."""

    def test_invalid_resolver_raises_configuration_error(self):
        with pytest.raises(ConfigurationError, match="invalid resolver"):
            Client(["not-an-ip"])

    def test_invalid_resolver_catchable_as_blastdns_error(self):
        with pytest.raises(BlastDNSError):
            Client(["not-an-ip"])

    def test_invalid_port_raises_configuration_error(self):
        with pytest.raises(ConfigurationError, match="invalid resolver"):
            Client(["8.8.8.8:99999"])


class TestResolverErrors:
    """Errors raised during DNS resolution."""

    @pytest.mark.asyncio
    async def test_unreachable_resolver_raises_resolver_error(self):
        # Use a non-routable address with short timeout to trigger a resolver error
        client = Client(
            ["192.0.2.1"],  # TEST-NET, non-routable
        )
        with pytest.raises((ResolverError, BlastDNSError)):
            await client.resolve_full("example.com", "A")


class TestMockClientErrors:
    """Verify mock client returns structured responses, not exceptions, for DNS-level errors."""

    @pytest.mark.asyncio
    async def test_nxdomain_is_not_exception(self):
        """NXDOMAIN should return a DNSResult with NXDomain response code, not raise."""
        client = MockClient()
        client.mock_dns({"_NXDOMAIN": ["notfound.example.com"]})
        # Should not raise - NXDOMAIN is a valid DNS response
        result = await client.resolve_full("notfound.example.com", "A")
        assert result.response.header.response_code == "NXDomain"

    @pytest.mark.asyncio
    async def test_unknown_host_is_not_exception(self):
        """Hosts not in mock data should return empty results, not raise."""
        client = MockClient()
        client.mock_dns({})
        result = await client.resolve("unknown.example.com", "A")
        assert result == []
