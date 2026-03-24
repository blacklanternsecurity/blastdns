"""BlastDNS exception hierarchy.

All DNS-related errors raised by blastdns are subclasses of BlastDNSError,
so consumers can catch broadly or narrowly as needed.
"""


class BlastDNSError(Exception):
    """Base exception for all blastdns errors."""


class ConfigurationError(BlastDNSError):
    """Invalid configuration (bad resolver address, invalid hostname, etc.)."""


class NoResolversError(ConfigurationError):
    """No DNS resolvers were provided or detected."""


class ResolverError(BlastDNSError):
    """A resolver failed to process a query (timeout, connection failure, etc.)."""
