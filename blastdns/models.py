from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field


class Header(BaseModel):
    """DNS message header."""

    id: int
    message_type: Literal["Query", "Response"]
    op_code: str
    authoritative: bool
    truncation: bool
    recursion_desired: bool
    recursion_available: bool
    authentic_data: bool
    checking_disabled: bool
    response_code: str
    query_count: int
    answer_count: int
    name_server_count: int
    additional_count: int


class EdnsFlags(BaseModel):
    """EDNS flags."""

    dnssec_ok: bool
    z: int


class EdnsOptions(BaseModel):
    """EDNS options container."""

    options: List[Any] = Field(default_factory=list)


class Edns(BaseModel):
    """EDNS (Extension mechanisms for DNS) metadata."""

    version: int
    rcode_high: int
    max_payload: int
    flags: EdnsFlags
    options: EdnsOptions


class Query(BaseModel):
    """DNS query record."""

    name: str
    query_type: str
    query_class: str


class Record(BaseModel):
    """DNS resource record (answer, name server, or additional).

    ``text`` and ``targets`` are computed on the Rust side from the underlying
    hickory ``RData`` so callers don't have to re-derive presentation format
    or pull host names out of nested rdata dicts in Python.
    """

    name_labels: str
    ttl: int
    dns_class: str
    rdata: Dict[str, Any]
    text: str = ""
    targets: List[Tuple[str, str]] = Field(default_factory=list)

    def to_text(self) -> str:
        """Presentation (zone-file) format of the rdata. Equivalent to
        dnspython's ``answer.to_text()``."""
        return self.text

    def extract_targets(self) -> List[Tuple[str, str]]:
        """Hostnames embedded in the rdata that BBOT-style consumers want to
        follow (A literal IPs, CNAME chain target, MX exchange, etc).

        TXT records return ``[]`` -- pulling hostnames out of free-form TXT
        content (SPF/DKIM/etc) is consumer-specific and is up to the caller.
        """
        return list(self.targets)


class Response(BaseModel):
    """DNS response message."""

    header: Header
    queries: List[Query]
    answers: List[Record]
    name_servers: List[Record]
    additionals: List[Record]
    signature: List[Any] = Field(default_factory=list)
    edns: Optional[Edns] = None


class DNSResult(BaseModel):
    """Complete DNS resolution result with host and response."""

    host: str
    response: Response


class DNSError(BaseModel):
    """DNS resolution error."""

    error: str


DNSResultOrError = Union[DNSResult, DNSError]
