from typing import Any, Dict, List, Literal, Optional, Union

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
    """DNS resource record (answer, name server, or additional)."""

    name_labels: str
    ttl: int
    dns_class: str
    rdata: Dict[str, Any]


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
