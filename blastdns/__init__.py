from pydantic import BaseModel, Field

from . import _native  # type: ignore

__all__ = ["ClientConfig", "Client"]


class ClientConfig(BaseModel):
    threads_per_resolver: int = Field(default=2, ge=1)
    request_timeout_ms: int = Field(default=1000, ge=1)
    max_retries: int = Field(default=10, ge=0)
    purgatory_threshold: int = Field(default=5, ge=0)
    purgatory_sentence_ms: int = Field(default=1000, ge=1)

    def to_dict(self):
        return self.model_dump()


class Client:
    def __init__(self, resolvers, config=None):
        if _native is None:
            raise RuntimeError(
                "blastdns native module is unavailable. "
                "Build it via `maturin develop --features python` "
                "or `cargo build --features python` before using Client."
            )
        config_dict = (config or ClientConfig()).to_dict()
        self._inner = _native.Client(list(resolvers), config_dict)

    async def resolve(self, host, record_type=None):
        return await self._inner.resolve(host, record_type)

