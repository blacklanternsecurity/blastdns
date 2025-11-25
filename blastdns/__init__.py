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
        self._resolvers = list(resolvers)
        self._config = (config or ClientConfig()).to_dict()
        self._inner = None

    async def _ensure_inner(self):
        if self._inner is not None:
            return
        if _native is None:
            raise RuntimeError(
                "blastdns native module is unavailable. "
                "Build it via `maturin develop --features python` "
                "or `cargo build --features python` before using Client."
            )
        self._inner = await _native.Client.create(self._resolvers, self._config)

    async def resolve(self, host, record_type=None):
        await self._ensure_inner()
        return await self._inner.resolve(host, record_type)

