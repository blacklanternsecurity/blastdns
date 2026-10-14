# AGENTS.md

Fast DNS resolver in Rust with Python bindings via maturin.

## Toolchain

| Concern | This repository |
|---|---|
| Language | Rust, edition in Cargo.toml. Python bindings, `requires-python` in pyproject.toml |
| Package manager | cargo, uv |
| Lint and format | clippy and rustfmt, no rust-toolchain.toml yet. ruff, version in the `dev` group of pyproject.toml |
| Tests | cargo test, pytest |

## Setup

```bash
uv sync && uv run maturin develop
```

## Tests

Integration tests need two local servers: dnsmasq (see "Integration Tests" in README.md) and a BIND9 AXFR server. Without dnsmasq the Python client tests fail with `ResolverError`. Without BIND9 the zone transfer tests fail.

```bash
sudo ./scripts/start-test-dns.sh     # dnsmasq on 127.0.0.1:5353
./scripts/start-test-axfr.sh         # BIND9 in Docker on 127.0.0.1:5354
cargo test --all --locked -- --include-ignored
uv run maturin develop --release && uv run pytest blastdns/tests
cargo build --release --locked && uv run pytest tests/cli_tests.py
./scripts/stop-test-axfr.sh && sudo ./scripts/stop-test-dns.sh
```

`tests/cli_tests.py` does not match pytest's default `test_*.py` pattern, so a bare `uv run pytest` skips it. It runs the binary at `target/release/blastdns`.

## Lint

CI fails on any of these.

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
uv run ruff check
uv run ruff format --check
```

Clippy runs with `-D warnings` on unpinned stable Rust, so a new Rust release can break CI with no code change, for example a newly deprecated std API.

## Benchmarks

`benchmark.yml` runs `scripts/benchmark.py` on every pull request, comparing the head build against the base branch, and comments the report on the PR. To run it locally, build each side into its own environment with `maturin develop --release` and start dnsmasq, then:

```bash
uv run python scripts/benchmark.py --base BASE/.venv/bin/python --head .venv/bin/python
```

## Standards

Org-wide, in [blacklanternsecurity/.github/standards](https://github.com/blacklanternsecurity/.github/tree/main/standards). Read the one your task touches, not all of them.

| Document | Read it when |
|---|---|
| [principles.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/principles.md) | Always |
| [toolchain.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/toolchain.md) | Touching dependencies, linting, formatting, or language versions |
| [git.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/git.md) | Branching, commit messages, opening or reviewing a pull request |
| [rfc.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/rfc.md) | A change needs agreement before work starts, or an RFC is ending |
| [testing.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/testing.md) | Writing or changing tests, or anything that has them |
| [ci.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/ci.md) | Touching a workflow, an action pin, or a permissions block |
| [releases.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/releases.md) | Versioning, tagging, or publishing |
| [repository-setup.md](https://github.com/blacklanternsecurity/.github/blob/main/standards/repository-setup.md) | Creating a repository, or auditing one |

Never restate a standard here. If this file and a standard disagree, the standard wins and this file is the bug.
