# Repark

**Trino's SQL, DuckDB's deployment model, deepest Iceberg support.**

Repark is a single-node data engine: pure Rust, no JVM, wrapped for Python. A lazy
DataFrame API and an ANSI SQL front end over Apache DataFusion and Apache Arrow, with
first-class Apache Iceberg integration — AWS Glue and S3 Tables catalogs first.

## Status

**v1.0.0 on PyPI (2026-09-03).** The engine was ported here from a private codebase where it
runs production pipelines; the port, the format-v3 north star and the API freeze are complete.
[STATUS.md](STATUS.md) is the single source of truth for current state: what is delivered, what
is deferred, and release state. From 1.0.0 the public API moves additively within the major;
the rule and the frozen inventory are in [docs/release.md](docs/release.md).

Issues and bug reports are welcome. External code contributions are not accepted yet — see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Where to start

Five hops, in order — the same path for a human contributor and an automated one. Each is the
single home for what it covers; none of them restates another.

1. **This file** — what repark is, one screen.
2. **[STATUS.md](STATUS.md)** — current state: release state, what is delivered, what is deferred,
   and what happens next.
3. **[ARCHITECTURE.md](ARCHITECTURE.md)** — component boundaries, the crate DAG, the three runtime
   flows.
4. **[DEVELOPMENT.md](DEVELOPMENT.md)** — local setup, the `make` targets, the CI surface,
   troubleshooting.
5. **[AGENTS.md](AGENTS.md)** — the authoritative contract every change obeys, and the rest of the
   read order from there (starting with [docs/testing.md](docs/testing.md)).

Then the `map.md` in each directory your change touches; every directory has one.

Using repark rather than working on it? The user-facing guides live in
[docs/guide/](docs/guide/map.md) — install, session + conf, the DataFrame API, the two SQL doors.

Runnable examples live in [examples/](examples/map.md) — start with the torture-dataset tour
notebook.

## License

[Apache-2.0](LICENSE)
