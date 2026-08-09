# Repark

**Trino's SQL, DuckDB's deployment model, deepest Iceberg support.**

Repark is a single-node data engine: pure Rust, no JVM, wrapped for Python. A lazy
DataFrame API and an ANSI SQL front end over Apache DataFusion and Apache Arrow, with
first-class Apache Iceberg integration — AWS Glue and S3 Tables catalogs first.

## Status

**Pre-alpha.** The engine was ported here from a private codebase where it runs production
pipelines; that port — milestone one — is complete. [STATUS.md](STATUS.md) is the single
source of truth for current state: what is delivered, what is deferred, and release state.
Nothing is released yet and no API is stable; the first tagged release is the next step.

Issues and bug reports are welcome. External code contributions are not accepted yet — see
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache-2.0](LICENSE)
