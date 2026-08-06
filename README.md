# Repark

**Trino's SQL, DuckDB's deployment model, deepest Iceberg support.**

Repark is a single-node data engine: pure Rust, no JVM, wrapped for Python. A lazy
DataFrame API and an ANSI SQL front end over Apache DataFusion and Apache Arrow, with
first-class Apache Iceberg integration — AWS Glue and S3 Tables catalogs first.

## Status

**Pre-alpha.** The engine is being ported here, piece by piece, from a private codebase
where it runs production pipelines. Nothing is released yet and no API is stable; the
first tagged release follows the completion of the port.

Issues and bug reports are welcome. Code contributions are not currently accepted while
the port is in progress.

## License

[Apache-2.0](LICENSE)
