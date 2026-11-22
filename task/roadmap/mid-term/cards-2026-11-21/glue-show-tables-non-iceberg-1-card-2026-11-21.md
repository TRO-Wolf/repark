# GLUE-SHOW-TABLES-NON-ICEBERG-1 — SHOW TABLES lists non-Iceberg Glue tables in a mixed namespace

**Rating: proposed S3 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-4`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. Read, not measured: the replay read the code and found the risk holds.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S3 (proposed).** A listing only; mixed namespaces only.

## Finding

- **[R] The fork.** At the fork pin `b6e578dc`, the Glue `list_tables` (catalog/glue/src/catalog.rs:568-601) maps every GetTables entry to a TableIdent and never checks the `table_type=ICEBERG` parameter.
- **[R] RePark passes the names through.** `list_table_names` (crates/repark-iceberg/src/catalog/mod.rs:110-119 at `dac2af61`), the encryption-guard wrapper (crates/repark-iceberg/src/catalog/encryption_guard.rs:424-426), and SHOW TABLES (crates/repark-spark/src/use_ddl.rs:405-421), which filters only by LIKE pattern.

## Why S3

A listing only, and only in a namespace that holds both Iceberg and non-Iceberg Glue tables.

## Scope

The listing of a mixed namespace. Read only; no replay cell measured it.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
