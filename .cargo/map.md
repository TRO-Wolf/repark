# map — .cargo/

## Purpose

Workspace-wide cargo configuration.

## Contents

- `config.toml` — sets `PYO3_PYTHON = "python3"` so the PyO3 build script finds an interpreter on
  systems that ship only `python3` (not `/usr/bin/python`). Inert until `repark-python` lands
  (phase 3); maturin overrides it when building wheels.
- `audit.toml` — `cargo audit` ignore list (the `make rust-audit` gate). Kept in sync with
  `deny.toml` `[advisories].ignore`; seven ignored advisory ids on 2026-11-02; reasons below.

## Ignored advisories

Reasons and revisit triggers for every id in `audit.toml` and `deny.toml` `[advisories].ignore`.
The TOML lists stay bare; the reason lives here, never as a TOML comment (owner ruling 2026-08-26).

- **RUSTSEC-2024-0436 (paste)**, unmaintained proc-macro, informational only: no known
  vulnerability and no drop-in successor the ecosystem has migrated to. Path: `paste` 1.0.15 <-
  `parquet` 58.4.0 <- `datafusion` 54.1.0 <- `iceberg-datafusion` 0.9.1 (the owned fork); see
  `cargo tree --workspace -i paste`. Advisory date: 2024 (exact date not recorded in the comment of
  record). Revisit when arrow/datafusion drop `paste` or a CVE is filed.
- **RUSTSEC-2026-0194 and RUSTSEC-2026-0195 (quick-xml, DoS)**, dated 2026-06-29. 0194 is a
  quadratic duplicate-attribute check; 0195 is an unbounded namespace-declaration allocation in
  `NsReader`. The fix is >=0.41.0, semver-unreachable from all three quick-xml copies: 0.39.4 <-
  `object_store` 0.13.2 (requires ^0.39) <- `datafusion` 54.1.0; 0.38.4 <- `opendal` 0.55.0
  (requires ^0.38) <- the fork's `iceberg-storage-opendal` 0.9.1; 0.37.5 <- `reqsign` 0.16.5
  (requires ^0.37). Exposure is DoS-only, and the parser only sees S3 API responses (AWS-authored
  XML), not attacker-controlled input. Revisit at the next DataFusion-family bump (when
  `object_store` moves past ^0.39) and when the fork bumps opendal/reqsign.
- **RUSTSEC-2026-0435 (thrift, CVE-2026-43868)**, dated 2026-11-02. `TBinaryInputProtocol::read_bytes`
  sizes a buffer from an attacker-controlled 4-byte length with no upper bound; the fix is thrift
  0.23.0. Single path: `thrift` 0.17.0 <- `parquet` 58.4.0 <- `datafusion` 54.1.0 <-
  `iceberg-datafusion` 0.9.1 (the fork). `parquet` 58.4.0 declares `thrift = "0.17"`, so 0.23 is
  semver-unreachable and `cargo update -p thrift` cannot reach it. `parquet` uses only
  `TCompactInputProtocol` and `TCompactOutputProtocol`; the vulnerable read is in the binary
  protocol (`src/protocol/binary.rs`), so the sink is unreachable from this tree. Verified
  2026-11-02: `cargo tree --workspace -i thrift@0.17.0` shows the single path, and
  `cargo deny check advisories` with the id un-ignored shows exactly one finding (this id).
  Revisit at the DataFusion-family bump: `parquet` 59.2.0 carries no thrift dependency.
- **RUSTSEC-2026-0391, RUSTSEC-2026-0398, RUSTSEC-2026-0403 (zstd, zstd-safe)**, dated 2026-10-26,
  tracked by card DEPS-1. Patched lines are zstd >=0.14.3 (0391) and zstd-safe >=8.2.0 (0398,
  0403); this tree ships zstd 0.13.3 and zstd-safe 7.2.4, pinned by nine dependents (`parquet`
  58.4.0, `arrow-ipc` 58.4.0, `datafusion` 54.1.0, `datafusion-datasource` 54.1.0, `apache-avro`
  0.21.0, `orc-rust` 0.8.0, `compression-codecs` 0.4.38, the fork's `iceberg` 0.9.1, `repark-core`).
  `datafusion` 55.2.0 still requires zstd ^0.13, so the patched line becomes reachable only after a
  datafusion release on arrow 60, then the fork repin, then the zstd bump. Reachability is not
  established for 0391 and 0403; the seekable API behind 0398 is absent from all six zstd-using
  dependents' sources (measured 2026-10-26). The full text of record is the `deny.toml` comment
  block above these ids.

## I want to...

| I want to... | go to |
|---|---|
| Ignore a RustSec advisory | `audit.toml` AND `deny.toml` `[advisories].ignore` (both, with reason + revisit trigger) |
| Record why an advisory is ignored | this file's `## Ignored advisories` section, never a TOML comment (owner ruling 2026-08-26) |
| Change cargo env configuration | `config.toml` |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../deny.toml](../deny.toml), [../Makefile](../Makefile) (`rust-audit` / `rust-deny`).

## Debug

| Symptom | First check |
|---|---|
| PyO3 build: "failed to run the Python interpreter" (phase 3+) | Ensure `python3` is on PATH; `config.toml` points PyO3 at it |
| cargo-audit and cargo-deny disagree on an advisory | The two ignore lists drifted — re-sync `audit.toml` and `deny.toml` |

First checks: `cargo audit`, `cargo deny check all`. Escalate to: [../map.md](../map.md).
