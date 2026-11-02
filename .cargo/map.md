# map — .cargo/

## Purpose

Workspace-wide cargo configuration.

## Contents

- `config.toml` — sets `PYO3_PYTHON = "python3"` so the PyO3 build script finds an interpreter on
  systems that ship only `python3` (not `/usr/bin/python`). Inert until `repark-python` lands
  (phase 3); maturin overrides it when building wheels.
- `audit.toml` — `cargo audit` ignore list (the `make rust-audit` gate). Kept in sync with
  `deny.toml` `[advisories].ignore`; three ignored advisory ids on 2026-11-02 after four were retired that day; reasons below.

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
### Retired

- **RUSTSEC-2026-0435 (thrift)**: retired 2026-11-02 by the compatible lock refresh, because parquet 58.4.1 requires thrift 0.23.0, which carries the fix for RUSTSEC-2026-0435.
- **RUSTSEC-2026-0391, RUSTSEC-2026-0398, RUSTSEC-2026-0403 (zstd, zstd-safe)**: retired 2026-11-02 by the compatible lock refresh, because the zstd lines moved to 0.13.4 / zstd-safe 7.4.0 and 0.14.3 / zstd-safe 8.2.0, outside the affected ranges of RUSTSEC-2026-0391, -0398 and -0403.

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
