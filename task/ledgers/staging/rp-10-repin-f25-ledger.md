# Charter ledger — RP-10 · fork repin 594bdbe5 → 85a4aaf0 (consume F-25; close PERF-DVCLOSE-STMT-1)

**Date:** 2026-09-04 · **Branch:** `feat/rp-10-repin-f25` · **Base:** `origin/main`
`e6ebd40` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-9-repin-f23-ledger.md](rp-9-repin-f23-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-9 r2 filed `PERF-DVCLOSE-STMT-1`: after the F-23 close skip, a 192-manifest
pure-DV `DELETE` still opened every data manifest once at commit in
`validate_fresh_dvs_only`. Fork F-25 (`85a4aaf0cda9ea643bfe34c1666228178e363e94`, PR `#265`)
walks newest-first and stops once every `added_dvs` key is found.

**Not in this unit:** any dependency change beyond the one `[patch.crates-io]` rev; a
Spark-visible design choice not measured (HALT). Scan 3× (`PERF-SCAN-3PASS-1`) is recorded,
not fixed.

## PROPOSITION LEDGER — RP-10 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every `iceberg*` `[patch.crates-io]` rev is `85a4aaf0cda9ea643bfe34c1666228178e363e94` and `Cargo.lock` resolves to it; `datafusion`, `datafusion-spark`, `arrow*`, `parquet` and `rust-toolchain.toml` are byte-identical to `main`; the pin-history row names F-25; the bare repin blast radius is tabled. | `make bump-fork-pin`; `grep` the five revs; compile at the untouched source. | **PROVEN** | Five revs and six lock sources are `85a4aaf0`. Family freeze holds (`datafusion` 54.1.0, `arrow`/`parquet` 58.4.0, toolchain 1.96.0). Bare-repin compiles. Citation: `docs/fork-sync.md`. |
| C-002 | On the RP-9 identity-path DELETE fixture (192 data manifests, pure DV, complete map), the COMMIT phase opens exactly 1 data manifest for the newest touched file; close phase 0; scan 3×N unchanged. Mutation: pin the old count → red. | The hide pin; mutation. | **OPEN** | Pin written; red-first on F-23 1 of 1 (`Failed to read file …-m0.avro`). Green + mutation after the repin. |
| C-003 | The RP-9 wall table re-run (8 / 48 / 192; three runs; medians) before/after the repin; opens per phase after. No wall-clock CI pin. | The `#[ignore]`d cell; the before/after table. | **OPEN** | Before (this clone, debug, `DELETE WHERE id = 0`): 8 = 489.5 / 101.8 / 106.0 ms (median 106); 48 = 414.6 / 1608 / 417.0 ms (median 417); 192 = 2.364 / 1.603 / 1.539 s (median 1.603 s). After pending. |
| C-004 | The record says what landed: registry `PERF-DVCLOSE-STMT-1` FIXED 2026-09-04 (RP-10) with the table; `PERF-SCAN-3PASS-1` updated if phase numbers moved; STATUS v3 workstream one line (25,000 B); every touched `map.md` in lockstep. | The registry row; STATUS; the maps; the gates. | **OPEN** | Pending the after table. |

VERDICT: 4 clauses, 1 PROVEN, 3 OPEN, 0 REJECTED.
