# Charter ledger — RP-17 · fork pin 41e25ba2 consumer (F-WRITE-COMPRESS-2; AP-1 re-measure 3)

**Date:** 2026-09-12 · **Branch:** `chore/repin-rp-17` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-16-ledger.md](rp-16-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The orchestrator bumped the iceberg-rust pin to
`41e25ba2b99a0d90caf4317b4236f5e1d4b683f5` (`make bump-fork-pin`). The reason for the
bump is `#278` F-WRITE-COMPRESS-2: the maintenance, COW/MoR rewrite and
position-delete writers now honour `write.parquet.compression-codec` (default zstd),
closing the four residue writer sites F-WRITE-COMPRESS-1-R-001…R-004 that `#276` left
uncompressed. Run 8's AP-1 re-measure recorded `rewrite_data_files` inflating a
3 074 844-byte zstd bed to 7 928 680 bytes of UNCOMPRESSED live files, which polluted
the 20 % check of residue AP-1-R-001 and parked owner question Q-1. This unit runs the
third re-measure: same beds, same candidate `identity(grp)`, live rewrite actuals
under one codec, both projections recorded for the orchestrator's Q-1 ruling.

**Not in this unit:** `Cargo.toml` / `Cargo.lock` (already on the branch); STATUS.md;
any formula tune or product code (D-4); the Q-1 ruling itself (D-3 — orchestrator).

## Take / skip — riders on `41e25ba2`

| Fork PR | Ask | Take or skip | What it means for RePark |
|---|---|---|---|
| `#278` | F-WRITE-COMPRESS-2 | **take** (reason for the bump, the whole bump) | `rewrite_data_files` and the COW/MoR / position-delete writers carry the table's parquet codec. The AP-1 third re-measure is this unit's consumer measurement: compaction output footers must read zstd, and the 20 % check is answered against live actuals under one codec. |

## PROPOSITION LEDGER — RP-17 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `pin_moved`: the five `rev` lines are byte-identical — `grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml \| sort -u` prints exactly one line, `41e25ba2b99a0d90caf4317b4236f5e1d4b683f5`; `docs/fork-sync.md` carries the RP-17 pin-history row and the root `map.md` pin sentence names the new sha. | The grep; five Cargo.toml hits; Cargo.lock sources; the fork-sync row; the map.md sentence. | **PROVEN** | `sort -u` prints one line `rev = "41e25ba2b99a0d90caf4317b4236f5e1d4b683f5"`. Cargo.toml 5 hits, Cargo.lock 6. Pin-history row 2026-09-12 names F-WRITE-COMPRESS-2 `#278`, no riders. Root map.md sentence `**RP-17 (2026-09-12):** \`41e25ba2\`` landed in the orchestrator's bump commit `3a813537`. This unit does not edit the pin. |
| C-002 | `ap1_remeasure_three`: the three AP-0 beds rebuilt on the release module at pin `41e25ba2` per run 8's exact commands; every live output file's footer reads zstd on every column chunk (asserted, one footer line pasted); `byte_ratio` recomputed from footers; `identity(grp)` plan frame taken; live `rewrite_data_files` actuals measured; `docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md` written with the bed × {stored bytes, uncompressed sum, byte_ratio, projection stored×ratio, projection uncompressed×ratio, live actual, Δ % per projection} table. | The run log; footer codec assertion; the doc table. | **PROVEN** | Release module, `__debug_assertions__ == False`, commit `3a813537`. Plan beds cell-for-cell run 8: futures 3 / 26 729 684 / 0.462675; uniform+skewed 206 / 3 074 844 / 0.376076, all chunks ZSTD. `_orun` copies identical pre-rewrite. CALL frames rewritten 206 / added 20 / failed 0. Live outputs (20 files under `data/grp=*/`): uniform 4 474 081, skewed 4 136 632 — every column chunk ZSTD (asserted); footer line `compacted-00006-01a09473-96a6-7fc2-b577-2d7702c3ffb5.parquet rg0 col0 codec=ZSTD compressed=160259 uncompressed=197592`. Doc written with the full table. |
| C-003 | `ap1_r001_verdict`: the 20 % check is answered as measured — AP-1-R-001 CLOSES iff `Σ file_size × byte_ratio` is within 20 % of the live rewrite actual on every bed, else it stays OPEN with the numbers (D-2); both projections sit beside the actual for the orchestrator's Q-1 ruling (D-3); the residue row is updated wherever it lives (completed AP-1 ledger errata, docs) and the AP-1 remeasure ledger gets a dated RP-17 note with verdicts untouched. | The computed deltas; the updated residue rows; the dated note. | **PROVEN** | stored×ratio = 1 156 376 vs actuals 4 474 081 / 4 136 632 → **−74.2 % / −72.0 %**, outside 20 % both beds → **AP-1-R-001 stays OPEN**. uncompressed×ratio = 2 831 692 → −36.7 % / −31.5 %, recorded for Q-1 (rewrite's own output ratio 0.563 / 0.538 ≠ inputs' 0.376). Residue updated: new errata prepended in `task/ledgers/completed/ap-1-ledger.md`, RP-17 note appended to `ap-1-remeasure-ledger.md` and `adapt-part-ap1-2026-09-11.md`, `completed/map.md` entry. |
| C-004 | `gates_green`: the listed gates pass on this branch — the adapt/partition/catalog_io pytest selection, the whole parity suite, `make check-docs-links`, `make check-ledger-grammar`, `make check-ledgers`, `make verify`. | Counts pasted into §Gates. | **PROVEN** | All listed gates exit 0 (counts in §Gates). One mechanical fix on the way: a hex `plan_id` in the new doc tripped typos on a two-letter token; the existing `[type.ap1remeasure]` glob in `.typos.toml` (created for run 8's doc) now covers this doc too. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

Measurement-only unit, no production pin written (D-4 forbids product code). The
before state is run 8's re-measure on pin `090bc821`
([docs/perf/adapt-part-ap1-remeasure-2026-09-11.md](../../../docs/perf/adapt-part-ap1-remeasure-2026-09-11.md)):
`rewrite_data_files` wrote 20 UNCOMPRESSED live files per synthetic bed (uniform
7 928 680, skewed 7 672 169), footer ratio 1.0, while the projection multiplied
already-zstd stored bytes by the stored zstd ratio (3 074 844 × 0.376076 =
1 156 376) — errors −85.4 % / −84.9 %. The red this unit must turn green: the same
rewrite under pin `41e25ba2` writes zstd.

## Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_perf_ice_catalog_io_1.py python/repark/tests -q -k "adapt or partition or catalog_io"` | 0 — 254 passed, 31 skipped, 6020 deselected in 85.68s |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 740 passed, 1 skipped, 11 xfailed in 79.99s |
| `make check-docs-links` | 0 — 773 files, 4955 links |
| `make check-ledger-grammar` | 0 — 106 live ledgers, 676 clauses, 1306 pinned clause ids |
| `make check-ledgers` | 0 — 324 ledgers in bins (218 archived), 887 ledger links |
| `make verify` | 0 |
| Comment fence (`git diff --cached` grep for added `//`/`#` lines) | prints nothing |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-17
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-002 rebuilds all three beds on the release module and reproduces run 8's plan-bed numbers cell for cell (stored bytes, footer sums, ratio, codec); the rewrite leg asserts ZSTD on every column chunk of every live output file, not a sample.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md, python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-2
      status: ATTACKED
      evidence: All three beds planned and both synthetic beds rewritten end to end (ALTER + rewrite_data_files); identity(grp) rows used for the 20 percent check even where ranking moved (skewed rank 4).
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md]
    - id: AT-3
      status: N/A
      justification: Measurement-only round; no refusal path or error-contract change.
    - id: AT-4
      status: N/A
      justification: Single-threaded local CALL per bed; no shared mutable state added.
    - id: AT-5
      status: ATTACKED
      evidence: Local memory catalogs under /tmp/ap1r-bed and /tmp/ap1r-orun; no credential, no network, no JVM started by this session, REPARK_PARITY_LIVE unset.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md]
    - id: AT-6
      status: ATTACKED
      evidence: Codecs and ratios claimed from pyarrow footer metadata, not plan notes; live actuals are the current-snapshot files-metadata sums and the live file set is the data/grp=*/ files whose sizes sum to those totals exactly (4474081 / 4136632).
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md]
    - id: AT-7
      status: N/A
      justification: No engine change and no wall-clock claim; build walls recorded as environment only.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs are one line 41e25ba2b99a0d90caf4317b4236f5e1d4b683f5; the bump is the orchestrator's commit 3a813537 and this unit does not touch Cargo.toml or Cargo.lock.
      artifacts: [docs/fork-sync.md, map.md]
    - id: AT-9
      status: ATTACKED
      evidence: AP-1-R-001 answered as measured per D-2 and stays OPEN (−74.2 % / −72.0 %); both projections recorded for the orchestrator's Q-1 ruling per D-3; residue updated in the completed AP-1 ledger errata, the remeasure ledger note, and the AP-1 doc.
      artifacts: [task/ledgers/completed/ap-1-ledger.md, task/ledgers/staging/ap-1-remeasure-ledger.md, docs/perf/adapt-part-ap1-2026-09-11.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before state is run 8's re-measure doc (UNCOMPRESSED outputs, −85.4/−84.9); after state is this unit's zstd outputs and both deltas. No formula or product code changed (D-4).
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md, docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md]
  complete: true
```
