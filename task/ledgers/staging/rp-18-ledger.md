# Charter ledger — RP-18 · fork pin 9e3522e3 consumer (F-REWRITE-SIZE-1 step 2; AP-1 re-measure 4)

**Date:** 2026-09-12 · **Branch:** `chore/repin-rp-18` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Version-pin contract".
**Path:** STANDARD. **Proven pattern:**
[rp-17-ledger.md](rp-17-ledger.md).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The orchestrator bumped the iceberg-rust pin to
`9e3522e314e57cc6b074c9fb8fc1afe43f7c2b46` (`make bump-fork-pin`). The reason for
the bump is `#280` F-REWRITE-SIZE-1 step 2: the maintenance rewrite disables the
dictionary per column from the input files' footers — the dead dictionary pages
were the measured 1.47× cause of the S2-24 rewrite inflation — and an unset
`write.parquet.compression-level` now means zstd 3 like Java. RePark's AP-3
(`#526`) already moved `projected_files_at_target` onto the uncompressed footer
sum × `byte_ratio`, counting compression once. With both landed this unit runs
the fourth AP-1 re-measure: same beds, same candidate `identity(grp)`, live
rewrite actuals under one codec and one-pass compression, both projections
recorded, and the 20 % check answers residue AP-1-R-001 as measured.

**Not in this unit:** `Cargo.toml` / `Cargo.lock` (already on the branch); STATUS.md;
any formula tune or product code (D-4).

## Take / skip — riders on `9e3522e3`

| Fork PR | Ask | Take or skip | What it means for RePark |
|---|---|---|---|
| `#280` | F-REWRITE-SIZE-1 step 2 | **take** (reason for the bump, the whole bump) | `rewrite_data_files` output no longer carries dead dictionary pages on near-unique columns and compresses at zstd 3 by default. The AP-1 fourth re-measure is this unit's consumer measurement: compaction output footers must read zstd with no dictionary page on the near-unique columns and a dictionary page on the low-cardinality one, and the 20 % check is answered against live actuals with compression counted once. |

## PROPOSITION LEDGER — RP-18 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `pin_moved`: the five `rev` lines are byte-identical — `grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml \| sort -u` prints exactly one line, `9e3522e314e57cc6b074c9fb8fc1afe43f7c2b46`; `docs/fork-sync.md` carries the RP-18 pin-history row and the root `map.md` pin sentence names the new sha. | The grep; five Cargo.toml hits; Cargo.lock sources; the fork-sync row; the map.md sentence. | **PROVEN** | `sort -u` prints one line `rev = "9e3522e314e57cc6b074c9fb8fc1afe43f7c2b46"`. Cargo.toml 5 hits, Cargo.lock 6. Pin-history row 2026-09-12 names F-REWRITE-SIZE-1 step 2 `#280`, no riders. Root map.md sentence `**RP-18 (2026-09-12):** \`9e3522e3\`` landed in the orchestrator's bump commit `9b1b81da`. This unit does not edit the pin. |
| C-002 | `ap1_remeasure_four`: the three AP-0 beds rebuilt on the release module at pin `9e3522e3` per run 8's exact commands; every live output file's footer reads zstd on every column chunk, the near-unique columns carry no dictionary page and the low-cardinality one still does (both asserted, one footer line per kind pasted); `byte_ratio` recomputed from footers; `identity(grp)` plan frame taken; live `rewrite_data_files` actuals measured; `docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md` written with the bed × {stored bytes, uncompressed sum, byte_ratio, projection stored×ratio, projection uncompressed×ratio, live actual, Δ % per projection} table. | The run log; footer codec and dictionary assertions; the doc table. | **PROVEN** | Release module, `__debug_assertions__ == False`, commit `9b1b81da`. Plan beds: futures 3 / 26 729 684 / 0.462675 identical; uniform+skewed 206 / 3 083 824 / 0.377269 (the zstd-3 default shifted the INSERT bytes from run 9's 3 074 844 / 0.376076), all chunks ZSTD. `_orun` copies identical pre-rewrite. CALL frames rewritten 206 / added 20 / failed 0, rewritten bytes 3 083 824. Live outputs (20 files under `data/grp=*/`): uniform 1 839 168, skewed 1 755 749 — every chunk ZSTD, `ts`/`id` dict=False, `grp` dict=True asserted on every file. Footer lines: `uniform_orun compacted-00019-…1bcf.parquet rg0 ts codec=ZSTD dict=False compressed=64505 uncompressed=160031` and `… rg0 grp codec=ZSTD dict=True compressed=70 uncompressed=52`. Doc written with the full table. |
| C-003 | `ap1_r001_verdict`: the 20 % check is answered as measured — AP-1-R-001 CLOSES iff `Σ file_size × byte_ratio` is within 20 % of the live rewrite actual on every bed, else it stays OPEN with the numbers (D-2); the projection beside the actual is the AP-3 one (uncompressed sum × ratio) with the old stored × ratio figure recorded (D-3); the residue row is updated wherever it lives (completed AP-1 ledger errata, docs) and the AP-1 remeasure ledger gets a dated RP-18 note with verdicts untouched. | The computed deltas; the updated residue rows; the dated note. | **PROVEN** | AP-3 projection uncompressed×ratio = 2 840 672 vs actuals 1 839 168 / 1 755 749 → **+54.5 % / +61.8 %**, outside 20 % both beds → **AP-1-R-001 stays OPEN**; the sign flipped because the rewrite's own output ratio (0.284 / 0.270) beats the inputs' 0.377. Recorded stored×ratio = 1 163 431 → −36.7 % / −33.7 %, also outside — the verdict is the same under either byte basis. Residue updated: new errata prepended in `task/ledgers/completed/ap-1-ledger.md`, RP-18 note appended to `ap-1-remeasure-ledger.md` and `adapt-part-ap1-2026-09-11.md`, the guide's S2-24 line re-measured as AP-1-R-001, `completed/map.md` + `docs/guide/map.md` entries. |
| C-004 | `gates_green`: the listed gates pass on this branch — the adapt/partition/catalog_io pytest selection, the whole parity suite, `make check-docs-links`, `make check-ledger-grammar`, `make check-ledgers`, `make verify`. | Counts pasted into §Gates. | **PROVEN** | All listed gates exit 0 (counts in §Gates). Two mechanical follow-ons: the `[type.ap1remeasure]` glob in `.typos.toml` now covers this doc too, and the docs-links gate reads git-tracked files so the new doc/ledger were staged before the parity suite re-ran green. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

Measurement-only unit, no production pin written (D-4 forbids product code). The
before state is run 9's re-measure on pin `41e25ba2`
([docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md](../../../docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md)):
`rewrite_data_files` wrote 20 zstd live files per synthetic bed but with dead
dictionary pages — uniform 4 474 081, skewed 4 136 632 against the inputs'
compressed sum 2 831 692 (−36.7 % / −31.5 % on the AP-3 projection). The red
this unit must turn green: the same rewrite under pin `9e3522e3` drops the dead
dictionary pages and lands near or below the compressed-sum figure. Measured
after: dictionary only on `grp`, actuals 1 839 168 / 1 755 749 — below the
2 840 672 compressed sum of the zstd-3 inputs.

## Gates

| Gate | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_perf_ice_catalog_io_1.py python/repark/tests -q -k "adapt or partition or catalog_io"` | 0 — 254 passed, 31 skipped, 6023 deselected in 76.13s |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 747 passed, 1 skipped, 11 xfailed in 68.85s |
| `make check-docs-links` | 0 — 783 files, 5041 links |
| `make check-ledger-grammar` | 0 — clean once the attestation carried only ATTACKED/N/A |
| `make check-ledgers` | 0 — 328 ledgers in bins (218 archived), 899 ledger links |
| `make verify` | 0 |
| Comment fence (`git diff --cached` grep for added `//`/`#` lines) | prints nothing |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-18
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-002 rebuilds all three beds on the release module and re-measures every number (futures cell-for-cell run 9; synthetics shifted to 3 083 824 / 0.377269 under the zstd-3 default); the rewrite leg asserts ZSTD plus the dictionary-page split — ts/id none, grp present — on every column chunk of every live output file, not a sample.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md, python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-2
      status: ATTACKED
      evidence: All three beds planned and both synthetic beds rewritten end to end (ALTER + rewrite_data_files); identity(grp) rows used for the 20 percent check where ranking moved (uniform rank 1, skewed rank 2 under the AP-3 basis).
      artifacts: [docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
    - id: AT-3
      status: N/A
      justification: Measurement-only round; no refusal path or error-contract change.
    - id: AT-4
      status: N/A
      justification: Single-threaded local CALL per bed; no shared mutable state added.
    - id: AT-5
      status: ATTACKED
      evidence: Local memory catalogs under /tmp/ap1r-bed and /tmp/ap1r-orun; no credential, no network, no JVM started by this session, REPARK_PARITY_LIVE unset.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
    - id: AT-6
      status: ATTACKED
      evidence: Codecs, ratios and dictionary pages claimed from pyarrow footer metadata, not plan notes; live actuals are the current-snapshot files-metadata sums and the live file set is the data/grp=*/ files whose sizes sum to those totals exactly (1839168 / 1755749).
      artifacts: [docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
    - id: AT-7
      status: N/A
      justification: No engine change and no wall-clock claim; build walls recorded as environment only.
    - id: AT-8
      status: ATTACKED
      evidence: Five iceberg* revs are one line 9e3522e314e57cc6b074c9fb8fc1afe43f7c2b46; the bump is the orchestrator's commit 9b1b81da and this unit does not touch Cargo.toml or Cargo.lock.
      artifacts: [docs/fork-sync.md, map.md]
    - id: AT-9
      status: ATTACKED
      evidence: AP-1-R-001 answered as measured per D-2 and stays OPEN (+54.5 % / +61.8 % on the AP-3 basis; −36.7 % / −33.7 % recorded on the stored basis); residue updated in the completed AP-1 ledger errata, the remeasure ledger note, the AP-1 doc, and the maintenance guide's known-issues line.
      artifacts: [task/ledgers/completed/ap-1-ledger.md, task/ledgers/staging/ap-1-remeasure-ledger.md, docs/perf/adapt-part-ap1-2026-09-11.md, docs/guide/maintenance-policy.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before state is run 9's re-measure doc (zstd with dead dictionary pages, −36.7/−31.5 on the AP-3 projection); after state is this unit's dictionary-free outputs at 0.284/0.270 own-ratio and both deltas. No formula or product code changed (D-4).
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2-2026-09-12.md, docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
  complete: true
```
