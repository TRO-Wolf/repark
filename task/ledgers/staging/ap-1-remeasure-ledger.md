# Unit ledger — AP-1 re-measure (RP-16 pin) · `plan_partitioning` byte_ratio

**Unit:** AP-1 re-measure · **Date:** 2026-09-11 · **Branch:** `feat/ap-1-remeasure` · **Base:** `864e3483` (RP-16)
**Model:** grok-4.6
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Residue AP-1-R-001 said `plan_partitioning`'s `projected_files_at_target`
was still +76.2 % / +88.0 % wrong on the two synthetic AP-0 beds because those beds
were INSERT-grown and uncompressed, so `byte_ratio` measured 1.000000. RP-16 bumps
the fork pin to `090bc821` (fork `#276` F-WRITE-COMPRESS-1): `INSERT INTO` data files
now carry the table's `write.parquet.compression-codec`, default zstd. This round
measures and documents. It changes no Rust and no Python source.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** any formula tune, any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, any JVM. Round 2 (same unit) re-runs the O-run
rewrite on the zstd INSERT beds; Q-1 stays an owner question.

## PROPOSITION LEDGER — AP-1 re-measure — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `insert_data_files_carry_zstd`: the uniform and skewed INSERT-grown beds' parquet column chunks carry zstd, not uncompressed. | Independent `pyarrow.parquet` over every `data/*.parquet` under `/tmp/ap1r-bed/warehouse/repark_ctas/ap/ns/<bed>/`; record the codec set per bed against AP-1 step 2. | **PROVEN** | Release module, `__debug_assertions__ == False`, commit `864e3483`. Codecs: futures `{ZSTD}` (CTAS, same as AP-1); uniform `{ZSTD}` (was uncompressed); skewed `{ZSTD}` (was uncompressed). Every column chunk of every file. Pin took: fork `#276` reached RePark's INSERT path. |
| C-002 | `byte_ratio_remeasured`: each bed's `byte_ratio` equals independently recomputed Σ `total_compressed_size` / Σ `total_uncompressed_size`, and the plan frame notes carry that ratio rounded to two places with source `footers`. | Footer sums via pyarrow; frame `notes` from `run_adaptpart.py --plan`. | **PROVEN** | futures 26 670 018 / 57 643 097 = 0.462675, frame `byte_ratio=0.46 (footers)` (unchanged from AP-1). uniform 2 831 692 / 7 529 566 = 0.376076, frame `byte_ratio=0.38 (footers)` (was 1.000000 / `1.00`). skewed identical to uniform. On-disk Iceberg bytes: futures 26 729 684 (unchanged), synthetics 3 074 844 (was 7 773 590). Fallback 0.55 did not fire. |
| C-003 | `twenty_percent_check_r001`: for candidate `identity(grp)`, `Σ file_size × byte_ratio` against the **live** rewrite actuals is either inside 20 % (close AP-1-R-001) or still outside (keep it OPEN). Stale AP-0 actuals stay as a continuity column only. | Same formula as [adapt-part-ap1-2026-09-11.md](../../../docs/perf/adapt-part-ap1-2026-09-11.md); no formula tune. New actuals from C-004. | **PROVEN** | Projection 3 074 844 × 0.376076 = 1 156 376. Vs **new** actuals: uniform 7 928 680 → **−85.4 %**; skewed 7 672 169 → **−84.9 %**. Vs stale AP-0 CTAS actuals: −73.8 % / −72.0 %. Honest column is the new actual (same zstd INSERT beds). Both outside 20 %. **AP-1-R-001 still OPEN.** |
| C-004 | `orun_rewrite_actuals`: `ALTER TABLE … ADD PARTITION FIELD identity(grp)` then `CALL rewrite_data_files` on a fresh INSERT-zstd copy of each synthetic bed returns live current-snapshot data-file byte sums and live footer codecs. | Paste the statements, the CALL frame, `Σ file_size_in_bytes` where `content = 0`, and pyarrow codecs of the live files. | **PROVEN** | Copies `ns.uniform_orun` / `ns.skewed_orun` rebuilt from the round-1 seeds (pre: 206 files, 3 074 844 bytes, ZSTD, ratio 0.376076). Statements: `ALTER TABLE ap.ns.<bed>_orun ADD PARTITION FIELD identity(grp)` then `CALL ap.system.rewrite_data_files(table => 'ns.<bed>_orun')`. Frames: rewritten 206, added 20, rewritten_bytes 3 074 844, failed 0. Live files: uniform 20 / 7 928 680 / UNCOMPRESSED (every file 396 434 B); skewed 20 / 7 672 169 / UNCOMPRESSED (min 196 394 / median 196 394 / max 3 745 643). Data-dir leftovers 226 parquet (206 superseded zstd + 20 live); live set is the 20 newest whose sizes sum to the metadata total. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED. Residue AP-1-R-001 remains OPEN
in the completed AP-1 ledger (dated errata prepended at its top; the freeze
gate does not allow an append).

## Owner question

Q-1: should `byte_ratio` multiply the footers' *uncompressed* sum rather than
the stored file bytes, i.e. predict the rewrite's own codec? Not decided here.
Round 2 measured that `rewrite_data_files` wrote UNCOMPRESSED live files (ratio
1.0, 7.93 MB / 7.67 MB) while the projection multiplied already-zstd stored
bytes by the stored zstd ratio (3 074 844 × 0.376076 = 1 156 376). The
column-chunk uncompressed sum of the INSERT inputs is 7 529 566. The formula
was not tuned.

## Red first

No production pin was written: the card forbids Rust and Python source changes.
The before measurement is AP-1 step 2 on the pre-RP-16 tree, pasted in
[adapt-part-ap1-2026-09-11.md](../../../docs/perf/adapt-part-ap1-2026-09-11.md):
uniform/skewed codecs uncompressed, `byte_ratio=1.000000`, projected bytes
7 773 590, errors +76.2 % / +88.0 %. Round 1 after: codecs zstd,
`byte_ratio=0.376076`, projected bytes 1 156 376, errors vs stale actuals
−73.8 % / −72.0 %. Round 2 after: live rewrite actuals 7 928 680 / 7 672 169
UNCOMPRESSED, errors −85.4 % / −84.9 %.

## Gates

- `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider`: 740 passed, 1 skipped, 11 xfailed, exit 0
- `python3 scripts/check_docs_links.py`: 765 files, 4872 links checked — clean
- `make check-ledgers`: 318 ledgers in bins (218 archived), 857 ledger links resolve, frozen rule clean
- `python3 scripts/check_ledger_grammar.py`: 100 live ledgers clean (640 clauses, 1270 pinned clause ids)
- `make verify`: exit 0 (round 2 re-run after the live rewrite actuals)
- Comment fence (`git diff --cached` grep for added `//`/`#` lines, attributes excepted): prints nothing

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-1-remeasure
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 independently recomputes every column-chunk codec on all three beds; uniform and skewed flipped from uncompressed to ZSTD; futures stayed ZSTD. C-002 recomputes the footer ratio and matches the frame notes. C-003 recomputes projected bytes against the live rewrite actuals (C-004) and keeps stale AP-0 actuals as a continuity column. C-004 records ALTER + rewrite_data_files statements, CALL frames, live file_size sums and live UNCOMPRESSED codecs.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md, python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-2
      status: ATTACKED
      evidence: All three AP-0 beds rebuilt and planned on the release module; top-three frame rows pasted verbatim per bed; identity(grp) rows used for the 20 percent check even when ranking moved. Round 2 rebuilt INSERT-zstd copies and ran ADD PARTITION FIELD identity(grp) plus rewrite_data_files end to end.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md, python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-3
      status: N/A
      justification: Measurement-only round. No new refusal path and no error-contract change.
    - id: AT-4
      status: N/A
      justification: Single-threaded local CALL per bed. No shared mutable state added.
    - id: AT-5
      status: N/A
      justification: Local memory catalog under /tmp/ap1r-bed. No credential, no network, no JVM started by this session.
    - id: AT-6
      status: ATTACKED
      evidence: Codec and ratio claimed from pyarrow footer metadata, not from the plan notes alone; Iceberg on-disk file sizes recorded beside column-chunk sums. Round 2 live actuals are the current-snapshot files metadata sum, and live codecs are the 20 newest parquet files whose sizes sum to that total, not the 226-file data directory leftover.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md]
    - id: AT-7
      status: N/A
      justification: No engine change. Bed rebuild walls recorded (futures 0.3 s, synthetics 14.9 s each) as environment, not as a performance claim.
    - id: AT-8
      status: ATTACKED
      evidence: The fork-fix premise was checked by reading the files the INSERT path wrote, not by assuming the pin. Uniform and skewed column chunks are ZSTD.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md]
    - id: AT-9
      status: N/A
      justification: No new user-facing error or frame column. The D-1 frame shape is unchanged; only measured numbers moved.
    - id: AT-10
      status: ATTACKED
      evidence: Before numbers are the AP-1 step-2 document (uncompressed, ratio 1.00, +76.2/+88.0). Round-1 after is zstd INSERT (ratio 0.376076) vs stale actuals. Round-2 after is live rewrite actuals 7928680/7672169 UNCOMPRESSED, errors -85.4/-84.9. Statements and CALL frames are in the document.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md, docs/perf/adapt-part-ap1-2026-09-11.md, task/ledgers/completed/ap-1-ledger.md]
  complete: true
```
