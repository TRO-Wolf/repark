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

**Not in this step:** any formula tune, any rewrite of the O-run, any dependency
file, `STATUS.md`, `briefs/next-sequence.md`, any JVM.

## PROPOSITION LEDGER — AP-1 re-measure — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `insert_data_files_carry_zstd`: the uniform and skewed INSERT-grown beds' parquet column chunks carry zstd, not uncompressed. | Independent `pyarrow.parquet` over every `data/*.parquet` under `/tmp/ap1r-bed/warehouse/repark_ctas/ap/ns/<bed>/`; record the codec set per bed against AP-1 step 2. | **PROVEN** | Release module, `__debug_assertions__ == False`, commit `864e3483`. Codecs: futures `{ZSTD}` (CTAS, same as AP-1); uniform `{ZSTD}` (was uncompressed); skewed `{ZSTD}` (was uncompressed). Every column chunk of every file. Pin took: fork `#276` reached RePark's INSERT path. |
| C-002 | `byte_ratio_remeasured`: each bed's `byte_ratio` equals independently recomputed Σ `total_compressed_size` / Σ `total_uncompressed_size`, and the plan frame notes carry that ratio rounded to two places with source `footers`. | Footer sums via pyarrow; frame `notes` from `run_adaptpart.py --plan`. | **PROVEN** | futures 26 670 018 / 57 643 097 = 0.462675, frame `byte_ratio=0.46 (footers)` (unchanged from AP-1). uniform 2 831 692 / 7 529 566 = 0.376076, frame `byte_ratio=0.38 (footers)` (was 1.000000 / `1.00`). skewed identical to uniform. On-disk Iceberg bytes: futures 26 729 684 (unchanged), synthetics 3 074 844 (was 7 773 590). Fallback 0.55 did not fire. |
| C-003 | `twenty_percent_check_r001`: for candidate `identity(grp)`, `Σ file_size × byte_ratio` against AP-0's O-run actuals (uniform 4 413 222, skewed 4 134 457) is either inside 20 % (close AP-1-R-001) or still outside (keep it OPEN). | Same formula as [adapt-part-ap1-2026-09-11.md](../../../docs/perf/adapt-part-ap1-2026-09-11.md); no formula tune. | **PROVEN** | uniform 3 074 844 × 0.376076 = 1 156 376 vs 4 413 222 → **−73.8 %** (was +76.2 %). skewed 3 074 844 × 0.376076 = 1 156 376 vs 4 134 457 → **−72.0 %** (was +88.0 %). Both outside 20 %. **AP-1-R-001 still OPEN.** `run_adaptpart.py` has no rewrite flag; the O-run actuals predate the INSERT codec change and were not re-measured. `identity(grp)` projected files: uniform 20 (same), skewed 21 (was 27). |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED. Residue AP-1-R-001 remains OPEN
in the completed AP-1 ledger (dated errata prepended at its top; the freeze
gate does not allow an append).

## Owner question

Should `byte_ratio` multiply the footers' *uncompressed* sum rather than the stored
file bytes, i.e. predict the rewrite's own codec? Not decided here. On these beds
the current formula applies the stored zstd ratio to already-zstd file bytes
(3 074 844 × 0.376076 = 1 156 376), while the column-chunk uncompressed sum is
7 529 566.

## Red first

No production pin was written: the card forbids Rust and Python source changes.
The before measurement is AP-1 step 2 on the pre-RP-16 tree, pasted in
[adapt-part-ap1-2026-09-11.md](../../../docs/perf/adapt-part-ap1-2026-09-11.md):
uniform/skewed codecs uncompressed, `byte_ratio=1.000000`, projected bytes
7 773 590, errors +76.2 % / +88.0 %. This run is the after: codecs zstd,
`byte_ratio=0.376076`, projected bytes 1 156 376, errors −73.8 % / −72.0 %.

## Gates

- `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider`: 740 passed, 1 skipped, 11 xfailed, exit 0
- `python3 scripts/check_docs_links.py`: 765 files, 4872 links checked — clean
- `make check-ledgers`: 318 ledgers in bins (218 archived), 857 ledger links resolve, frozen rule clean
- `make verify`: exit 0 (Rust tests 2985 passed, 0 failed, 5 ignored; docs-links/ledgers/grammar/compaction clean)
- Comment fence (`git diff --cached` grep for added `//`/`#` lines, attributes excepted): prints nothing

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-1-remeasure
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 independently recomputes every column-chunk codec on all three beds; uniform and skewed flipped from uncompressed to ZSTD; futures stayed ZSTD. C-002 recomputes the footer ratio and matches the frame notes. C-003 recomputes projected bytes against the recorded O-run actuals.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md, python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-2
      status: ATTACKED
      evidence: All three AP-0 beds rebuilt and planned on the release module; top-three frame rows pasted verbatim per bed; identity(grp) rows used for the 20 percent check even when ranking moved.
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
      evidence: Codec and ratio claimed from pyarrow footer metadata, not from the plan notes alone; Iceberg on-disk file sizes recorded beside column-chunk sums.
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
      evidence: Before numbers are the AP-1 step-2 document (uncompressed, ratio 1.00, +76.2/+88.0). After numbers are this run (zstd, ratio 0.376076, -73.8/-72.0). Reproduce command is in the document.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-2026-09-11.md, docs/perf/adapt-part-ap1-2026-09-11.md, task/ledgers/completed/ap-1-ledger.md]
  complete: true
```
