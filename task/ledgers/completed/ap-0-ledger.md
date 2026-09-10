# Unit ledger — AP-0 (measure) · ADAPT-PART partition candidates from manifest bounds

**Unit:** AP-0 (measure) · **Date:** 2026-09-10 · **Branch:** `feat/ap-0` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** ADAPT-PART needs a measured answer before any plan/apply engine work:
do P-2 candidates generated from manifest bounds, scored with the P-3 target-band
model, order plausibly against the file-size facts the manifests already report? This
unit builds three local beds, prints one ranked table per bed, and files the numbers
as [the AP-0 document](../../../docs/perf/ap-0-partition-candidates-2026-09-10.md).
No `crates/` edits, no facade edits.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** `plan_partitioning` / `apply_partitioning` (AP-1/AP-2), any
engine or facade change, any dependency file, `STATUS.md`,
`briefs/next-sequence.md`.

**Decision D-5 (orchestrator ruling, replaces the card's table list).** The card names
the futures Iceberg table, one bronze table from the cutover namespace, and one
synthetic. The bronze table lives in AWS Glue and this box carries no AWS credentials
(a `type = "glue"` session build fails `CredentialsNotLoaded`), so it is unreachable.
Measured instead, all local: (1) futures — the 20 MB market-data parquet CTAS
unpartitioned into a scratch warehouse; (2) synthetic-skewed — generated in-script,
timestamp plus low-cardinality string plus high-cardinality int, group shares skewed
50/50; (3) synthetic-uniform — same schema, even group shares, 200+ files at the
512 KiB scoring target. The document says so.

## PROPOSITION LEDGER — AP-0 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `ap0_reads_manifests_not_data`: every scored number comes from the `files` and `partitions` metadata tables; the only data reads are bounded single-column head samples for distinct counts, listed per bed. | The three bed-facts blocks name files/bytes/rows from both metadata tables, agree cell for cell, and list every sampled column with its LIMIT. | **PROVEN** | Bed-facts blocks pasted verbatim from the run (see Red first for the base-tree refusal): `rows 1000000 files 3 bytes 26729684` with `partitions table: rows 1000000 file_count 3 bytes 26729684 partition_column False` on futures; `rows 400000 files 206 bytes 7773590` with `partitions table: file_count 206` on both synthetic beds; `constant from bounds (no sample): interval_minutes, open_interest, ticker` on futures; `sampled: contract_symbol distinct=3; ticker_epoch distinct=200000; total_volume distinct=14029; total_ticks distinct=5883; up_ticks distinct=3512; down_ticks distinct=3507; up_volume distinct=8869; down_volume distinct=8915` on futures and `sampled: grp distinct=20; id distinct=200000` on both synthetic beds; `nulls observed: none` everywhere; no bounds-missing column anywhere, so the uniform-spread fallback never fired on missing bounds. Full blocks in the document §3. |
| C-002 | `ap0_generates_p2_candidates_for_each_column_kind`: timestamps/dates get year/month/day/hour, low-cardinality string/int gets identity, high-cardinality string/int gets bucket(8..128), plus no-partitioning, plus two-field pairs from the top three single fields. | One ranked table per bed shows every branch: truncate grains on all 6 futures temporal columns and on synthetic ts; identity on ticker/interval_minutes/open_interest/contract_symbol/grp; bucket rows on all 7 high-cardinality futures columns and on synthetic id; `none`; 3 pair rows per bed. | **PROVEN** | Futures ranks 22-27 are the six `month()` rows, ranks 42-48 the `year()` rows, ranks 53-58 the `day()` rows, ranks 62-67 the `hour()` rows; ranks 49-51 are `identity(interval_minutes)`, `identity(open_interest)`, `identity(ticker)`; rank 44 is `identity(contract_symbol)`; ranks 1-41 are the 35 `bucket(N)` rows over `down_ticks`, `down_volume`, `ticker_epoch`, `total_ticks`, `total_volume`, `up_ticks`, `up_volume`; rank 52 is `none`; ranks 59-61 are the two-field pairs from the top three fields (`down_ticks`, `down_volume`, `event_date`). Uniform and skewed show `identity(grp)`, `bucket(8/16/32/64/128, id)`, all four `ts` grains, `none`, and 3 pair rows each. Full tables in the document §3. |
| C-003 | `ap0_scores_and_ranks_by_p3`: one P-3 band score per candidate, ranked best first with projected partition and file counts at the 512 KiB target; the skew penalty bites on the skewed bed and not on the uniform bed. | Ranked tables are monotone non-decreasing in score; the uniform/skewed `identity(grp)` rows differ (0.000 vs 0.853) while every other shared row agrees; `month()` outranks `day()`/`hour()`/`year()` on all beds. | **PROVEN** | Pasted verbatim — uniform ranks 1-9: `bucket(16, id) 0.000`, `bucket(8, id) 0.000`, `identity(grp) 0.000 20 20`, `month(ts) 0.000 24 24`, `bucket(32, id) 0.000`, `year(ts) 1.707 2 16`, `none 2.707 1 15`, `bucket(64, id) 4.692`, `bucket(128, id) 68.692`. Skewed ranks 1-9 are identical except rank 5: `identity(grp) 0.853 20 27` — `g00` carries near 3.9 MB against the 2 MB cap. Futures: `month()` rows at 0.000/36/72, `year()` near 9.7, `day()` near 800-873, `hour()` near 24000-25000, `none` at 11.746/1/51. Full tables in the document §3; the plausibility read in §5. |
| C-004 | `ap0_reports_three_beds`: the run builds and reports futures, synthetic-skewed and synthetic-uniform, each with its build recipe, file layout and one ranked table, reproducible by one command. | Three `== bed ==` blocks print in one run; the document carries all three beds, the target size, the sample lists, the plausibility section and the exact reproduce command. | **PROVEN** | Pasted verbatim build lines: `built uniform: 200 batches in 129.4s`, `built skewed: 200 batches in 126.6s`, `built futures: CTAS in 5.4s`, `target_file_size_bytes 524288 sample_rows 200000`, then `== bed futures (ap.ns.futures) ==`, `== bed uniform (ap.ns.uniform) ==`, `== bed skewed (ap.ns.skewed) ==` with 67, 14 and 14 ranked rows respectively. Reproduce: `.venv/bin/python python/repark-parity/bench/adaptpart/run_adaptpart.py --scratch /tmp/ap0-bed` on a fresh root. |
| C-005 | `ap0_prediction_within_20pct`: the P-3 model predicts actual rewritten file sizes within 20 percent on one manually rewritten table. | A manual rewrite of one bed plus a before/after comparison of projected vs actual file sizes. | **REJECTED** (measured; the answer the card asked for) | Orchestrator O-run, 2026-09-10, on **two** beds. Partition count predicted exactly on both (20 predicted, 20 actual) and file count exactly on uniform (20/20). **Bytes are over-predicted by +76.2 % (uniform: 7,773,590 projected vs 4,413,222 actual) and +88.0 % (skewed: 7,773,590 vs 4,134,457)**, because P-3 sums pre-rewrite file bytes and a rewrite recompresses at 0.53-0.57x. The 20 percent bar is **not met on bytes**; it is met, exactly, on partition count. Numbers, the rewrite statement and the correction AP-1 must carry are in the document's O-run section. |

VERDICT: 5 clauses, 4 PROVEN, 0 OPEN, 1 REJECTED.

**Residue AP-0-R-001 (for AP-1).** P-3's byte projection must stop summing pre-rewrite
file bytes. Project from `record_count` times a measured post-rewrite bytes-per-row
(sampled from one rewritten partition, or a per-table compaction factor from the
table's own history). Partition-count arithmetic is unchanged.

**Residue AP-0-R-002 (for AP-1).** The file-count column was not fairly tested: the
O-run's partitioned CTAS writes one file per partition value (WRITE-DISTRIBUTION-1), so
no target-size binpack ran. Re-test it with `rewrite_data_files` carrying
`target-file-size-bytes`.

## Red first

The document's reproduce command was run on the base tree before the script existed.
It fails because there is nothing to run — the honest red for a measurement unit
whose pin is the script run itself. Exit 2:

```text
.venv/bin/python: can't open file '/tmp/oc-dp4/python/repark-parity/bench/adaptpart/run_adaptpart.py': [Errno 2] No such file or directory
```

After the script landed, the same command exits 0 and prints the three ranked tables
(evidence in C-001..C-004; full output in the document §3). No file was edited to
make the red pass except adding the script the command names.

## Gates

- `python/repark-parity` script run end to end: exit 0, three beds, 67 + 14 + 14 ranked rows.
- `make check-docs-compaction`: exit 0.
- `make check-ledgers`: exit 0.
- `make verify`: exit 0.
- Comment fence (`git diff --cached` grep for added `//`/`#` lines): prints nothing.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-0
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause was re-derived from the run rather than read off the hand-back — the orchestrator re-ran the document's own reproduce command on a fresh scratch root and matched the ranked tables cell for cell (skewed identity(grp) 0.853/20/27, uniform 0.000/20/20, none 2.707/1/15), then answered C-005 by measurement instead of accepting the card's expectation.
      artifacts: [python/repark-parity/bench/adaptpart/run_adaptpart.py, docs/perf/ap-0-partition-candidates-2026-09-10.md]
    - id: AT-2
      status: ATTACKED
      evidence: Both distribution boundaries exercised on the same schema — a uniform bed (20 even groups) and a skewed bed (one group carrying near half the rows) — plus a real 1M-row market frame with three files, so the scorer met a single-file table, a 206-file table, and a table whose largest partition is 18x its median. The P-2 branches were each driven by a real column: 200,000 distinct ids past the 1,000 limit into the bucket branch, 20 distinct groups into identity, three constant futures columns resolved from bounds with no sample.
      artifacts: [docs/perf/ap-0-partition-candidates-2026-09-10.md]
    - id: AT-3
      status: N/A
      justification: The unit adds no production failure path — a measurement script that builds throwaway beds in a scratch warehouse and prints tables. Its own failure mode is a non-zero exit, which is what the red-first evidence records.
    - id: AT-4
      status: N/A
      justification: No shared or mutable state, no concurrency: one session, built and stopped in a try/finally, over a private scratch warehouse per run. The document says a rerun needs a fresh root.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network. The AWS-backed bed the card named was dropped precisely because reaching it would need credentials this box does not carry (decision D-5).
    - id: AT-6
      status: ATTACKED
      evidence: The unit's integrity claim is P-1 — statistics come from manifests, not data. It was attacked by cross-checking the `files` and `partitions` metadata tables against each other on every bed (rows, file_count and bytes agree cell for cell) and by requiring every data read to be a declared, bounded, single-column head sample, listed per bed in the document.
      artifacts: [docs/perf/ap-0-partition-candidates-2026-09-10.md]
    - id: AT-7
      status: N/A
      justification: Not system-breaking. The script's cost is bounded by its own flags (200 batches, 2,000 rows) and it touches no product path; the ~5-minute build time is stated in the document.
    - id: AT-8
      status: ATTACKED
      evidence: The one upstream behaviour the unit could have silently presumed — that a rewrite preserves the byte totals the manifests report — was not presumed. It was measured, and it is false at 0.53-0.57x, which is the whole content of C-005 and residue AP-0-R-001. No `crates/` or facade code changed; no public name added.
      artifacts: [task/ledgers/completed/ap-0-ledger.md, docs/perf/ap-0-partition-candidates-2026-09-10.md]
    - id: AT-9
      status: ATTACKED
      evidence: The run is its own record — each bed prints its facts block, its sample list and its ranked table, and the document pastes them verbatim with the exact command that regenerates them, so a disagreement is diagnosable without rerunning anything.
      artifacts: [docs/perf/ap-0-partition-candidates-2026-09-10.md]
    - id: AT-10
      status: ATTACKED
      evidence: A measurement unit's pin is its reproduce command, and it was run red on the base tree (exit 2, no such file) and green after. Adequacy was attacked by re-running it independently on a second scratch root and by extending C-005's O-run from the card's one table to two, so a single bed's quirk could not carry the verdict — and it did not: uniform and skewed disagree on the file-count column and agree on the byte error.
      artifacts: [python/repark-parity/bench/adaptpart/run_adaptpart.py, task/ledgers/completed/ap-0-ledger.md]
  complete: true
```
