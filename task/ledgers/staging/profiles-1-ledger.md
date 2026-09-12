# Unit ledger — PROFILES-1 steps 1–2 · the measurement bed and the knob sweep

**Unit:** PROFILES-1 steps 1–2 · **Date:** 2026-09-10, step 2 2026-09-12 · **Branch:** `feat/profiles-1` / `feat/profiles-1-step-2` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor); step 2 Devin SWE-2 (swe-2-high)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Step 0 is merged (`docs/perf/profiles-1-passthrough-probe-2026-09-09.md`:
11 pass through, 9 accepted but unread, 0 refused). Step 1 builds the sweep harness step 2
will run alone on the box on a release build: three datasets, eight query shapes, a
knob × value timing harness writing one CSV row per cell, a one-JVM guard, and a `--smoke`
mode that proves the harness at tiny scale.

**Not in this step:** the sweep, any timing number as a result, `maturin develop --release`,
TPC-H at SF10, `docs/perf/config-profiles-*.md`, profile tables. Another lane builds on this
box; timed runs now would be lies, so the harness refuses unless the box is quiet.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

## PROPOSITION LEDGER — PROFILES-1 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The bed builds all three D-2 datasets: the owner's futures parquet, TPC-H through the existing `tpch.datagen` dbgen path (no second dbgen), and a local Iceberg v2 table with 200 files. | `profiles_bed_builds_three_datasets` | **PROVEN** | `test_bed_builds_three_datasets` green; `datasets.py` imports `ensure_parquet_sf` from `tpch.datagen` (no second dbgen); one file per single-partition write measured (fresh 4-file build holds exactly 4 parquet files), so 200 writes hold 200 files. |
| C-002 | The bed runs five read shapes (scan+filter, group-by, hash join, sort-merge join, window) and three write shapes (append 8 files, INSERT OVERWRITE one partition, MERGE 10 % updates). | `profiles_bed_runs_five_reads_and_three_writes` | **PROVEN** | `test_bed_runs_five_reads_and_three_writes` green; every query forces execution through `to_arrow`. The engine refused the first overwrite shape (`too many data columns`: static `PARTITION` injects `g`), so the source projects `(id, v)` only — measured, not guessed. |
| C-003 | The harness writes one CSV row per (dataset, query, knob, value, repetition, seconds) and reports the median over three repetitions. | `profiles_harness_writes_one_csv_row_per_cell`, `profiles_harness_writes_rows_for_a_matrix` | **PROVEN** | Both pins green; the smoke CSV holds the header plus one row per cell (10 rows, pasted below). Non-smoke runs refuse unless `--repeats 3` (exit 2, measured). |
| C-004 | The harness checks `pgrep -f java` before every timed run and refuses loudly when a JVM is running. | `profiles_harness_refuses_when_a_jvm_is_running` | **PROVEN** | Pin green via stubbed `pgrep` (found refuses naming `pgrep -f java`, absent passes); `measure` and every write rebuild call the guard per repetition. A shell whose own command contains that string self-matches; the runner is immune and the refusal names PIDs. |
| C-005 | `--smoke` runs every dataset and query once at tiny scale and completes. | `profiles_smoke_completes` | **PROVEN** | `SMOKE OK` on a debug native, SF0.01 (smallest scale the dbgen path accepts), 4-file Iceberg bed, 2000-row futures stand-in. Full output pasted below. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

The pins ran before the bed existed and failed on the missing module:

```text
E   ModuleNotFoundError: No module named 'profiles'
1 error in 0.11s
```

One pin needed a fix after the bed landed (the pin's fault, not the bed's): the
stubbed `subprocess.run` took `*a` only while the harness passes keyword arguments,
so `test_harness_refuses_when_a_jvm_is_running` failed with `TypeError ... got an
unexpected keyword argument 'capture_output'` until the stub accepted `**k`.

## Smoke evidence

`.venv/bin/python python/repark-parity/bench/profiles/run_profiles.py --smoke --scratch /tmp/oc-profiles-smoke`
(debug native; box quiet-checked with `pgrep -f '[j]ava'`, exit 1, before the run):

```text
futures  scan_filter         (baseline)=@default n=1 median=0.028s
tpch     scan_filter         (baseline)=@default n=1 median=0.068s
futures  group_by            (baseline)=@default n=1 median=0.057s
tpch     group_by            (baseline)=@default n=1 median=0.115s
tpch     hash_join           (baseline)=@default n=1 median=0.175s
tpch     sort_merge_join     (baseline)=@default n=1 median=0.088s
futures  window              (baseline)=@default n=1 median=0.055s
iceberg  append_files        (baseline)=@default n=1 median=0.124s
iceberg  overwrite_partition (baseline)=@default n=1 median=0.050s
iceberg  merge_updates       (baseline)=@default n=1 median=0.275s
cells=/tmp/oc-profiles-smoke/cells.csv iceberg_parquet_files=16
SMOKE OK
```

These numbers prove the harness runs; they are not measurements (debug native, tiny
scale, another lane building on the box). No timing is reported as a result.

The 16-file count reconciles exactly: three fresh 4-file builds (12) + 2 smoke
appends + 1 overwrite rewrite + 1 merge rewrite. `DROP TABLE` orphans prior files,
so step 2 should sweep each knob on a fresh scratch root.

## PROPOSITION LEDGER — PROFILES-1 step 2 — 2026-09-12

Step 2 (M, card PROFILES-1): the sweep and the measurements document. The step-0 probe's
table is re-checked after CONF-UNREAD-1: the three `datafusion.execution.parquet.*` keys it
left UNREAD now pass through, `coalesce_batches` refuses loud, so the swept set is the
nineteen keys whose values reach the engine. One harness fix was needed and is named in the
hand-back: the two `write.*` knobs are Iceberg table properties, not session conf, so
`run_profiles.py` lands them on the rebuilt bed table with `ALTER TABLE … SET TBLPROPERTIES`
before the timed write (`table_property_alter`, pinned).

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-006 | The baseline ran at full scale on a release build with the quiet-box guard: futures parquet, TPC-H SF10, 200-file Iceberg table, three repetitions per cell. | `profiles_baseline_ran_at_full_scale` | OPEN | |
| C-007 | Every swept knob's CSV carries its full value set with three repetitions per (dataset, query, value) cell; the two `write.*` knobs reach the bed table as properties. | `profiles_sweep_csvs_carry_three_reps_per_cell` | OPEN | |
| C-008 | The measurements document's numbers equal the medians recomputed from the committed CSVs, and its "no effect measured" table equals the <5 % rule applied to those medians. | `profiles_doc_tables_equal_csv_medians` | OPEN | |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: profiles-1-step-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Pins assert the three datasets, dbgen reuse, and the 200-file shape.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py, python/repark-parity/bench/profiles/datasets.py]
    - id: AT-2
      status: ATTACKED
      evidence: Pins assert all eight query names plus append count and merge fraction.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py, python/repark-parity/bench/profiles/queries.py]
    - id: AT-3
      status: ATTACKED
      evidence: JVM guard refusal is pinned both ways; live JVM absence checked before smoke.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py, python/repark-parity/bench/profiles/harness.py]
    - id: AT-4
      status: ATTACKED
      evidence: One session per knob value, stopped after; write table rebuilt per repetition.
      artifacts: [python/repark-parity/bench/profiles/run_profiles.py]
    - id: AT-5
      status: N/A
      justification: No product code changed; the bed is measurement-only under bench/.
    - id: AT-6
      status: N/A
      justification: No catalog, write path, or exception taxonomy touched.
    - id: AT-7
      status: ATTACKED
      evidence: CSV header and row shape pinned; smoke CSV holds 10 data rows.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py]
    - id: AT-8
      status: ATTACKED
      evidence: Refusal paths measured live (repeats guard, knobless values guard, both exit 2).
      artifacts: [python/repark-parity/bench/profiles/run_profiles.py]
    - id: AT-9
      status: N/A
      justification: No concurrency, locks, or shared state; one process, sequential cells.
    - id: AT-10
      status: ATTACKED
      evidence: Overwrite arity refusal and catalog re-registration refusal met live and fixed in the bed.
      artifacts: [python/repark-parity/bench/profiles/queries.py, python/repark-parity/bench/profiles/datasets.py]
  complete: true
```
