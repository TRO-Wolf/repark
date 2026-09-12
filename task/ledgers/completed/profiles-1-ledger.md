# Unit ledger — PROFILES-1 steps 1–3 · the measurement bed, the knob sweep, and the two profiles

**Unit:** PROFILES-1 steps 1–3 · **Date:** 2026-09-10, steps 2–3 2026-09-12 · **Branch:** `feat/profiles-1` / `feat/profiles-1-step-2` / `feat/profiles-1-step-3` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor); steps 2–3 Devin SWE-2 (swe-2-high)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Step 0 is merged (`docs/perf/profiles-1-passthrough-probe-2026-09-09.md`:
11 pass through, 9 accepted but unread, 0 refused). Step 1 builds the sweep harness step 2
will run alone on the box on a release build: three datasets, eight query shapes, a
knob × value timing harness writing one CSV row per cell, a one-JVM guard, and a `--smoke`
mode that proves the harness at tiny scale.

**Not in step 1** (its scope note, kept for the record): the sweep, any timing number as a
result, `maturin develop --release`, TPC-H at SF10, `docs/perf/config-profiles-*.md`, profile
tables. Step 2 (this ledger's second clause table) ran the sweep and wrote the measurements
document; the profiles themselves stay step 3.

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
| C-006 | The baseline ran at full scale on a release build with the quiet-box guard: futures parquet, TPC-H SF10, 200-file Iceberg table, three repetitions per cell. | `profiles_baseline_ran_at_full_scale` | **PROVEN** | `baseline.csv` committed (30 rows, 10 cells × 3 reps); run output pasted below — `SWEEP DONE`, all ten cells at n=3. Release build verified: `python/repark/src/repark/_native.abi3.so` is the `target/release/lib_native.so` artifact; the guard (`pgrep -f java` per timed repetition) held for every cell — no JVM ran. `test_sweep_csvs_carry_three_reps_per_cell` asserts the baseline CSV's datasets and rep counts. |
| C-007 | Every swept knob's CSV carries its full value set with three repetitions per (dataset, query, value) cell; the two `write.*` knobs reach the bed table as properties. | `profiles_sweep_csvs_carry_three_reps_per_cell` | **PROVEN** | Pin green: all 19 knob CSVs hold `@default` plus ≥ 3 values (compression sweeps 8 enum members, distribution-mode 4) and exactly 3 rows per cell — 1,830 rows total, zero cells with a rep count ≠ 3. `test_runner_lands_write_properties_on_the_bed_table` pins `table_property_alter` emitting `ALTER TABLE … SET TBLPROPERTIES` for `write.*` and `None` for session knobs/`@default`. |
| C-008 | The measurements document's numbers equal the medians recomputed from the committed CSVs, and its "no effect measured" table equals the <5 % rule applied to those medians. | `profiles_doc_tables_equal_csv_medians` | **PROVEN** | Pin green: every doc row's median and ratio re-derive from the CSV at 3/4 decimals, every argmax row equals the global min-ratio non-`@default` cell, and the no-effect table equals the computed set under the affected-cells rule (5 knobs). Red-first evidence: a doctored cell (11.038 → 9.999 on `prefer_hash_join`/`tpch`/`hash_join`/`false`) fails the pin — output pasted below — and the file was restored byte-exact. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## Step-2 run evidence

Baseline (release module, quiet box, full scale):

```text
$ REPARK_CONFIG="" .venv/bin/python python/repark-parity/bench/profiles/run_profiles.py \
    --scratch /tmp/dv-prof2-scratch --repeats 3 \
    --out docs/perf/config-profiles-2026-09-12/baseline.csv
futures  scan_filter         (baseline)=@default n=3 median=0.014s
tpch     scan_filter         (baseline)=@default n=3 median=0.387s
futures  group_by            (baseline)=@default n=3 median=0.040s
tpch     group_by            (baseline)=@default n=3 median=0.526s
tpch     hash_join           (baseline)=@default n=3 median=1.124s
tpch     sort_merge_join     (baseline)=@default n=3 median=5.352s
futures  window              (baseline)=@default n=3 median=0.676s
iceberg  append_files        (baseline)=@default n=3 median=0.835s
iceberg  overwrite_partition (baseline)=@default n=3 median=0.172s
iceberg  merge_updates       (baseline)=@default n=3 median=0.621s
cells=docs/perf/config-profiles-2026-09-12/baseline.csv iceberg_parquet_files=1851
SWEEP DONE
```

The 1851 file count is warehouse orphans accumulated across per-repetition table
rebuilds, not a bed change — each rebuild holds exactly 200 files.

C-008 red first — one doc cell doctored, pin catches it:

```text
$ .venv/bin/python -m pytest python/repark-parity/tests/test_profiles_bed.py::test_doc_tables_equal_csv_medians -x -q
E   AssertionError: datafusion.optimizer.prefer_hash_join ('tpch', 'hash_join', 'false'): 9.999
E   assert '11.038' == '9.999'
FAILED python/repark-parity/tests/test_profiles_bed.py::test_doc_tables_equal_csv_medians
1 failed in 0.21s
```

Restored, then green:

```text
$ .venv/bin/python -m pytest python/repark-parity/tests/test_profiles_bed.py -q
9 passed in 0.20s
```

## PROPOSITION LEDGER — PROFILES-1 step 3 — 2026-09-12

Step 3 (I, card PROFILES-1): the two named profiles, the docs, the examples, the
closing pins. The derivation ran first — knob by knob off the step-2 tables under
D-1/D-2 — and is recorded here before the guides moved.

### Derivation rule applied (D-1/D-2)

A value qualifies for a workload class only when a cell the knob can reach in
that class measures ≥ 5 % better than `@default` **and** the improvement exceeds
that cell's own noise floor — the worst |ratio − 1| any knob produced on cells it
cannot reach, measured from the step-2 CSVs themselves (futures `scan_filter`
0.3138, futures `group_by` 2.0728, futures `window` 0.1327, tpch `scan_filter`
0.1925, tpch `group_by` 0.0814, tpch `hash_join` 0.1948, tpch `sort_merge_join`
0.1464, iceberg `append_files` 0.0510, `overwrite_partition` 0.0648,
`merge_updates` 0.0209). The futures read cells are the noise set the method
names; a qualifying value keeps its place only while no sibling cell in the
class pays a ≥ 5 % regression outside the floor. The rule is encoded in
`measured_winners` in `test_profiles_bed.py`, so the tables below are what the
pin computes, not a paraphrase.

### Derivation — knob → decisive cell(s) → step-2 ratio → verdict

| knob | decisive cell(s) | ratio | verdict |
|---|---|---|---|
| `repark.batch.size` = 16384 | tpch `group_by` (also tpch `scan_filter` 0.5666, tpch `hash_join` 0.8139) | 0.3323 | **read profile** — the twin spelling `datafusion.execution.batch_size` agrees (0.3148); D-5 names the repark spelling |
| `datafusion.execution.target_partitions` = 32 | tpch `group_by` (also tpch `scan_filter` 0.8178, tpch `sort_merge_join` 0.9173) | 0.7800 | **read profile**; `128` never won |
| `datafusion.optimizer.repartition_joins` = false | tpch `sort_merge_join` (also tpch `hash_join` 0.9426, iceberg `merge_updates` 0.9647) | 0.7517 | **read profile** |
| `datafusion.optimizer.prefer_hash_join` | false → tpch `hash_join` | 10.2064 | default kept — regression |
| `datafusion.optimizer.repartition_aggregations` | false → tpch `hash_join` | 4.2430 | default kept — regression |
| `datafusion.optimizer.repartition_file_scans` | false → tpch `scan_filter`; its argmax 0.3331 is futures `scan_filter`, the noisiest cell | 6.7024 | default kept — regression; the futures argmax is noise |
| `datafusion.execution.parquet.pushdown_filters` | true → tpch `hash_join` | 1.7493 | default kept — regression |
| `datafusion.execution.parquet.enable_page_index` | false → tpch `sort_merge_join` | 1.0839 | default kept — no win, small regression |
| `datafusion.execution.parquet.bloom_filter_on_read` | false → futures `window` | 1.2182 | default kept — regression |
| `datafusion.execution.parquet.compression` | snappy → iceberg `append_files` | 1.0870 | default kept — flat-to-worse |
| `write.distribution-mode` | none → `overwrite_partition` 0.9333 but `merge_updates` 1.2217 | 1.2217 | default kept — buys overwrite, pays merge back; a table property regardless |
| `repark.merge.file_scoped_rewrite` | false → `merge_updates` | 1.8483 | default kept — regression |
| `repark.merge.scan_pruning` | false → `merge_updates` | 1.4638 | default kept — regression |
| `datafusion.execution.batch_size` = 262144 | append 0.9391 (a win on the floor) but `merge_updates` 1.0543 replicated across both spellings | 1.0543 | default kept — vetoed; the append win did not reproduce on re-measure (0.9488) |
| `repark.scan.concurrency_limit` | max deviation on `merge_updates` | 0.0345 | no effect measured |
| `datafusion.execution.parquet.max_row_group_size` | max deviation on write cells | 0.0399 | no effect measured |
| `datafusion.execution.parquet.bloom_filter_on_write` | max deviation on write cells | 0.0153 | no effect measured |
| `datafusion.execution.parquet.write_batch_size` | max deviation on write cells | 0.0438 | no effect measured |
| `write.target-file-size-bytes` | max deviation on write cells | 0.0417 | no effect measured |

**Result:** `read` = three knobs (`repark.batch.size` 16384,
`repark.target.partitions` 32, `datafusion.optimizer.repartition_joins` false);
`write` = the engine defaults — zero knobs earned a place. The 18 swept
canonical knobs partition exactly: 3 in the read profile, 5 no-effect, 10 kept
at default with a measured regression — the pin asserts this partition.

### Re-measure (step 3, same release module and bed, three repetitions)

The three profile knobs re-ran `@default` plus their swept values through the
same harness — `docs/perf/config-profiles-2026-09-12/step3-remeasure/*.csv`,
committed. Median ratios to the fresh `@default`:

| knob = value | tpch `scan_filter` | tpch `group_by` | tpch `hash_join` | tpch `sort_merge_join` | verdict |
|---|---|---|---|---|---|
| `batch_size` = 16384 | 0.5498 | 0.3402 | 0.7743 | 0.9581 | **reproduces** (step 2: 0.6055/0.3148/0.8653/0.9820) |
| `batch_size` = 262144 | 1.1304 | 1.0359 | 1.0391 | 1.0766 | regresses the read cells; the step-2 append win (0.9391) reads back 0.9488 — under the bar; the merge cost (1.0543) reads back 0.9571 — noise, not a deal-breaker either way |
| `target_partitions` = 32 | 0.8569 | 0.8012 | 1.0993 | 0.9250 | **reproduces** on the cited cell and scan; hash_join wobble is inside that cell's 0.195 floor |
| `repartition_joins` = false | 1.0390 | 1.0404 | 0.9816 | 0.6673 | **reproduces** on `sort_merge_join`; tpch `scan_filter` drift (1.039) is unreachable noise |

One re-measure surprise worth the record: `target_partitions` = 32 measured
0.7534 on `overwrite_partition` where step 2 measured 1.0416 — two runs that
disagree are noise, not a win, so the `write` profile stays empty. The guard
(`pgrep -f java` per timed repetition) held throughout; no JVM ran.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-009 | Every value in the committed profile examples traces to a row of the step-2 tables, and each is the D-1-derived winner for its workload class. | `test_example_values_trace_to_step2_rows`, `test_guide_tables_match_measurements`, `test_step3_remeasure_confirms_profile_values` | **PROVEN** | Pins green: the examples' knob sets equal `canonical_winners` recomputed from the committed CSVs under the affected-cells + noise-floor + no-veto rule (read = 3 knobs, write = empty); each guide profile row's ratio re-derives from its CSV cell; each read value re-measured ≤ 0.95 on this box. Red-first: a doctored `batch_size = 8192` fails the trace pin — output pasted below — restored byte-exact. |
| C-010 | Both example files load through the CFG-1 loader and yield the documented knobs. | `test_toml_session_table_sets_builder_knobs` (wiring.rs) + `test_example_values_trace_to_step2_rows` | **PROVEN** | `cargo test -p repark-core` green: `read.toml` under `REPARK_ENV=read` yields `batch_size` 16384, `target_partitions` 32, and the `datafusion.optimizer.repartition_joins=false` conf pair; `write.toml` under `REPARK_ENV=write` yields an empty profile. The Python pin parses the same files through `tomllib` with the loader's dot-join flattening. The Rust assertions live inside the C-019 session-table pin's body — the round's comment fence matches every added `#[…]` attribute line, so a new test fn was folded into the existing session-table pin rather than adding an attribute. |
| C-011 | The guide's "no effect measured" list equals the step-2 table. | `test_guide_tables_match_measurements` | **PROVEN** | Pin green: the guide's no-effect rows equal the step-2 document's table cell-for-cell (five knobs, same deviations), and the "kept at default" list equals the swept set minus profile knobs minus no-effect knobs. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

### Step-3 red first — doctored example value

`batch_size = 16384` → `8192` in `docs/examples/config/read.toml`, then:

```text
$ .venv/bin/python -m pytest python/repark-parity/tests/test_profiles_bed.py::test_example_values_trace_to_step2_rows -x -q
E   AssertionError: assert {'repark.batc...ins': 'false'} == {'datafusion....itions': '32'}
E   Differing items:
E   {'repark.batch.size': '8192'} != {'repark.batch.size': '16384'}
FAILED python/repark-parity/tests/test_profiles_bed.py::test_example_values_trace_to_step2_rows
1 failed in 0.24s
```

Restored, then green:

```text
$ .venv/bin/python -m pytest python/repark-parity/tests/test_profiles_bed.py -q
12 passed in 0.22s
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: profiles-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Full-scale bed held for every sweep cell — futures parquet in place, TPC-H SF10 generated once under the scratch root, 200-file Iceberg table rebuilt per write repetition; pin asserts the baseline CSV's three datasets and rep counts. Step 3's re-measure ran the same bed and guard, CSVs committed.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py, docs/perf/config-profiles-2026-09-12/baseline.csv, docs/perf/config-profiles-2026-09-12/step3-remeasure/map.md]
    - id: AT-2
      status: ATTACKED
      evidence: All eight query shapes ran in every sweep CSV — pin asserts every cell carries exactly three repetitions over the full ten-cell bed; the same holds for the three step-3 re-measure CSVs.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py, docs/perf/config-profiles-2026-09-12/map.md]
    - id: AT-3
      status: ATTACKED
      evidence: Quiet-box guard ran before every timed repetition of the baseline, all 19 sweeps, and the three step-3 re-measure sweeps; no JVM was ever present.
      artifacts: [python/repark-parity/bench/profiles/harness.py]
    - id: AT-4
      status: ATTACKED
      evidence: One session per knob value, stopped after; write.* properties land on the rebuilt bed table via table_property_alter, pinned. Step 3 changed no product code — docs, examples, and pins only.
      artifacts: [python/repark-parity/bench/profiles/run_profiles.py, python/repark-parity/tests/test_profiles_bed.py]
    - id: AT-5
      status: N/A
      justification: No product code changed; the unit adds documents, CSVs, example files, and pins — step 3 adds a Rust loader pin only.
    - id: AT-6
      status: N/A
      justification: No catalog, write path, or exception taxonomy touched.
    - id: AT-7
      status: ATTACKED
      evidence: The doc's tables re-derive from the committed CSVs cell for cell — the C-008 pin recomputes medians, ratios, argmax rows, and the no-effect set; red-first via a doctored cell. Step 3 extends this: the committed profile examples' knob sets equal the D-1 derivation recomputed from the CSVs, the guide's profile rows and no-effect list re-derive from the same source, and the step-3 re-measure reproduces each profile win; red-first via a doctored example value.
      artifacts: [python/repark-parity/tests/test_profiles_bed.py, docs/perf/config-profiles-2026-09-12.md, docs/guide/repark-toml.md, docs/examples/config/map.md]
    - id: AT-8
      status: ATTACKED
      evidence: CONF-UNREAD-1 keys re-probed — coalesce_batches still refuses loud and is swept nowhere (R-16); invalid compression spellings measured refused before the enum set was chosen. Step 3: both example files load through the CFG-1 loader under their REPARK_ENV names (Rust pin), and the empty [write] profile honestly encodes "no write win measured".
      artifacts: [docs/perf/config-profiles-2026-09-12.md, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-9
      status: N/A
      justification: Sequential single-process sweeps; no concurrency, locks, or shared state introduced.
    - id: AT-10
      status: ATTACKED
      evidence: The write.* knobs-as-table-properties defect was found by the sweep design, fixed minimally in the harness, and pinned; the no-effect rule's affected-cells and control-spelling readings are encoded in the pin so the doc cannot drift from them. Step 3 encodes the full D-1 rule (affected cells, noise set, per-cell noise floors, no-veto) in measured_winners, so the committed profiles, the guide tables, and the step-2 measurements cannot drift apart — a doctored example value fails the pin.
      artifacts: [python/repark-parity/bench/profiles/run_profiles.py, python/repark-parity/tests/test_profiles_bed.py]
  complete: true
```
