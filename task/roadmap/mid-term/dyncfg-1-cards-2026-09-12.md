# DYNCFG-1 cards — dynamic configuration (`[<profile>.autotune]`), intake cut

**Date:** 2026-09-12 · **Author:** Devin SWE-2 (swe-2-high), orchestrated by Claude Opus 5
(run 9b) · **Base read:** `0233d96f` · **Status:** intake — cards proposed, owner charters.
The epic and its entry criterion live in
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) "### DYNCFG-1 — dynamic
configuration planning"; the process, preamble, tier legend and gates are that slate's §1 and
[cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) §0–§1 (S2-15: Devin is the
default for M rounds and first try for I rounds; Grok is the I fallback). This file adds only
the intake verdict, the pre-made rulings, the parked questions and the cards.

**What this is.** PROFILES-1 has reported: the sweep document
[docs/perf/config-profiles-2026-09-12.md](../../../docs/perf/config-profiles-2026-09-12.md),
its committed CSVs under
[docs/perf/config-profiles-2026-09-12/](../../../docs/perf/config-profiles-2026-09-12/map.md)
(including `step3-remeasure/`), and the shipped profiles in
[docs/guide/repark-toml.md](../../../docs/guide/repark-toml.md) "The measured `read` and
`write` profiles" / "No effect measured". Every figure below traces to one of those files;
nothing is estimated.

## 0. What the sweep says

The epic's entry criterion: **PROFILES-1 has shown at least one knob moving a measurement by
more than 5 %**. The verdict is **met, widely**: 13 of the 18 canonical swept knobs (14 of
the 19 swept spellings — the two batch-size spellings drive one engine option and both moved)
produced a deviation beyond 5 % on a cell the knob can reach; three produced qualifying wins
that the step-3 re-measure confirmed and that now ship as the `read` profile
([docs/guide/repark-toml.md](../../../docs/guide/repark-toml.md)). The unit is un-deferred on
this evidence.

One row per swept spelling. "Baseline" is that knob's own `@default` median on the cited
cell, "best" is the most favourable non-control spelling measured there; a knob whose only
movement was worse is marked regression, matching the guide's "kept at default" list. Ratios
are medians of three repetitions (repetition 1 cold, per the step-2 method).

### Read-side knobs (affected cells include the seven read cells; join/scan knobs reach `merge_updates` too)

| knob | decisive cell(s) | baseline s | best value → ratio | Δ | noise / repeat evidence | step-3 re-measure |
|---|---|---|---|---|---|---|
| `repark.batch.size` | tpch `group_by` (also tpch `scan_filter` 0.5666, tpch `hash_join` 0.8139) | 0.526 | `16384` → 0.3323 | −66.8 % | wins sit on tpch cells, not the futures noise set; rep 1 cold absorbed by the median | **confirmed** — the twin spelling's re-measure reads 0.3402 / 0.5498 / 0.7743 / 0.9581 on the four tpch cells (`step3-remeasure/datafusion.execution.batch_size.csv`) |
| `datafusion.execution.batch_size` | tpch `group_by` (also tpch `scan_filter` 0.6055, tpch `hash_join` 0.8653) | 0.537 | `16384` → 0.3148 | −68.5 % | agrees with the repark spelling (0.3323) — one engine option, two spellings | **confirmed** — same re-measure CSV as above |
| `datafusion.execution.target_partitions` | tpch `group_by` (also tpch `scan_filter` 0.8178, tpch `sort_merge_join` 0.9173) | 0.535 | `32` → 0.7800 | −22.0 % | `128` regressed append 1.2230 and window 1.2212 beyond those cells' floors; the futures-scan argmax 0.5542 is the noisiest cell | **confirmed** — re-measure 0.8569 / 0.8012 / 1.0993 / 0.9250; the `hash_join` wobble sits inside that cell's noise floor (`step3-remeasure/datafusion.execution.target_partitions.csv`, `@default` + `32` rows) |
| `datafusion.optimizer.repartition_joins` | tpch `sort_merge_join` (also tpch `hash_join` 0.9426, iceberg `merge_updates` 0.9647) | 5.247 | `false` → 0.7517 | −24.8 % | `true` is a control spelling (equals the default); the win is on the bed's heaviest cell | **confirmed** — re-measure 0.6673 on `sort_merge_join` (`step3-remeasure/datafusion.optimizer.repartition_joins.csv`, `@default` + `false` rows) |
| `datafusion.optimizer.prefer_hash_join` | tpch `hash_join`, tpch `sort_merge_join` | 1.082 | `false` → 10.2064 | +920.6 % | catastrophic regression, not noise; its argmax 0.8233 landed on an unreachable futures cell | not re-measured (only the three winners re-ran) |
| `datafusion.optimizer.repartition_aggregations` | tpch `hash_join` | 1.105 | `false` → 4.2430 | +324.3 % | regression far outside the floor | not re-measured |
| `datafusion.optimizer.repartition_file_scans` | futures `scan_filter` vs tpch `scan_filter` | 0.013 / 0.272 | `false` → 0.3331 and 6.7024 | −66.7 % / +570.2 % | the sweep's largest single improvement landed on its noisiest cell while the same shape on TPC-H regressed 6.7× — vetoed by sibling regression (guide) | not re-measured |
| `datafusion.execution.parquet.pushdown_filters` | tpch `hash_join` | 1.107 | `true` → 1.7493 | +74.9 % | regression on every read cell it reached (`false` is the control spelling) | not re-measured |
| `datafusion.execution.parquet.enable_page_index` | tpch `sort_merge_join` | 5.263 | `false` → 1.0839 | +8.4 % | small regression, no win; its futures `group_by` 1.2763 sits inside that cell's floor | not re-measured |
| `datafusion.execution.parquet.bloom_filter_on_read` | futures `window` | 0.656 | `false` → 1.2182 | +21.8 % | regression beyond that cell's floor; the futures `group_by` 2.2571 stays inside its 2.0728 floor | not re-measured |
| `repark.scan.concurrency_limit` | iceberg `merge_updates` (only reachable cell) | 0.592 | `16` → 1.0345 | +3.5 % | **no effect measured** — max deviation on affected cells 0.0345 (doc "No effect measured" row) | not re-measured |

### Write-side knobs (affected cells are the three Iceberg write cells; merge knobs reach `merge_updates` only)

| knob | decisive cell(s) | baseline s | best value → ratio | Δ | noise / repeat evidence | step-3 re-measure |
|---|---|---|---|---|---|---|
| `datafusion.execution.parquet.compression` | iceberg `append_files` | 0.784 | `snappy` → 1.0870 | +8.7 % | only `snappy` left the floor; `zstd(3)` spelled is a control | not re-measured |
| `datafusion.execution.parquet.max_row_group_size` | write cells | — | — | ≤ 4.0 % | **no effect measured** — max deviation 0.0399 | not re-measured |
| `datafusion.execution.parquet.bloom_filter_on_write` | write cells | — | — | ≤ 1.5 % | **no effect measured** — max deviation 0.0153 | not re-measured |
| `datafusion.execution.parquet.write_batch_size` | write cells | — | — | ≤ 4.4 % | **no effect measured** — max deviation 0.0438 | not re-measured |
| `write.target-file-size-bytes` | write cells | — | — | ≤ 4.2 % | **no effect measured** — max deviation 0.0417; a table property, not session conf | not re-measured |
| `write.distribution-mode` | iceberg `overwrite_partition` vs `merge_updates` | 0.181 / 0.629 | `none` → 0.9333 and 1.2217 | −6.7 % / +22.2 % | buys `INSERT OVERWRITE`, pays `MERGE` back — vetoed; `hash`/`range` are control spellings; a table property | not re-measured |
| `repark.merge.file_scoped_rewrite` | iceberg `merge_updates` | 0.598 | `false` → 1.8483 | +84.8 % | large regression on the only cell it reaches | not re-measured |
| `repark.merge.scan_pruning` | iceberg `merge_updates` | 0.594 | `false` → 1.4638 | +46.4 % | regression on the only cell it reaches | not re-measured |

Not swept: `datafusion.execution.coalesce_batches` — refuses loud at `.config()` and at
runtime `conf.set` because DataFusion 54.1.0 defines the option but no engine path reads it
(the step-2 Method "Not swept" row; the step-0 probe's §2 row, R-16).

Noise and repeat evidence, as the CSVs record it: every cell carries three repetitions with
the median reported; the per-cell noise floors recomputed from the step-2 CSVs (worst
|ratio − 1| any knob produced on a cell it cannot reach) are futures `scan_filter` 0.3138,
futures `group_by` 2.0728, futures `window` 0.1327, tpch `scan_filter` 0.1925, tpch
`group_by` 0.0814, tpch `hash_join` 0.1948, tpch `sort_merge_join` 0.1464, iceberg
`append_files` 0.0510, `overwrite_partition` 0.0648, `merge_updates` 0.0209 — the futures
read cells are the noise set the guide names, and no profile entry rests on one.

Measured cost basis for the matrix: the no-knob bed pass totals 30.3 s of timed cells
(`baseline.csv`, 30 rows); each three-value knob sweep totals ~83–110 s (per-knob CSVs, 90
rows each; `prefer_hash_join`'s 157.5 s includes its own regression cells) and the eight-value
`compression` sweep 241.2 s; the whole 19-spelling sweep is 1 995.4 s of timed cells. A
three-knob matrix over the full bed is therefore ~4.5 minutes of timed work; a sampled
dataset only lowers the per-cell times.

## 1. Rulings pre-made (binding where the numbers decide them)

- **DC-1 Matrix membership.** The autotune matrix contains exactly the canonical knobs with
  a measured qualifying win: `batch_size` (`16384` measured best; `262144` the other swept
  bound), `target_partitions` (`32`, `128`), `repartition_joins` (`false`). `@default` is
  always a cell — the honest control every ratio is measured against. Citations: the guide's
  `read` profile table (`repark.batch.size` 0.3323, `repark.target.partitions` 0.7800,
  `datafusion.optimizer.repartition_joins` 0.7517); the step-2 swept-values table and each
  knob's table rows; the re-measure CSVs.
- **DC-2 The five no-effect knobs are excluded** — `repark.scan.concurrency_limit` (0.0345),
  `datafusion.execution.parquet.max_row_group_size` (0.0399),
  `datafusion.execution.parquet.bloom_filter_on_write` (0.0153),
  `datafusion.execution.parquet.write_batch_size` (0.0438), `write.target-file-size-bytes`
  (0.0417). The doc's "No effect measured" table and the guide's identical list.
- **DC-3 `datafusion.execution.coalesce_batches` is excluded** — it refuses loud at
  `.config()` (R-16; step-2 Method "Not swept"; probe §2 row).
- **DC-4 The `write.*` table properties are excluded from a session-conf matrix** — they are
  Iceberg table properties the session only stores; the write path reads the table's own
  property (probe §3 rows for `write.target-file-size-bytes` and `write.distribution-mode`;
  the harness's pinned `table_property_alter` fix lands them with `ALTER TABLE … SET
  TBLPROPERTIES`). Both also failed on the numbers anyway (no-effect 0.0417; vetoed 1.2217).
- **DC-5 Control spellings are not matrix cells.** A spelled value identical to the engine
  default is a control, not a change (step-2 Method "Control spellings"): `true` for
  `repartition_joins` and the default-`true` booleans, `zstd(3)` for `compression`. Matrix
  cells per knob = `@default` plus the step-2 non-control spellings — the proven-bounded set.
- **DC-6 The win rule is the sweep's own derivation.** A value wins only when a reachable
  cell measures ≥ 5 % better than `@default` **and** beyond that cell's noise floor, and it
  keeps the place only while no reachable sibling pays a ≥ 5 % regression outside its floor —
  the rule that produced the guide's exact partition (3 read-profile knobs, 5 no-effect, 10
  kept at default). Nothing may be declared a winner by a weaker rule.
- **DC-7 Measurement discipline is the sweep's.** Three repetitions, median (repetition 1 is
  cold); a quiet-box guard before every timed repetition; a release build; one session per
  value, stopped after. Every cited number was measured under exactly these conditions
  (step-2 Method); an autotune measurement taken under weaker ones is not comparable to them.
- **DC-8 The matrix budget is measured, not guessed.** From §0's cost basis: ~30 s of timed
  cells per full-bed value pass, ~88 s per three-value knob. The runner's budget model prices
  its plan from these per-cell times before it starts; the cap value itself is DQ-7's.
- **DC-9 The write half of the matrix exists only on a writable Iceberg bed.** Every measured
  write cell ran on the memory-catalog Iceberg v2 table (the CSVs' `iceberg` dataset rows —
  append 8 files, `INSERT OVERWRITE` one partition, `MERGE` 10 % updates). A dataset that is
  not an Iceberg table has no measured write workload; autotune on such a dataset measures
  the read half only and can write back only a `read` profile — on this box's evidence that
  is also all it could write back, since no write knob qualified (guide "write" section).

## 2. Owner questions (the numbers do not decide these)

- **DQ-1 Where the winners are written.** *Premise:* the epic reads "writes the winning
  `read` and `write` profiles back into the file". The file is hand-maintained (the guide's
  example carries comments); a TOML round-trip loses comments and ordering unless the writer
  is surgical, and discovery today is one file (`$REPARK_CONFIG` → `./repark.toml` →
  `~/.config/repark/repark.toml`, `config_file/discovery.rs`). *Lean:* a sibling generated
  file the discovery chain reads after the user's own (never rewriting a hand-edited file);
  in-place only if the owner rules it, and then restricted to the profile tables it owns.
- **DQ-2 The public key names under `[<profile>.autotune]`.** *Premise:* the epic sketches
  `enabled = true`, `dataset = …`; undecided are the bound keys (sample size, budget), the
  write-back selector, a knob allowlist, and what `dataset` names (a `cat.db.t` identifier,
  a filesystem path, or either). The `[<profile>.maintenance]` precedent is a typed
  allowlist: every key optional, unknown keys refuse naming the key path. *Lean:* minimal
  surface — `enabled`, `dataset`, `sample_rows` or `sample_files`, `budget_seconds`,
  `write_back` — with maintenance-style refusals; `dataset` accepts either spelling and one
  entry runs one dataset.
- **DQ-3 May autotune run writes — and against what?** *Premise:* the write cells are
  destructive shapes (append 8 files, `INSERT OVERWRITE` a partition, `MERGE` 10 % updates)
  and the harness rebuilt a fresh bed table before every timed repetition; nothing measured
  autotune writes against a user's own table. *Lean:* writes allowed only against a
  session-local sampled copy the runner builds (the harness's own pattern); never against the
  named dataset's files.
- **DQ-4 The sample definition.** *Premise:* every win was measured at full scale — the
  20 MB futures file read in place, TPC-H SF10, a 200-file table — and the three winners are
  plausibly scale-dependent (their decisive cells are the big tpch shapes); the measurements
  do not bound a sample. *Lean:* cap by files/rows rather than time, keeping the sample in
  the measured regime (a `target_partitions` trial needs enough files for partitioning to
  act); the budget cap of DQ-8 bounds the tail.
- **DQ-5 Do the ten regression-only knobs re-enter the matrix?** *Premise:* every non-default
  spelling of theirs measured flat-to-worse on every reachable cell of this bed
  (`prefer_hash_join` 10.2×, `repartition_aggregations` 4.2×, `repartition_file_scans` 6.7×,
  `pushdown_filters` 1.75×, `enable_page_index` 1.08×, `bloom_filter_on_read` 1.22×,
  `compression/snappy` 1.087×, `write.distribution-mode/none` 1.22×, `file_scoped_rewrite`
  1.85×, `scan_pruning` 1.46×); nothing was measured on a different dataset. *Lean:* excluded
  — the matrix stays DC-1's three winners; an opt-in knob list is a later feature if the
  owner wants exploration.
- **DQ-6 Live session, or file only?** *Premise:* the epic's text — "starts a session on
  defaults, runs a bounded knob matrix … and writes the winning profiles back" — reads as
  this session staying on defaults while the file gains the winners; applying the winners to
  the live session is a second, riskier behaviour nothing measured. *Lean:* file only; the
  live session is untouched by its own measurement.
- **DQ-7 Overrun and error behaviour.** *Premise:* the matrix is minutes-scale even on the
  full bed (DC-8); on an unbounded dataset or a busy box it can overrun, and a mid-matrix
  failure is a normal event. *Lean:* a hard wall-clock cap beside the sample cap; any overrun
  or cell error ends autotune cleanly — the session continues on defaults, nothing is
  written back, one warning is emitted (partial results are never committed).

## 3. Cards

Measure-first: the first card re-establishes the sweep as a reproducible, budgeted harness —
no product behaviour. Later cards cut the feature behind the DQ rulings.

---

### Card DYNCFG-1-BED — the autotune matrix as a reproducible, budgeted harness

**Why.** Every number this unit acts on came out of
`python/repark-parity/bench/profiles/run_profiles.py`; today it sweeps one knob per
invocation, at hand-chosen values, on the full bed. Autotune needs the same measurement as
one bounded, sampled, self-describing run: the matrix spelled out as data, the dataset
builders capped, and a winners report computed by the same rule the guide is pinned to —
otherwise the runner card has no oracle to agree with.

**Home.** `python/repark-parity/bench/profiles/run_profiles.py` (`--matrix <file>`,
`--sample` and `--budget-seconds` modes), `datasets.py` (sampled variants of the three
builders), `harness.py` (budget accounting beside `measure`), `queries.py` (unchanged cell
roster), `matrix.py` (NEW — the matrix-spec reader and the winners report reusing the
derivation rule of `measured_winners` in `test_profiles_bed.py`), the matrix spec
`autotune_matrix.toml` (NEW beside the bed — DC-1's cells exactly),
`python/repark-parity/tests/test_profiles_matrix.py` (NEW),
`docs/perf/dyncfg-1-matrix-2026-XX/` (NEW directory + `map.md` — step 2's committed CSV and
winners report), maps.

**Decisions.**

- **D-1** The matrix spec is a data file listing knob → values exactly as DC-1/DC-5 rule
  (three knobs, `@default` plus their swept non-control spellings); no knob names are
  hard-coded in the runner beyond the spec reader.
- **D-2** The winners report applies DC-6's rule to the run's own CSV — same noise floors
  recomputed from the run, same sibling veto; nothing is declared a win by hand.
- **D-3** Sample mode caps rows/files per dataset builder through a CLI argument; the
  default value waits for DQ-4.
- **D-4** `--budget-seconds` refuses up front when the plan's projected cost (cells × the
  measured per-cell medians, DC-8) exceeds it; a mid-run overrun stops at the next cell
  boundary and reports partial results marked partial.

**Pins (red-first).** `test_matrix_spec_loads_dc1_cells`, `test_matrix_run_writes_csv_and_winners`,
`test_sampled_bed_respects_file_cap`, `test_budget_refuses_overrun_projection`,
`test_winners_rule_matches_step3_derivation`.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | The spec file, the `--matrix`/`--sample`/`--budget-seconds` modes, the sampled builders, the winners report; pins red first. |
| 2 | M | One quiet-box run of the spec on the sampled bed (box alone, release build — the S2-11 window rule); commit the CSV and winners report under the new `docs/perf/` directory. This run is the oracle the engine runner is compared against. |

**Done when.** One command reproduces the committed matrix CSV and winners report; the
winners the report derives on the sampled bed match the guide's `read` profile or the
differences are recorded as measurements.

**Hand back when.** The sample caps cannot be expressed in the existing dataset builders
without redesign; the derivation rule cannot be reused from `test_profiles_bed.py` without
copying (then it moves into a shared module under `bench/profiles/`).

**Rounds.** 2 (M, M). Step 2 needs the box alone.

---

### Card DYNCFG-1-TOML — `[<profile>.autotune]` parses and validates

**Why.** The feature's public surface is a new profile table, and the loader already carries
the exact precedent: `[<profile>.maintenance]` is a typed allowlist parsed by
`config_file/maintenance.rs` — every key optional, unknown keys refuse naming the
`name.maintenance.key` path, the `[default]` merge keeps it (`config_file/map.md`). Parsing
first, with the refusals pinned, puts the public shape under review before any runner
exists.

**Home.** `crates/repark-core/src/config_file.rs` (`Profile` gains `autotune` beside
`maintenance`), `crates/repark-core/src/config_file/autotune.rs` (NEW — `AutotuneSpec` +
`from_table`), `config_file/profile.rs` (the slot carried through `effective_table` merge),
`config_file/wiring.rs` (the stamp path by which the parsed spec reaches session build — the
maintenance precedent), `config_file/tests/` (the pins), `config_file/map.md`.

**Decisions.**

- **D-1** The table is typed and allowlisted exactly like `maintenance`: every key optional,
  unknown keys refuse with the `name.autotune.key` path. The key set is DQ-2's — the card
  parses `enabled` and `dataset` plus whatever DQ-2 rules.
- **D-2** A profile with no `autotune` table means "no autotune" (the maintenance reading);
  `enabled = false` parses and does nothing.
- **D-3** Red first: the refusal pins (unknown key, wrong type, bad duration/count shape)
  land before the accept pins.

**Pins (red-first).** `test_autotune_table_parses_enabled_and_dataset`,
`test_autotune_unknown_key_refuses_with_path`, `test_autotune_merges_from_default`,
`test_autotune_absent_means_disabled`.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I | The `autotune.rs` parser, the `Profile` slot, the merge and the wiring stamp; pins red first; `cargo test -p repark-core config_file` green. |

**Done when.** The ruled key set loads through `parse()` and the typed API with
maintenance-identical refusal shapes, and `REPARK_ENV` merge keeps the slot.

**Hand back when.** DQ-2 rules a key whose TOML type the maintenance-shaped parser cannot
express, or the stamp needs a session seam that does not exist.

**Rounds.** 1 (I). **Waits for DQ-2.**

---

### Card DYNCFG-1-RUNNER — the bounded matrix run at session start

**Why.** The epic's behaviour: `enabled = true` → the session starts on defaults, samples the
named dataset, runs the bounded matrix, derives winners. The runner is engine code — the
Python harness of DYNCFG-1-BED is the oracle its results must agree with on the same beds,
not the implementation.

**Home.** A new autotune module under `crates/repark-core/src/` (NEW — the matrix runner:
per-cell session conf, the bed's query shapes, median-of-three timing, the DC-6 derivation,
the DC-8 budget check), the session-build seam where the TOML card's stamp lands
(`config_file/wiring.rs` / `session.rs`), the sampler (NEW — DQ-4's cap over the named
dataset), pins, maps. **Waits for DQ-3, DQ-4, DQ-5, DQ-6, DQ-7.**

**Decisions.**

- **D-1** Matrix membership and cells are DC-1's and DC-5's; DQ-5 decides whether anything
  else can enter.
- **D-2** The write half exists only on a writable Iceberg sample (DC-9) and only under
  DQ-3's answer; on a non-Iceberg dataset the runner measures the read cells only.
- **D-3** The live session runs defaults; DQ-6 decides whether the winners also apply to it.
- **D-4** Budget and abort follow DQ-7: projected cost checked before the first cell,
  overrun aborts at a cell boundary, nothing partial is written back.

**Pins (red-first).** `test_autotune_disabled_is_noop`, `test_autotune_matrix_cells_equal_spec`,
`test_autotune_restores_conf_between_values`, `test_autotune_budget_aborts_clean`,
`test_autotune_winners_apply_derivation_rule`, `test_autotune_write_cells_need_iceberg_sample`.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I | The runner core on the read cells: sample, per-value conf, timing, derivation; pins red first; agreement with the BED card's committed winners report on the same bed is the pin. |
| 2 | I | The write cells on a session-local sampled Iceberg copy (per DQ-3), the budget/abort path (DQ-7). |

**Done when.** On the harness's own beds the runner derives the same winners as the committed
BED report; an overrun leaves the session on defaults with nothing written.

**Hand back when.** Per-value session conf cannot be applied and restored without leaking
into the user session; sampling the named dataset needs a reader path that does not exist;
the runner cannot reproduce the BED oracle's winners on the same bed (that is a measurement
finding, not a bug to absorb).

**Rounds.** 2 (I, I).

---

### Card DYNCFG-1-WRITEBACK — the winning profiles written back

**Why.** The epic's last mile: the derived winners become profile content. The emitted
spelling is already committed in `docs/examples/config/read.toml` — `[read.session]` carries
`batch_size`/`target_partitions` (the `repark.*` knobs) and `[read.conf]` carries the
`datafusion.*` keys — and `write.toml` shows an empty profile is the honest record when
nothing qualifies. Where the content lands is DQ-1's.

**Home.** A writer module beside the loader (NEW under
`crates/repark-core/src/config_file/` — the DQ-1 target: surgical in-place edit or sibling
file), the runner's winners output (DYNCFG-1-RUNNER), pins, maps. **Waits for DQ-1, DQ-2,
DQ-6.**

**Decisions.**

- **D-1** The emitted spelling equals the guide's committed shape: `repark.*` knobs under
  `[<name>.session]`, `datafusion.*` keys under `[<name>.conf]` (the `read.toml` example).
- **D-2** No qualifying win → no write; an autotune run that finds nothing writes nothing
  (the empty-`write` precedent), and an aborted run writes nothing (DQ-7).
- **D-3** If DQ-1 rules in-place, the writer touches only the profile tables it owns — the
  rest of the file, comments and ordering included, is byte-preserved.

**Pins (red-first).** `test_writeback_emits_guide_spelling`, `test_writeback_no_win_no_edit`,
`test_writeback_preserves_unrelated_tables`, `test_writeback_abort_writes_nothing`.

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | I | The writer on the runner's winners output; pins red first; a round-trip load of the written file reproduces the profile the guide documents. |

**Done when.** A file written by the runner loads through the CFG-1 loader and yields exactly
the derived profile; a no-win or aborted run leaves the target untouched.

**Hand back when.** DQ-1 picks in-place and the loader's TOML handling cannot do a
comment-preserving surgical edit (then the card's design changes, not just its keys).

**Rounds.** 1 (I).

---

### Card DYNCFG-1-DOCS — the autotune guide section and example

**Why.** A public `[<profile>.autotune]` table ships documented or not at all; the guide's
"The tables" section enumerates every profile table (six today — autotune makes seven), and
the measured-profiles section is where the write-back result is explained.

**Home.** `docs/guide/repark-toml.md` (the tables section + the autotune behaviour:
measurement, budget, write-back, failure semantics), `docs/guide/session-and-conf.md` if its
conf semantics section needs the interaction note, `docs/examples/config/` (NEW example
file), the DYNCFG-1-BED perf directory (a pointer), maps. **Waits for DQ-2** and lands after
DYNCFG-1-TOML so the documented names are the parsed ones.

**Decisions.**

- **D-1** The documented key names are DQ-2's ruling verbatim; the example file loads through
  the loader (the `test_toml_session_table_sets_builder_knobs` precedent for example pins).

**Steps.**

| Step | Tier | Do |
|---|---|---|
| 1 | M | The guide section, the example file, the perf pointer, maps; the example-loads pin. |

**Done when.** The documented shape equals the merged parser's shape and the example loads.

**Hand back when.** The documented shape diverges from the merged parser (a docs/impl skew —
hand back, do not paper over).

**Rounds.** 1 (M).

## 4. Sequence

| # | Card | Depends on | Tiers | Rounds |
|---|---|---|---|---|
| 1 | DYNCFG-1-BED | — (step 2 needs the box alone, S2-11) | M | 2 |
| 2 | DYNCFG-1-TOML | DQ-2 | I | 1 |
| 3 | DYNCFG-1-RUNNER | DYNCFG-1-BED; DQ-3, DQ-4, DQ-5, DQ-6, DQ-7 | I | 2 |
| 4 | DYNCFG-1-WRITEBACK | DYNCFG-1-RUNNER; DQ-1, DQ-2, DQ-6 | I | 1 |
| 5 | DYNCFG-1-DOCS | DQ-2; DYNCFG-1-TOML merged | M | 1 |

The BED card can start today — it decides nothing the questions gate. The four later cards
are ordered so every public shape is ruled (DQ-2) before it is parsed, measured (BED) before
it is re-implemented (RUNNER), and landed (TOML, RUNNER) before it is written back and
documented.

## Pointers

- Up: [map.md](map.md)
- The epic: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) "### DYNCFG-1" ·
  the format and tiers: [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)
- The measurements: [docs/perf/config-profiles-2026-09-12.md](../../../docs/perf/config-profiles-2026-09-12.md)
  and [its CSVs](../../../docs/perf/config-profiles-2026-09-12/map.md) · the shipped profiles:
  [docs/guide/repark-toml.md](../../../docs/guide/repark-toml.md) · the probe:
  [docs/perf/profiles-1-passthrough-probe-2026-09-09.md](../../../docs/perf/profiles-1-passthrough-probe-2026-09-09.md)
- The harness: [python/repark-parity/bench/profiles/map.md](../../../python/repark-parity/bench/profiles/map.md) ·
  the loader it extends: [crates/repark-core/src/config_file/map.md](../../../crates/repark-core/src/config_file/map.md)
