# PROFILES-1 step 0 — the `.config()` pass-through probe (2026-09-09)

Step 0 record for card PROFILES-1 (`brief-0.md` in the orchestrator's lane): does a
`datafusion.*` or `repark.*` key set through `.config()` actually reach the engine, and can
it be read back? This document is the round's product. No knob is chosen here, no profile
is written, no engine code changed. The unit's ledger is born in step 1 with the bed
script; this file is step 0's record alone.

## 1. Method

Two probe scripts, both run against a debug native provisioned the way `make
py-test-facade` does (D-8; a pass-through probe measures no time, so a debug module is
enough). Subjects are small generated parquet frames, never the owner's datasets, never a
JVM or Spark. Both scripts are kept beside this document in
[profiles-1-probe/](profiles-1-probe/map.md) and are the reproduce method (§5).

- [profiles1_probe.py](profiles-1-probe/profiles1_probe.py) builds two 200,000-row parquet
  files, opens one session per key with `builder.config(key, value)`, records
  `session.conf.get(key)` and `session.conf.getAll[key]`, re-sets the key at runtime with
  `session.conf.set`, and captures `explain(mode="simple")` plans for join / agg / scan
  subjects plus `range(10)` batch counts and written-file facts. Output:
  `probe_out.json` (20 keys, baseline, 9 validation probes, 3 runtime probes).
- [profiles1_probe2.py](profiles-1-probe/profiles1_probe2.py) is the follow-up the first
  run's gaps demanded: an 8-file directory subject plus a small single-file subject, with
  `pushdown_filters=false`, `repartition_file_scans=false`, `coalesce_batches=false`
  (alone and with `target_partitions=1`) against a same-process baseline. Output:
  `probe2_out.txt`. Both runs' stderr files were empty.

Global results, true of all twenty keys: `builder.config(key, value)` accepted every
valid value with no refusal; `session.conf.set(key, value)` at runtime accepted every
key as well; `conf.get` and `conf.getAll` agreed on all twenty. The only refusals in the
capture are the deliberate validation probes (§4).

Verdict meanings (D-7): `PASSES THROUGH` (set, readable, engine evidence observed),
`ACCEPTED BUT UNREAD` (set and readable, no engine-visible signal on the probed
subjects), `REFUSED` (with the message). A key the probe never reached would read
`NOT MEASURED`; every D-1 key was reached, so that verdict does not occur.

## 2. Read keys (twelve)

| key (set value) | set | read back | reaches the engine | verdict |
|---|---|---|---|---|
| `datafusion.optimizer.prefer_hash_join` (`false`) | accepted | `false` | join plan: `HashJoinExec: mode=Partitioned` becomes `SortMergeJoinExec: join_type=Inner, on=[(k@0, k@0)]` | PASSES THROUGH |
| `datafusion.execution.target_partitions` (`1`) | accepted | `1` | join plan: `HashJoinExec: mode=CollectLeft`, both `RepartitionExec` lines gone; scan plan loses its `RepartitionExec: partitioning=RoundRobinBatch(64)` | PASSES THROUGH |
| `datafusion.execution.batch_size` (`3`) | accepted | `3` | not plan-visible; `range(10)` yields 4 batches against a 1-batch baseline (runtime `conf.set` to `5` yields 2) | PASSES THROUGH |
| `datafusion.optimizer.repartition_joins` (`false`) | accepted | `false` | join plan: `HashJoinExec: mode=CollectLeft`; left `RepartitionExec: partitioning=Hash([k@0], 64)` gone, right side keeps `RepartitionExec: partitioning=RoundRobinBatch(64)` | PASSES THROUGH |
| `datafusion.optimizer.repartition_aggregations` (`false`) | accepted | `false` | agg plan: `AggregateExec: mode=FinalPartitioned` + `RepartitionExec: partitioning=Hash([g@0], 64)` becomes `AggregateExec: mode=Final` over `CoalescePartitionsExec` | PASSES THROUGH |
| `datafusion.optimizer.repartition_file_scans` (`false`) | accepted | `false` | single-file subject: byte-identical to baseline; 8-file subject: `DataSourceExec: file_groups={64 groups: [[...part_0.parquet:0..352905], ...]}` becomes `RepartitionExec: partitioning=RoundRobinBatch(64), input_partitions=8` over `file_groups={8 groups: [[...part_0.parquet], ...]}` | PASSES THROUGH |
| `datafusion.execution.parquet.pushdown_filters` (`true`) | accepted | `true` | scan plan loses the `FilterExec: v@2 > 150000` wrapper: bare `DataSourceExec: ... predicate=v@2 > 150000, pruning_predicate=...` (probe 2: `false` on the 8-file bed leaves all three plans byte-identical) | PASSES THROUGH |
| `datafusion.execution.parquet.enable_page_index` (`false`) | accepted | `false` | not plan-visible: filtered single-file scan plan byte-identical to baseline | ACCEPTED BUT UNREAD |
| `datafusion.execution.parquet.bloom_filter_on_read` (`false`) | accepted | `false` | not plan-visible: filtered single-file scan plan byte-identical to baseline | ACCEPTED BUT UNREAD |
| `datafusion.execution.coalesce_batches` (`false`) | accepted | `false` | not plan-visible: agg and join plans byte-identical to baseline; probe-2 8-file plans byte-identical too | ACCEPTED BUT UNREAD |
| `repark.scan.concurrency_limit` (`4`) | accepted | `4` | no subject measured for this key; nothing plan-visible was checked | ACCEPTED BUT UNREAD |
| `repark.batch.size` (`3`) | accepted | `3` | not plan-visible; `range(10)` yields 4 batches against a 1-batch baseline | PASSES THROUGH |

Quoted plan baselines these rows compare against (`probe_out.json`, `baseline`):

- join: `HashJoinExec: mode=Partitioned, join_type=Inner, on=[(k@0, k@0)], projection=[lv@1, rw@3]` with `RepartitionExec: partitioning=Hash([k@0], 64), input_partitions=1` on both inputs.
- agg: `AggregateExec: mode=FinalPartitioned, gby=[g@0 as g]` over `RepartitionExec: partitioning=Hash([g@0], 64), input_partitions=64` over a partial aggregate over `RepartitionExec: partitioning=RoundRobinBatch(64), input_partitions=1`.
- scan: `FilterExec: v@2 > 150000` over `RepartitionExec: partitioning=RoundRobinBatch(64), input_partitions=1` over `DataSourceExec` with `predicate=v@2 > 150000`.
- `range_batches`: 1.

## 3. Write keys (eight)

File facts compare against the default-write baseline (`probe_out.json`, `baseline.write`):
codecs `[ZSTD]`, `part_files` 4, `row_groups` 1, `bytes` 575742, `bloom_filter_lengths`
`[null, null, null, null]`. Row and byte counts describe the first part file, the only
file the probe's fact collector opens.

| key (set value) | set | read back | reaches the engine | verdict |
|---|---|---|---|---|
| `datafusion.execution.parquet.compression` (`uncompressed`) | accepted | `uncompressed` | written file: codecs `[UNCOMPRESSED]`, `bytes` 2302311 against 575742 | PASSES THROUGH |
| `datafusion.execution.parquet.max_row_group_size` (`1000`) | accepted | `1000` | written file: `row_groups` 66 against 1, `bytes` 485195 | PASSES THROUGH |
| `datafusion.execution.parquet.bloom_filter_on_write` (`true`) | accepted | `true` | written file: `bloom_filter_lengths` `[65553, 47, 65553, 65553]` against `[null, null, null, null]` | PASSES THROUGH |
| `datafusion.execution.parquet.write_batch_size` (`1000`) | accepted | `1000` | no subject measured for this key; the probe gave it an empty subject list | ACCEPTED BUT UNREAD |
| `write.target-file-size-bytes` (`134217728`) | accepted | `134217728` | no subject measured; the Iceberg write path was not exercised | ACCEPTED BUT UNREAD |
| `write.distribution-mode` (`hash`) | accepted | `hash` | no subject measured; the Iceberg write path was not exercised | ACCEPTED BUT UNREAD |
| `repark.merge.file_scoped_rewrite` (`true`) | accepted | `true` | no subject measured; no MERGE ran in the probe | ACCEPTED BUT UNREAD |
| `repark.merge.scan_pruning` (`true`) | accepted | `true` | no subject measured; no MERGE ran in the probe | ACCEPTED BUT UNREAD |

## 4. Refusal shape (validation probes, all refused as designed)

No D-1 key with its probed value was refused. The probe's deliberate bad values show
refusals are loud and exact, which is what a `REFUSED` row would quote:

- `builder.config("datafusion.execution.batch_size", "notanumber")`:
  `repark config error: invalid DataFusion session config 'datafusion.execution.batch_size' = 'notanumber': Error parsing 'notanumber' as usize`
- `session.conf.set("datafusion.execution.parquet.not_a_real_key", "true")`:
  `[INVALID_CONF_VALUE.REQUIREMENT] The value 'true' in the config 'datafusion.execution.parquet.not_a_real_key' is invalid. datafusion engine error: Invalid or Unsupported Configuration: Config value "not_a_real_key" not found on ParquetOptions`
- `builder.config("repark.scan.concurrency_limit", "0")`:
  ``repark config error: Error during planning: config `repark.scan.concurrency-limit` must be >= 1 (got 0)``
- `builder.config("repark.merge.file_scoped_rewrite", "maybe")`:
  ``repark config error: Error during planning: config `repark.merge.file-scoped-rewrite` must be true or false (got "maybe")``
- `repark.scan.concurrency-limit` (dash spelling) validates identically to the underscore
  spelling; both `repark.merge` dash spellings validate identically too.

## 5. Answer, counts, and reproduce

Answer to the card's gating question: yes. Every `datafusion.*` and `repark.*` key in
the D-1 inventory is accepted through `.config()`, reads back through `conf.get` and the
dump, accepts a runtime `conf.set` as well, and where the engine has an observable
signal it responds: join, agg and scan plans change for six read keys, batch counts
move for the two batch-size keys, and written parquet files change codec, row-group
count and bloom filters for three write keys. Counts: **PASSES THROUGH 11, ACCEPTED BUT
UNREAD 9, REFUSED 0, NOT MEASURED 0.** The nine `ACCEPTED BUT UNREAD` rows are not
counter-evidence: five had no subject that could show them (page index, read bloom
filter and coalesce affect execution, not the plan text; the concurrency, write-batch,
file-size, distribution and merge keys had no exercised path in this probe) and are
step-1 sweep material, not premise threats. The card's premise holds; no separate
pass-through step is needed before measurement.

Reproduce (the two scripts are the method; each generates its own data directory, so no
fixture setup precedes them):

- `.venv/bin/python docs/perf/profiles-1-probe/profiles1_probe.py > probe_out.json`
  produces §2–§4's main table (the twenty `keys` entries against `baseline`), the
  `validation` refusals, and the `runtime` `conf.set` probes.
- `.venv/bin/python docs/perf/profiles-1-probe/profiles1_probe2.py > probe2_out.txt`
  produces the five-case multi-file follow-up that the `repartition_file_scans`,
  `pushdown_filters=false` and `coalesce_batches` rows rest on.

The copies differ from the measuring worker's originals in exactly one line each: the
`WORK` directory is script-relative (`Path(__file__).resolve().parent / "data"`), since
the originals named this clone's scratch path. Environment provisioning was not
recorded beyond "debug native as `make py-test-facade` provisions it"; the exact
provisioning command is an open question below.

## 6. Gates

- `make check-docs-links`
- `make check-docs-compaction`
- `make check-ledgers`

All three green in this round; exit codes are in the hand-back. `make preflight`,
`make verify`, `cargo` and `maturin` were deliberately not run: another lane builds on
this machine and the orchestrator runs `preflight` itself. Open questions for the
orchestrator: (a) the exact environment-provisioning command the measuring worker used
was never captured — worth one line in the step-1 bed header; (b) whether step 1's
sweep should add execution-level (non-plan) signals for `coalesce_batches`,
`enable_page_index` and `bloom_filter_on_read`, whose plans cannot show them.
