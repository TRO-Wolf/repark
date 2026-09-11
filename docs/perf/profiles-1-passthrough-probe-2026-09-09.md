# PROFILES-1 step 0 — the `.config()` pass-through probe (2026-09-09)

Step 0 record for card PROFILES-1 (`brief-0.md` in the orchestrator's lane): does a
`datafusion.*` or `repark.*` key set through `.config()` actually reach the engine, and can
it be read back? This document is the round's product. No knob is chosen here, no profile
is written, no engine code changed. The unit's ledger is born in step 1 with the bed
script; this file is step 0's record alone.

**REVIEW-FIX-8 (2026-09-11):** the probe is re-runnable (a unique temporary
directory per run, `REPARK_CONFIG=""` in every session, the work prefix scrubbed
to `<work>` so two outputs agree byte for byte), the table gains the `VALIDATED`
state, the three `repark.*` rows move into it, and the two `write.*` rows are
re-described as table properties with this round's measurements. Counts are
re-derived in §5. The four remaining `ACCEPTED BUT UNREAD` rows are
CONF-UNREAD-1's; the next round owns them.

**CONF-UNREAD-1 (2026-09-11):** the four `ACCEPTED BUT UNREAD` rows are
resolved. Both tables gain an `after CONF-UNREAD-1` column carrying each key's
new state and one line of the unit ledger's evidence: `enable_page_index`,
`bloom_filter_on_read` and `write_batch_size` are pinned reaching the engine
structures that read them (`PASSES THROUGH`); `coalesce_batches` is `REFUSED`
loud at build and at runtime `conf.set` — DataFusion 54.1.0 defines the option
but no engine path reads it. §5's counts are re-derived again. Ledger:
[../../task/ledgers/completed/conf-unread-1-ledger.md](../../task/ledgers/completed/conf-unread-1-ledger.md).

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

Global results after the CONF-UNREAD-1 re-run: `builder.config(key, value)`
accepted nineteen keys and refused `datafusion.execution.coalesce_batches` (the
D-1 refuse branch — its §2 row quotes the message); `session.conf.set(key,
value)` at runtime agrees, accepting the same nineteen and refusing
`coalesce_batches`. `conf.get` and `conf.getAll` agreed on every accepted key.
The remaining refusals in the capture are the deliberate validation probes (§4).

Verdict meanings (D-7, plus REVIEW-FIX-8 D-2): `PASSES THROUGH` (set, readable,
engine evidence observed), `VALIDATED` (set, readable, and an invalid value
refuses loud at `getOrCreate` — the key is parsed at session build, so "accepted
but unread" overstates nothing and understates the check), `ACCEPTED BUT UNREAD`
(set and readable, no engine-visible signal on the probed subjects), `REFUSED`
(with the message). A key the probe never reached would read `NOT MEASURED`;
every D-1 key was reached, so that verdict does not occur. The two `write.*`
rows are table properties: their engine evidence is observed at the table
(measured in REVIEW-FIX-8, cited per row), not in the probe's plan subjects.

## 2. Read keys (twelve)

| key (set value) | set | read back | reaches the engine | verdict | after CONF-UNREAD-1 |
|---|---|---|---|---|---|
| `datafusion.optimizer.prefer_hash_join` (`false`) | accepted | `false` | join plan: `HashJoinExec: mode=Partitioned` becomes `SortMergeJoinExec: join_type=Inner, on=[(k@0, k@0)]` | PASSES THROUGH | — |
| `datafusion.execution.target_partitions` (`1`) | accepted | `1` | join plan: `HashJoinExec: mode=CollectLeft`, both `RepartitionExec` lines gone; scan plan loses its `RepartitionExec: partitioning=RoundRobinBatch(64)` | PASSES THROUGH | — |
| `datafusion.execution.batch_size` (`3`) | accepted | `3` | not plan-visible; `range(10)` yields 4 batches against a 1-batch baseline (runtime `conf.set` to `5` yields 2) | PASSES THROUGH | — |
| `datafusion.optimizer.repartition_joins` (`false`) | accepted | `false` | join plan: `HashJoinExec: mode=CollectLeft`; left `RepartitionExec: partitioning=Hash([k@0], 64)` gone, right side keeps `RepartitionExec: partitioning=RoundRobinBatch(64)` | PASSES THROUGH | — |
| `datafusion.optimizer.repartition_aggregations` (`false`) | accepted | `false` | agg plan: `AggregateExec: mode=FinalPartitioned` + `RepartitionExec: partitioning=Hash([g@0], 64)` becomes `AggregateExec: mode=Final` over `CoalescePartitionsExec` | PASSES THROUGH | — |
| `datafusion.optimizer.repartition_file_scans` (`false`) | accepted | `false` | single-file subject: byte-identical to baseline; 8-file subject: `DataSourceExec: file_groups={64 groups: [[...part_0.parquet:0..352905], ...]}` becomes `RepartitionExec: partitioning=RoundRobinBatch(64), input_partitions=8` over `file_groups={8 groups: [[...part_0.parquet], ...]}` | PASSES THROUGH | — |
| `datafusion.execution.parquet.pushdown_filters` (`true`) | accepted | `true` | scan plan loses the `FilterExec: v@2 > 150000` wrapper: bare `DataSourceExec: ... predicate=v@2 > 150000, pruning_predicate=...` (probe 2: `false` on the 8-file bed leaves all three plans byte-identical) | PASSES THROUGH | — |
| `datafusion.execution.parquet.enable_page_index` (`false`) | accepted | `false` | not plan-visible: filtered single-file scan plan byte-identical to baseline | ACCEPTED BUT UNREAD | PASSES THROUGH — `false` reaches `SessionConfig` and the session table options the scan source gates page pruning on (ledger C-001); still plan-invisible on the probe's index-less files |
| `datafusion.execution.parquet.bloom_filter_on_read` (`false`) | accepted | `false` | not plan-visible: filtered single-file scan plan byte-identical to baseline | ACCEPTED BUT UNREAD | PASSES THROUGH — `false` reaches both structures the source reads into `enable_bloom_filter` (ledger C-002); plan-invisible on the probe's bloom-filter-less files |
| `datafusion.execution.coalesce_batches` (`false`) | accepted | `false` | not plan-visible: agg and join plans byte-identical to baseline; probe-2 8-file plans byte-identical too | ACCEPTED BUT UNREAD | REFUSED — build `.config()` and runtime `conf.set` fail naming the key: `unsupported DataFusion session config 'datafusion.execution.coalesce_batches' = 'false': DataFusion 54.1.0 defines the option but no engine path reads it, so the value cannot take effect` (ledger C-003) |
| `repark.scan.concurrency_limit` (`4`) | accepted | `4` | parsed at session build (`0` and `abc` refuse, §4); the value bounds MERGE target-scan file concurrency (`session.rs:235`, `target_scan.rs:99`); no plan-visible signal on the probed subjects | VALIDATED | — |
| `repark.batch.size` (`3`) | accepted | `3` | not plan-visible; `range(10)` yields 4 batches against a 1-batch baseline | PASSES THROUGH | — |

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

| key (set value) | set | read back | reaches the engine | verdict | after CONF-UNREAD-1 |
|---|---|---|---|---|---|
| `datafusion.execution.parquet.compression` (`uncompressed`) | accepted | `uncompressed` | written file: codecs `[UNCOMPRESSED]`, `bytes` 2302311 against 575742 | PASSES THROUGH | — |
| `datafusion.execution.parquet.max_row_group_size` (`1000`) | accepted | `1000` | written file: `row_groups` 66 against 1, `bytes` 485195 | PASSES THROUGH | — |
| `datafusion.execution.parquet.bloom_filter_on_write` (`true`) | accepted | `true` | written file: `bloom_filter_lengths` `[65553, 47, 65553, 65553]` against `[null, null, null, null]` | PASSES THROUGH | — |
| `datafusion.execution.parquet.write_batch_size` (`1000`) | accepted | `1000` | no subject measured for this key; the probe gave it an empty subject list | ACCEPTED BUT UNREAD | PASSES THROUGH — `1000` reaches the writer options and moves the written file: the new `["write"]` subject lands first-part-file bytes 575692 against the 575742 baseline with `part_files` and `row_groups` equal (ledger C-004) |
| `write.target-file-size-bytes` (`134217728`) | accepted | `134217728` | an Iceberg table property: the session stores any value (even `abc`), the write path reads the table's own property at commit (`append.rs:272`); measured this round — with `target_partitions` pinned to 16, a MERGE rewrite with target `1` lands 16 new data files against 4 on default (20k rows, seed files excluded), pinned by `test_profiles1_table_properties.py` (ledger C-004) | PASSES THROUGH | — |
| `write.distribution-mode` (`hash`) | accepted | `hash` | an Iceberg table property: the session stores any value (even `bogus`), RePark-owned writes read the table's property (`distribution.rs:169`); measured this round — `bogus` refuses loud at partitioned CTAS with more than one writer (`write.distribution-mode 'bogus' is not supported`, ledger C-004; the pin fixes `target_partitions` to 16 so the property is always consulted) | PASSES THROUGH | — |
| `repark.merge.file_scoped_rewrite` (`true`) | accepted | `true` | parsed at session build (`maybe` refuses, §4); gates the copy-on-write file-scoped rewrite path (`merge/mod.rs:226,622`); no MERGE ran in the probe | VALIDATED | — |
| `repark.merge.scan_pruning` (`true`) | accepted | `true` | parsed at session build (`maybe` refuses, §4); gates MERGE scan pruning (`merge/mod.rs:219`) and the predicate-DML residual probe (`residual.rs:168`); no MERGE ran in the probe | VALIDATED | — |

## 4. Refusal shape (validation probes, all refused as designed)

One D-1 key's probed value is refused since CONF-UNREAD-1 —
`datafusion.execution.coalesce_batches` fails at `builder.config` and at runtime
`conf.set` (its §2 row quotes the message). Before that round, no D-1 key with
its probed value was refused. The probe's deliberate bad values show the other
refusal shapes, loud and exact:

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

REVIEW-FIX-8 measured the other side of the `write.*` rows: the session
validates nothing there — `builder.config("write.distribution-mode", "bogus")`
and `builder.config("write.target-file-size-bytes", "abc")` are both accepted
and read back. Refusal for those keys happens at the table, per the §3 rows.

## 5. Answer, counts, and reproduce

Answer to the card's gating question: yes, with one loud exception. Every
`datafusion.*` and `repark.*` key in the D-1 inventory that the engine can honor
is accepted through `.config()`, reads back through `conf.get` and the dump, and
accepts a runtime `conf.set`; `datafusion.execution.coalesce_batches` is refused
at both doors instead of being stored unread. Where the engine has an
observable signal it responds: join, agg and scan plans change for six read
keys, batch counts move for the two batch-size keys, written parquet files
change codec, row-group count, bloom filters and file bytes for four write
keys, the three `repark.*` session keys refuse invalid values at build, and the
two `write.*` table properties apply at the table (or refuse loud there).
Counts after CONF-UNREAD-1: **PASSES THROUGH 16, VALIDATED 3, ACCEPTED BUT
UNREAD 0, REFUSED 1, NOT MEASURED 0.** The last unread rows are closed: the
page-index, read-bloom and write-batch keys are pinned reaching the options the
scan source and writer read, and `coalesce_batches` refuses because DataFusion
54.1.0 defines the option but no engine path reads it (the `after CONF-UNREAD-1`
column carries each row's one-line evidence). No row rests on an empty subject
any more (Q-41's mapping is gone): every row carries a probe subject, a
build-time refusal, or a table measurement. The card's premise holds; no
separate pass-through step is needed before measurement.

Reproduce (the two scripts are the method; each builds its subjects in a unique
temporary directory per run, so no fixture setup precedes them and nothing lands
under tracked `docs/perf/`):

- `.venv/bin/python docs/perf/profiles-1-probe/profiles1_probe.py > probe_out.json`
  produces §2–§4's main table (the twenty `keys` entries against `baseline`), the
  `validation` refusals, and the `runtime` `conf.set` probes.
- `.venv/bin/python docs/perf/profiles-1-probe/profiles1_probe2.py > probe2_out.txt`
  produces the five-case multi-file follow-up that the `repartition_file_scans`,
  `pushdown_filters=false` and `coalesce_batches` rows rest on.

Each run sets `REPARK_CONFIG=""` before opening any session, so a developer's
own `repark.toml` cannot register catalogs into a measurement (Q-56), and
scrubs its work prefix to `<work>` in every plan, so two runs in a row exit 0
with byte-identical outputs — the committed pin
(`python/repark/tests/test_profiles1_probe_rerun.py`) asserts exactly that,
plus immunity to a poisoned discovered file. Run data stays under the system
temporary root for the operator to delete. The scripts differ from the measuring
worker's originals in the work directory (once script-relative `data/`, now the
temporary directory) and the two guards above. Environment provisioning was not
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
