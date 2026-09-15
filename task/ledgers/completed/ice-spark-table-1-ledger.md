# Unit ledger — ICE-SPARK-TABLE-1 · RePark writes into a Spark-created table — step 1

**Date:** 2026-09-14 · **Branch:** `test/ice-spark-table-1` · **Base:** `629f3d5f`
(v1.4.1 release tip; the orchestrator rebased `5e7a2ed3` onto it as `d5708359`)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card ICE-SPARK-TABLE-1 (gap G-3, the Spark half of G-4; assessment row I4).
Every table RePark has ever written in its tests was created by RePark. Production
tables — and the shadow tables, per `docs/cutover/inventory.md` §8 ruling 6 — are
created by Spark, and the owner ruled writing into Spark-created tables a standing
requirement. At the C4 switch RePark's MERGE meets Spark-stamped properties,
`write.distribution-mode`, a sort order, Spark-written manifests and snapshot summaries
for the first time. This unit measures and pins that meeting: it is evidence only —
no product code changes — and any divergence found is a registry row plus a pin, not a
fix.

**Not in this step:** product code, `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, `.github/`, dependency changes.

## PROPOSITION LEDGER — ICE-SPARK-TABLE-1 step 1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The cheapest working loop is measured: Spark (PySpark 4.1.2 + iceberg-runtime 1.11.0) creates and seeds a v2 copy-on-write table with the production properties on a local-filesystem catalog RePark can adopt; `InMemoryCatalog` is tried first and rejected (writes no local bytes); `type = hadoop` is the working pairing. | Attempts recorded with outcomes; the Spark-written `metadata.json` surface recorded verbatim under Evidence. | PROVEN | InMemory attempt: `wh_files_written = []` — Iceberg's in-memory FileIO writes nothing to the local warehouse, RePark cannot adopt. Hadoop: full loop green, see §C-001 evidence. |
| C-002 | RePark adopts the Spark-created table via `CALL <cat>.system.register_table(table => 'ns.facts', metadata_file => 'v5.metadata.json')` and reads the same 20 seed rows — all five columns, values AND Arrow types — after Spark's own CoW MERGE and `write.distribution-mode='hash'` ALTER. | Pin asserts full five-column rows AND schema; `truth.json` is the fixture's seed oracle. | PROVEN | Adopted `v5`; `snap_ops` `[append, append, overwrite]`; rows equal `truth.json` verbatim including the `spark-19`/`spark-20` post-MERGE rows; `ingestion_timestamp` reads `timestamptz`. |
| C-003 | RePark runs the production silver shape — parquet source → row_number dedup temp view → `MERGE … UPDATE SET * / INSERT *` — twice, with overlapping ids: 20 → 30 → 35 rows, `dup-old-5` dropped by dedup, per-partition counts `{09-13:4, 09-14:10, 09-15:11, 09-16:10}`. | Pin asserts the full five-column set at every stage — seed, 30, 35 — against an independently derived oracle (`_merged_rows`/`_dedup_latest`). | PROVEN | `_assert_table_rows` after adoption, after MERGE 1 (30) and after MERGE 2 (35) in both the fixture test and the live cell; the pinned columns are value AND type, not only `(id,name,ds)`. |
| C-004 | The weekly maintenance CALLs succeed on the Spark-created table in production order: `expire_snapshots` with `older_than` between snapshots 1–2 `committed_at` + `retain_last => 1`, `rewrite_manifests`, `rewrite_data_files(binpack)` (admitted; no-op under the min-input floor — the `options` map is refused in v1), `remove_orphan_files(older_than = now−48h, dry_run => false)` sweeping exactly one planted pre-dated orphan; `rewrite_position_delete_files` runs as a labelled extra (not inventory §1 row 5). | Pin asserts each measured output row; the time predicate is load-bearing (ignoring it deletes 3 manifest lists, not 1). | PROVEN | Expire deleted exactly 1 manifest list; manifests 3→1; binpack zeros; orphan sweep `[orphan-000.parquet]` exactly, file removed; position-delete rewrite zeros. |
| C-005 | Spark reads the RePark-written snapshot back: `EXCEPT ALL` empty both ways against the independently derived expected rows AND against RePark's own `to_arrow()` answer, count 35, equal per-partition counts, `.snapshots` / `.history` 5 rows, `.files` 4 rows, 0 delete files. The write-side behavior on a stale cache is measured: a same-session `INSERT` commits `v10` cleanly (scan-forward), `refreshTable` then reads 36 and a second `INSERT` commits `v11`. | Live pin; readback path measured; stale-write path pinned. | PROVEN | Stale `SELECT` 20 → stale `INSERT` committed `v10` (no failure) → `refreshTable` 36 → `INSERT` `v11`. `register_table ns.facts_repark` at `v9` reads repark's snapshot; EXCEPT ALL 0/0 vs both oracles. |
| C-006 | The transform sort-order residual is pinned as stated, not a failure: a Spark table `WRITE ORDERED BY (bucket(4, id))` (`write.distribution-mode` becomes `range`) registers and reads fine, but RePark's MERGE refuses loudly and commits no snapshot. | Live pin: exact refusal text + snapshot ids unchanged. | PROVEN | `This feature is not implemented: sorting by the table's default sort order uses transform 'bucket[4]' on source id 1, only identity sort fields are supported` — snapshot ids identical before and after; WRITE-ORDER-TRANSFORM-1. |
| C-007 | Every property or metadata shape RePark handles differently on a Spark-created table than on a RePark-created one is named in the difference table (§C-007 evidence) and in the registry row — now including Spark `overwrite` history, `hash` distribution mode, and a second partition spec. | The complete table; registry row `ICE-SPARK-TABLE-1` beside `WRITE-ORDER-TRANSFORM-1`. | PROVEN | Eleven shapes diffed; load-bearing for C4: stale table-cache read → `refreshTable` after repark commits (catalog-agnostic — C4 is Glue, no version hint); transform sort order → loud refusal. |
| C-008 | Offline evidence for PR CI: the Spark-created table directory (metadata + data files + `truth.json` + `map.md`) measures 67,799 bytes < ~300 KB and is committed at `python/repark-parity/fixtures/torture/data/ice_spark_table_1/`; an always-run test materializes it at its baked-in path under a directory lock, runs the full adopt→MERGE→CALLs→assert flow JVM-free, and removes the materialized copy on exit. | Committed fixture + always-run test green. | PROVEN | 67,799 bytes including `map.md`. Fresh mtimes keep every referenced file younger than the `older_than` argument, so the planted pre-dated orphan is the only sweeper candidate on every run date. |
| C-009 | Mutation proof: breaking one assertion's input — emptying the second MERGE's staging batch in a scratch run — turns the pin red. | Red output pasted in §C-009 evidence. | PROVEN | Mutant red at `_assert_table_rows`: 30 rows vs the derived 35 (per-partition counts `{09-15:16}` vs `{09-15:11}`). |
| C-010 | The live module is green co-collected with the existing live suite: `REPARK_PARITY_LIVE=1 pytest test_v3e3_fixtures.py test_ice_spark_table_1.py`. | Co-collected run output pasted. | PROVEN | `9 passed in 17.74s` — 5 v3e3 + 4 ICE-SPARK-TABLE-1 cells on one JVM. |
| C-011 | The card's gates all pass: the module offline (`2 passed, 2 skipped`), `make verify`, `make check-docs-links`, `make check-ledger-grammar`, `make check-ledgers`, the whole parity suite, and the staged comment-scrub grep printing nothing. | Command → result table in §C-011 evidence. | PROVEN | All green; see table. |

## Critic round (Grok 4.6 critic-logic) — findings and disposition

The orchestrator ran a critic-logic review of `5e7a2ed3`
(`/tmp/oc-worker/i-scrit/report.md`). Verdict there: no P1, six P2, several P3; four of
its scratch mutants went red and it independently derived the same 35-row set. Every
finding is addressed in this round; the one place the measurement contradicted the
critic's bytecode-derived expectation (P2-3 write side) is recorded under §C-005.

| Finding | Disposition |
|---|---|
| P2-1 / P2-6 — row pins dropped `amount`/`ingestion_timestamp` and skipped the 30-row state; EXCEPT ALL compared repark's answer to itself | `_assert_table_rows` pins all five columns (values and Arrow types) after adoption, after MERGE 1 (30) and after MERGE 2 (35), fixture and live. The live `EXCEPT ALL` now diffs Spark's read of `v9` against the independently derived expected set (a parquet of `_expected_final_rows`) and against repark's answer. `truth.json` is the fixture seed oracle (P3-2). |
| P2-2 — `older_than 2999`/`2020` made three CALL pins vacuous | `expire_snapshots` now uses the midpoint of snapshots 1–2 `committed_at` + `retain_last => 1`: only snapshot 1 is both time-eligible and non-retained, so an ignored time predicate would delete 3 lists vs the pinned 1. `remove_orphan_files` sweeps a planted `orphan-000.parquet` (mtime 2020) with `older_than` = now−48h — exactly one row returned, file removed, all fresh files protected. `rewrite_data_files` is pinned as admitted-and-no-op under the min-input floor (the `options` map is refused in v1, so the floor cannot be lowered in the CALL). `rewrite_position_delete_files` is labelled an extra CALL — not inventory §1 row 5 (P3-3). |
| P2-3 — the stale `version-hint` finding was a stale READ, not the C4 write mechanism | Re-measured live: the same-session `SELECT` serves the stale cache (20) until `refreshTable` — cache, not hint. The critic's bytecode-derived `CommitFailedException` on a stale write did **not** occur: Spark's `INSERT` without refresh committed `v10` (the write path scan-forwards to repark's `v9`), `refreshTable` read 36, the post-refresh `INSERT` committed `v11`, hint moved to `11`. C-007/registry/inventory corrected: the C4 action is `refreshTable` after repark commits (Spark's table cache is catalog-agnostic; Glue has no hint). The rename-time `exists` check remains a race guard only; the reverse direction — repark (fork R167) has no exists-fail on a Hadoop `vN` collision — is named as the residue. |
| P2-4 — property preservation and codec honour unpinned | The final `v9` metadata `properties` are asserted equal to the adopted Spark properties (all seven keys) in fixture and live. Every repark-written data file's parquet footer is pinned `ZSTD` in `_assert_final_state`; the live snappy leg (`write.parquet.compression-codec='snappy'` table) pins repark's output file `SNAPPY`. |
| P2-5 — G-3 shapes missing: Spark `overwrite` history, `write.distribution-mode`, second partition spec | All three added and measured. Spark runs its own CoW MERGE pre-adoption (history `[append, append, overwrite]`; fixture regenerated with it). `write.distribution-mode='hash'` is set pre-adoption on the main table and carried verbatim through `v9`. `ALTER TABLE … ADD PARTITION FIELD days(ingestion_timestamp)` creates spec-id 1 as default; repark's MERGE **honours** it — the new file carries `spec_id 1` under `ds=…/ingestion_timestamp_day=…/` while the old file keeps `spec_id 0`; rows read correctly and Spark re-reads the evolved table after refresh. Fixture stayed 67,799 bytes ≪ 300 KB so (a)+(b) were regenerated into it; (c) is live-only because an always-run spec-evolution expectation is not simpler than the live pin. |
| P3-1 — fixture size omitted `map.md` | C-008 now reports 67,799 bytes including `map.md`. |
| P3-4 — materialized copy left behind | `_materialize` removes `_TABLE_ROOT` on exit (the lock still guards concurrent runs). |
| P3-5 — `ORDER BY committed_at` vs baked fixture timestamps | The dynamic `older_than` is read from `.snapshots` at run time, so a host clock skew against the fixture's baked `timestamp-ms` cannot misclassify snapshots; noted as resolved by construction. |
| P3-6 — branch reverted v1.4.1 | Orchestrator rebased the unit onto `629f3d5f` as `d5708359`; this round's commit sits on top. |
| P3-7 — live re-run blocked in the critic sandbox | Re-executed here: `9 passed in 17.74s` co-collected. |
| P3-8 — M4 red was a `KeyError` | The new mutant empties a staging batch — a behavioral red at the row pin, not a lookup failure. |

## Evidence

### C-001 — the mechanism, measured 2026-09-14 (PySpark 4.1.2, Iceberg 1.11.0, zulu-17)

**Catalog attempts, in the card's order.**

| Attempt | Outcome |
|---|---|
| `catalog-impl = org.apache.iceberg.inmemory.InMemoryCatalog`, local `warehouse` | `DESCRIBE` reports the location but `wh_files_written = []` — the in-memory FileIO writes no bytes anywhere; there is no `metadata.json` on disk for `register_table` to adopt. Rejected. |
| `type = hadoop`, `warehouse = <tmp>` | Working pairing. Spark writes `<warehouse>/ns/facts/` with `data/ds=…/`, `metadata/v1…v5.metadata.json`, manifest Avro files, `version-hint.text` (`5`). |

**Spark-created `metadata/v5.metadata.json`, verbatim surface:**

```json
{
  "format-version": 2,
  "properties": {
    "owner": "repark",
    "write.merge.mode": "copy-on-write",
    "write.delete.mode": "copy-on-write",
    "write.parquet.compression-codec": "zstd",
    "write.update.mode": "copy-on-write",
    "write.target-file-size-bytes": "268435456",
    "write.distribution-mode": "hash"
  },
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "ds", "transform": "identity", "source-id": 5, "field-id": 1000}]}],
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "schemas": [{"schema-id": 0, "fields": [
    {"id": 1, "name": "id", "required": false, "type": "long"},
    {"id": 2, "name": "name", "required": false, "type": "string"},
    {"id": 3, "name": "amount", "required": false, "type": "decimal(12, 2)"},
    {"id": 4, "name": "ingestion_timestamp", "required": false, "type": "timestamptz"},
    {"id": 5, "name": "ds", "required": false, "type": "string"}]}],
  "snapshots": ["append", "append", "overwrite"]
}
```

Spark stamps three properties the DDL never asked for (`owner`, the parquet codec,
`write.distribution-mode` via ALTER) plus snapshot-summary keys repark never writes
(`spark.app.id`, `app-id`, `app-name`, `engine-name`, `engine-version`,
`iceberg-version`, `manifests-created/kept/replaced`). Snapshots: two `append` (the
seed batches) and one `overwrite` (Spark's own CoW MERGE updating ids 19–20);
`statistics: []`, `partition-statistics: []`, `version-hint.text` = `5`.

**RePark adoption and write phase (same run).** `register_table` on the memory catalog
reads all 20 adopted rows. Two production MERGEs commit `v6` and `v7` (30 then 35 rows).
Expire commits `v8`, `rewrite_manifests` `v9`. Final metadata files: `v1…v9` — **repark
continues Spark's `vN` naming into the adopted table**. `version-hint.text` stays at
`5` — repark never rewrites it.

### C-004 — CALL outputs (verbatim)

| CALL | Output row |
|---|---|
| `expire_snapshots` (`older_than` = midpoint of snaps 1–2 `committed_at`, `retain_last => 1`) | `deleted_data_files_count 0, deleted_position_delete_files_count 0, deleted_equality_delete_files_count 0, deleted_manifest_files_count 0, deleted_manifest_lists_count 1, deleted_statistics_files_count 0` — only snapshot 1 is both older than the cutoff and outside the retained newest 1; an ignored time predicate would delete 3 lists |
| `rewrite_manifests` | `rewritten_manifests_count 3, added_manifests_count 1` |
| `rewrite_data_files` (binpack) | `rewritten_data_files_count 0, added_data_files_count 0, rewritten_bytes_count 0, failed_data_files_count 0, removed_delete_files_count 0` — **CALL admitted, no-op under the min-input floor** (v1 refuses the `options` map, so the floor cannot be lowered; four live files per partition-group is under the default `min_input_files=5`) |
| `remove_orphan_files` (`older_than` = now−48h) | `[{orphan_file_location: <table>/data/ds=2026-09-13/orphan-000.parquet}]` — exactly the planted pre-dated file, removed; the procedure layer refuses an `older_than` less than 24 h in the past, and every real file (mtime = now) is younger than the cutoff, so nothing referenced is eligible |
| `rewrite_position_delete_files` (extra — not inventory §1 row 5) | `rewritten_delete_files_count 0, added_delete_files_count 0, rewritten_bytes_count 0, added_bytes_count 0` |

Snapshot log after the phase (`.snapshots` by `committed_at`): `append` (Spark's
second seed insert), `overwrite` ×3 (Spark's own MERGE, then repark's two MERGEs),
`replace` (`rewrite_manifests`). Zero delete files throughout — copy-on-write
confirmed end to end.

### C-005 — Spark read-back and stale-cache numbers

Stale `SELECT` `20` (Spark's cached table, adopted state) → same-session `INSERT`
commits `v10` — the write path scan-forwards to repark's `v9`, so there is **no**
`CommitFailedException` and no clobber of repark's files (the critic's bytecode
expectation; measured otherwise) → `refreshTable` reads `36` → a second `INSERT`
commits `v11`, `version-hint.text` moves to `11`, count `37`.

`register_table ns.facts_repark` at `v9` reads repark's snapshot directly:
`EXCEPT ALL` both directions `0` against the independently derived expected rows AND
against repark's `to_arrow()` parquet answer; count `35`; per-partition
`{2026-09-13: 4, 2026-09-14: 10, 2026-09-15: 11, 2026-09-16: 10}`; `.snapshots` 5 rows,
`.history` 5 rows, `.files` 4 rows, `.delete_files` 0.

### C-006 — the transform-sort residual (WRITE-ORDER-TRANSFORM-1)

Spark accepts `ALTER TABLE … WRITE ORDERED BY (bucket(4, id))` and stamps
`write.distribution-mode = 'range'` plus sort-order 1 `bucket[4]` on source id 1.
RePark registers and reads the table (3 seed rows), then refuses the production MERGE:

```
This feature is not implemented: sorting by the table's default sort order uses
transform `bucket[4]` on source id 1, only identity sort fields are supported
```

Snapshot ids before and after the refused MERGE are identical — nothing committed, no
partial state. This is the stated residual: a Spark-created table with a transform
sort order is adopted and read correctly but cannot take RePark's sorted write; a loud
refusal, never a wrong write.

### C-007 — the difference table (owner's standing requirement)

Twin created in the same run: repark `CREATE TABLE … USING iceberg` with identical DDL
and `TBLPROPERTIES` (including `write.distribution-mode='hash'`), two `INSERT`
batches, on a memory catalog.

| Shape | Spark-created value | RePark-created (twin) value | RePark writing into the Spark-created table | Spark read-back affected? |
|---|---|---|---|---|
| Stamped properties | The 4 requested `write.*` keys plus `owner`, `write.parquet.compression-codec = 'zstd'`, `write.distribution-mode = 'hash'` | The 5 requested keys only | **Honours and carries**: repark's `v9` preserves all 7 properties verbatim (pinned: `v9.properties ==` adopted `v5.properties`); a `snappy`-stamped table gets SNAPPY repark files (footer pin) | No — Spark reads repark's zstd files |
| Spark `overwrite` history | Third snapshot is Spark's own CoW MERGE (`summary.operation = overwrite`, Spark summary keys) | n/a on a fresh twin | **Honours**: repark MERGEs and the maintenance CALLs operate over Spark-written overwrite manifests/summaries; expire drops only snapshot 1 | No — `.snapshots` reads all rows |
| `write.distribution-mode` | `hash` (ALTER) | n/a unless stamped | **Honours**: carried through `v9`; MERGE distributes and writes correctly | No |
| Second partition spec | `ALTER … ADD PARTITION FIELD days(ingestion_timestamp)` → spec-id 1 becomes default (fields `ds` + `day(ingestion_timestamp)`) | n/a | **Honours**: repark's MERGE writes the new file with `spec_id 1` under `ds=…/ingestion_timestamp_day=…/`; the old file keeps `spec_id 0`; `.files` shows `{0,1}` | No — Spark re-reads after refresh |
| Spark table cache / `version-hint.text` | Hint present (`5`); the Hadoop catalog scan-forwards on refresh | Never written | **Never rewritten** by repark. Stale **read**: cached table serves the adopted snapshot (20) until `refreshTable` or a fresh `register_table` — cache, not hint. Stale **write**: none — a same-session `INSERT` scan-forwards and commits `v10` cleanly; post-refresh write commits `v11` | **Reads only — refreshTable after a repark commit.** Production C4 is Glue (no hint); the cache refresh is the catalog-agnostic action. Reverse residue: repark (fork R167) has no exists-fail on a Hadoop `vN` collision |
| Metadata file naming | `vN.metadata.json` sequence | `NNNNN-<uuid>.metadata.json` | **Continues the adopted convention**: repark writes `v6…v9` into the Spark-created table | No |
| Snapshot summary keys | Spark engine keys: `spark.app.id`, `app-id`, `app-name`, `engine-name`, `engine-version`, `iceberg-version`, `manifests-created/kept/replaced` | Repark keys: `engine.operation-id`, `removed-files-size`, `deleted-data-files`, `deleted-records`; no engine identity | **Writes its own vocabulary**; Spark's surviving snapshots keep their Spark keys — mixed summaries coexist | No — `.snapshots` reads all rows fine |
| `statistics` / `partition-statistics` keys | Present as empty lists | Absent (repark never writes the keys) | **Dropped** on repark's commits (v9 has neither key) | No |
| Default sort order | `order-id 0`, empty | `order-id 0`, empty | Same on the plain table; **refuses loudly** when the order carries a transform (`bucket[4]`, C-006) | Only on the transform table: the MERGE refuses rather than writing wrong |
| Partition spec 0 / schema / requiredness | `identity(ds)` `source-id 5` `field-id 1000`; 5 nullable fields, `timestamptz` | Byte-identical spec and schema (`decimal(12,2)` vs `decimal(12, 2)` whitespace-only) | Carried forward unchanged | No |
| Manifest + data-file surface | Hadoop manifests, `data/ds=…/` layout, zstd codec | Same layout and codec read from table properties | **Honours**: Spark's `.files` lists repark's 4 live data files; `EXCEPT ALL` both ways empty | No |

### C-008 — the offline fixture

`python/repark-parity/fixtures/torture/data/ice_spark_table_1/` — 67,799 bytes
(including `map.md`), under the ~300 KB ceiling: three zstd data files
(`ds=2026-09-13/14`, the third written by Spark's own MERGE), `v1…v5.metadata.json`,
four manifests + three manifest lists, `version-hint.text`, `truth.json` (the 20
adopted rows verbatim — the seed oracle), `map.md`. Materialized at
`/tmp/repark-ice-spark-table-1/ns/facts` (the path baked into its metadata) under
`_DirLock`, copied with `shutil.copy` so file mtimes are always fresh, and removed on
test exit. Fresh mtimes put every referenced file newer than the `older_than` cutoff,
so the planted pre-dated orphan is the only sweeper candidate on every run date; the
procedure layer's own floor refuses an `older_than` less than 24 h old regardless.

### C-009 — mutation proof

Mutant: `_merge_source_two` emptied (the second MERGE commits a no-op batch instead of
five updates and five inserts).

```
MUTANT RED: AssertionError {'2026-09-13': 4, '2026-09-14': 10, '2026-09-15': 16}
```

Red at `_assert_table_rows` / the partition-count pin: 30 rows vs the derived 35,
`ds=2026-09-15` holding 16 rows vs the pinned 11 — the pin is not vacuously green.

### C-010 — co-collected live run

```
REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=/tmp/oc-worker/i-spark/ivy2 pyspark-shell" \
.venv/bin/python -m pytest python/repark/tests/test_v3e3_fixtures.py \
  python/repark/tests/test_ice_spark_table_1.py -q -p no:cacheprovider
.........                                                                [100%]
9 passed in 17.74s
```

`pgrep -x java` was empty before the run; one JVM for the whole co-collected session;
`SparkSession.getActiveSession()` recorded first and never stopped (the suite's
`_shared_oracle_context_guard` forbids stopping the shared context). The ivy redirect
lives outside the clone — `.ivy2/` inside the repo is tracked and untouched
(`git status` shows nothing under it).

### C-011 — gate results

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_ice_spark_table_1.py -q` | `2 passed, 2 skipped in 1.24s` |
| Co-collected live (C-010 command line) | `9 passed in 17.74s` |
| `make verify` | green |
| `make check-docs-links` | green |
| `make check-ledger-grammar` | green |
| `make check-ledgers` | green |
| parity suite `python/repark-parity/tests` | green |
| `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' \| grep -P '^\+\s*(//\|#(?! noqa))'` | prints nothing |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-spark-table-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-011 walked one by one against measured behavior — the InMemoryCatalog dead end was tried first and recorded, the hadoop loop measured end to end, every pin asserts measured values, the mutation went red, the co-collected live run is pasted, and the Grok critic-logic round's six P2s and P3s are all dispositioned with the changed evidence.
      artifacts: [task/ledgers/staging/ice-spark-table-1-ledger.md, python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised: two partitions, four after merge; an overlapping-id dedup row that must drop; a transform sort order refusal; the stale-cache reread and the stale-cache write; a second partition spec; a snappy-stamped table; a planted orphan against a dynamic cutoff; Spark-written overwrite history.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal path is pinned — the bucket[4] MERGE raises the exact transform message and the Spark-side snapshot ids are byte-identical before and after, proving nothing committed. The stale-cache reread pins the other silent-failure shape (20 vs 36 after refresh).
      artifacts: [python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: One JVM for the whole live cell; SparkSession.getActiveSession() recorded before getOrCreate and never stopped (the conftest guard also fails any test that stops the shared context); the fixture copy sits under a cross-process directory lock and is removed on exit; repark sessions are created and stopped per test.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py, python/repark/tests/conftest.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, network, injection or deserialization surface — a local Hadoop-catalog Iceberg table and a memory catalog.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned at every stage (int64/string/decimal128(12,2)/timestamptz); the read-back oracle is EXCEPT ALL both directions against an independently derived row set, not repark's own answer and not a row count; every divergence found is named in C-007 and the registry row rather than absorbed.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: The fixture is 67,799 bytes including map.md against the ~300 KB ceiling; the module adds no code paths to product crates (evidence-only unit); the test file is 896 lines against the 1000-line default ceiling.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_spark_table_1, task/ledgers/staging/ice-spark-table-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Dependency surface untouched — the pinned iceberg-spark-runtime 4.1_2.13:1.11.0 GAV and PySpark 4.1.2 are the suite's existing oracle; no Cargo/pyproject/lockfile edits.
      artifacts: [python/repark/tests/_oracle_pins.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change; the refusal text is the engine's existing typed error surfaced through the facade.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held by the mutation proof — the empty-merge-2 mutant reds the five-column row pin (30 vs 35, ds=2026-09-15 holding 16 vs 11); the live legs genuinely ran (9 co-collected passes on one JVM including the snappy, spec-evolution and stale-write legs), not skips.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py, task/ledgers/staging/ice-spark-table-1-ledger.md]
```
