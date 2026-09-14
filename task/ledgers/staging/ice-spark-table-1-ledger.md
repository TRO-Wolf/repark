# Unit ledger — ICE-SPARK-TABLE-1 · RePark writes into a Spark-created table — step 1

**Date:** 2026-09-14 · **Branch:** `test/ice-spark-table-1` · **Base:** `941deca8`
(`main` tip at session start)
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
| C-002 | RePark adopts the Spark-created table via `CALL <cat>.system.register_table(table => 'ns.facts', metadata_file => 'v3.metadata.json')` and reads the same 20 seed rows with the same Arrow types (int64 / string / decimal128(12,2) / timestamp[us,tz=UTC] / string). | Pin asserts rows AND schema. | PROVEN | `repark_seed_count = 20`, rows equal, `ingestion_timestamp` reads `timestamptz` (Spark's TIMESTAMP DDL stamps tz). |
| C-003 | RePark runs the production silver shape — parquet source → row_number dedup temp view → `MERGE … UPDATE SET * / INSERT *` — twice, with overlapping ids: 20 → 30 → 35 rows, `dup-old-5` dropped by dedup, per-partition counts `{09-13:4, 09-14:10, 09-15:11, 09-16:10}`. | Pin asserts the `(id, name, ds)` set verbatim. | PROVEN | `merge1_names` / `merge2_names` measured; the 35-row expected set is derived in `_expected_final_rows`. |
| C-004 | The weekly maintenance CALLs succeed on the Spark-created table in production order: `expire_snapshots(older_than 2999, retain_last 3)`, `rewrite_manifests`, `rewrite_data_files(binpack)`, `remove_orphan_files(older_than 2020, dry_run => false)`, `rewrite_position_delete_files`. | Pin asserts each measured output row. | PROVEN | Expire deleted 1 manifest list (oldest of 4 snapshots, retain 3); manifests 3→1; binpack a no-op under the min-input floor; orphan sweep empty (24-hour mtime floor protects the just-written files); position-delete rewrite zeros on a CoW table. |
| C-005 | Spark reads the RePark-written snapshot back: `EXCEPT ALL` empty both ways against RePark's own answer, count 35, equal per-partition counts, `.snapshots` / `.history` / `.files` readable (4 rows each), 0 delete files. | Live pin; readback path measured. | PROVEN | Direct reread served the STALE snapshot (20) — Spark's `version-hint.text` still says `3` because repark never rewrites it; `spark.catalog.refreshTable` yields 35 and `register_table` of `ns.facts_repark` at `v7.metadata.json` reads it directly. EXCEPT ALL 0/0. |
| C-006 | The transform sort-order residual is pinned as stated, not a failure: a Spark table `WRITE ORDERED BY (bucket(4, id))` (`write.distribution-mode` becomes `range`) registers and reads fine, but RePark's MERGE refuses loudly and commits no snapshot. | Live pin: exact refusal text + snapshot ids unchanged. | PROVEN | `This feature is not implemented: sorting by the table's default sort order uses transform 'bucket[4]' on source id 1, only identity sort fields are supported` — snapshot ids `[5963042274880927714]` identical before and after; WRITE-ORDER-TRANSFORM-1. |
| C-007 | Every property or metadata shape RePark handles differently on a Spark-created table than on a RePark-created one is named in the difference table (§C-007 evidence) and in the registry row. | The complete table; registry row `ICE-SPARK-TABLE-1` beside `WRITE-ORDER-TRANSFORM-1`. | PROVEN | Nine shapes diffed; two are load-bearing for C4 (stale version-hint → refresh required; transform sort order → refusal). |
| C-008 | Offline evidence for PR CI: the Spark-created table directory (metadata + data files + `truth.json` + `map.md`) measures 36,612 bytes < ~300 KB and is committed at `python/repark-parity/fixtures/torture/data/ice_spark_table_1/`; an always-run test materializes it at its baked-in path under a directory lock and runs the full adopt→MERGE→CALLs→assert flow JVM-free. | Committed fixture + always-run test green. | PROVEN | 36,612 bytes. Copy uses `shutil.copy` (fresh mtimes) so the 24-hour orphan floor is deterministic on every run date. |
| C-009 | Mutation proof: breaking one assertion's input in a scratch copy — running only the first MERGE — turns the pin red. | Red output pasted in §C-009 evidence. | PROVEN | Mutant red at the maintenance-output pin: one MERGE leaves 3 snapshots pre-expire, so `retain_last => 3` deletes 0 manifest lists vs the pinned 1. |
| C-010 | The live module is green co-collected with the existing live suite: `REPARK_PARITY_LIVE=1 pytest test_v3e3_fixtures.py test_ice_spark_table_1.py`. | Co-collected run output pasted. | PROVEN | `9 passed in 15.83s` — 5 v3e3 + 4 ICE-SPARK-TABLE-1 cells on one JVM. |
| C-011 | The card's gates all pass: the module offline (`2 passed, 2 skipped`), `make verify`, `make check-docs-links`, `make check-ledger-grammar`, `make check-ledgers`, the whole parity suite, and the staged comment-scrub grep printing nothing. | Command → result table in §C-011 evidence. | PROVEN | All green; see table. |

## Evidence

### C-001 — the mechanism, measured 2026-09-14 (PySpark 4.1.2, Iceberg 1.11.0, zulu-17)

**Catalog attempts, in the card's order.**

| Attempt | Outcome |
|---|---|
| `catalog-impl = org.apache.iceberg.inmemory.InMemoryCatalog`, local `warehouse` | `DESCRIBE` reports the location but `wh_files_written = []` — the in-memory FileIO writes no bytes anywhere; there is no `metadata.json` on disk for `register_table` to adopt. Rejected. |
| `type = hadoop`, `warehouse = <tmp>` | Working pairing. Spark writes `<warehouse>/ns/facts/` with `data/ds=…/`, `metadata/v1…v3.metadata.json`, manifest Avro files, `version-hint.text` (`3`). |

**Spark-created `metadata/v3.metadata.json`, verbatim surface:**

```json
{
  "format-version": 2,
  "properties": {
    "owner": "john",
    "write.merge.mode": "copy-on-write",
    "write.delete.mode": "copy-on-write",
    "write.parquet.compression-codec": "zstd",
    "write.update.mode": "copy-on-write",
    "write.target-file-size-bytes": "268435456"
  },
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "ds", "transform": "identity", "source-id": 5, "field-id": 1000}]}],
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "schemas": [{"schema-id": 0, "fields": [
    {"id": 1, "name": "id", "required": false, "type": "long"},
    {"id": 2, "name": "name", "required": false, "type": "string"},
    {"id": 3, "name": "amount", "required": false, "type": "decimal(12, 2)"},
    {"id": 4, "name": "ingestion_timestamp", "required": false, "type": "timestamptz"},
    {"id": 5, "name": "ds", "required": false, "type": "string"}]}]
}
```

Spark stamps two properties the DDL never asked for (`owner`, the parquet codec) plus
snapshot-summary keys repark never writes (`spark.app.id`, `app-id`, `app-name`,
`engine-name`, `engine-version`, `iceberg-version`, `manifests-created/kept/replaced`).
Two `append` snapshots (one per INSERT), `statistics: []`, `partition-statistics: []`,
`version-hint.text` = `3`.

**RePark adoption and write phase (same run).** `register_table` on the memory catalog
reads all 20 seed rows. Two production MERGEs commit `v4` and `v5` (30 then 35 rows).
The five CALLs produce the measured outputs under C-004. Final metadata files on the
Spark-created table: `v1…v7.metadata.json` — **repark continues Spark's `vN` naming
into the adopted table** (a repark-created twin writes `NNNNN-uuid` names instead;
see C-007). `version-hint.text` is left at `3` — repark never rewrites it.

**Spark read-back paths, measured.** A direct re-`SELECT` on the Hadoop catalog table
returns the stale count 20 (Spark cached the version-hint snapshot). After
`spark.catalog.refreshTable` the same table answers 35; a second
`CALL … register_table` under `ns.facts_repark` pointing at `v7.metadata.json` reads
the RePark snapshot directly. Both doors work; the version hint itself never moved.

### C-004 — CALL outputs (verbatim)

| CALL | Output row |
|---|---|
| `expire_snapshots` | `deleted_data_files_count 0, deleted_position_delete_files_count 0, deleted_equality_delete_files_count 0, deleted_manifest_files_count 0, deleted_manifest_lists_count 1, deleted_statistics_files_count 0` |
| `rewrite_manifests` | `rewritten_manifests_count 3, added_manifests_count 1` |
| `rewrite_data_files` | `rewritten_data_files_count 0, added_data_files_count 0, rewritten_bytes_count 0, failed_data_files_count 0, removed_delete_files_count 0` (binpack below the min-input floor) |
| `remove_orphan_files` | empty result set (0 rows; the 24-hour mtime floor protects every just-written file) |
| `rewrite_position_delete_files` | `rewritten_delete_files_count 0, added_delete_files_count 0, rewritten_bytes_count 0, added_bytes_count 0` |

Snapshot log after the phase: `append` (Spark's second seed insert — the first expired),
`overwrite` ×2 (the two CoW MERGEs), `replace` (`rewrite_manifests`). Zero delete files
throughout — copy-on-write confirmed end to end.

### C-005 — Spark read-back numbers

`stale reread 20` → `refreshTable` → `35`; `register_table ns.facts_repark` at `v7`;
`EXCEPT ALL` both directions `0`; count `35`; per-partition `{2026-09-13: 4,
2026-09-14: 10, 2026-09-15: 11, 2026-09-16: 10}`; `.snapshots` 4 rows, `.history` 4
rows, `.files` 4 rows, `.delete_files` 0. RePark's own answer was written to parquet
and read into Spark for the diff — Spark reads repark's `timestamp[us,tz=UTC]`
parquet as `timestamptz` and the multiset diff is empty.

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
and the same `TBLPROPERTIES`, two `INSERT` batches, on a memory catalog.

| Shape | Spark-created value | RePark-created (twin) value | RePark writing into the Spark-created table | Spark read-back affected? |
|---|---|---|---|---|
| Stamped properties | The 4 requested `write.*` keys **plus** `owner` and `write.parquet.compression-codec = 'zstd'` | Exactly the 4 requested `write.*` keys | **Honours and carries**: repark's `v7` preserves all 6 properties verbatim; a direct probe (Spark table stamped `snappy`) shows repark reads the codec at write time — its files come out SNAPPY, so this is honoured, not a coincidental default | No — Spark reads repark's zstd files |
| `version-hint.text` | Present (`3`), maintained by the Hadoop catalog | Never written | **Never rewritten**: after repark commits `v4…v7` the hint still says `3`; Spark's direct reread serves the STALE snapshot (20) until `refreshTable` or a fresh `register_table` | **Yes — refresh or register_table is required after a repark commit.** The C4 runbook must refresh |
| Metadata file naming | `vN.metadata.json` sequence | `NNNNN-<uuid>.metadata.json` | **Continues the adopted convention**: repark writes `v4…v7` into the Spark-created table | No |
| Snapshot summary keys | Spark engine keys: `spark.app.id`, `app-id`, `app-name`, `engine-name`, `engine-version`, `iceberg-version`, `manifests-created/kept/replaced` | Repark keys: `engine.operation-id`, `removed-files-size`, `deleted-data-files`, `deleted-records`; no engine identity | **Writes its own vocabulary**; Spark's surviving append snapshot keeps its Spark keys — mixed summaries coexist | No — `.snapshots` reads all rows fine |
| `statistics` / `partition-statistics` keys | Present as empty lists | Absent (repark never writes the keys) | **Dropped** on repark's commits (v7 has neither key) | No |
| Default sort order | `order-id 0`, empty | `order-id 0`, empty | Same on the plain table; **refuses loudly** when the order carries a transform (`bucket[4]`, C-006) | Only on the transform table: the MERGE refuses rather than writing wrong |
| Partition spec | `identity(ds)`, `source-id 5`, `field-id 1000` | Byte-identical spec | Carried forward unchanged | No |
| Schema / requiredness | 5 fields, all nullable, `timestamptz` | Same fields; `decimal(12,2)` vs `decimal(12, 2)` whitespace-only in the JSON | Carried forward unchanged | No |
| Manifest + data-file surface | Hadoop manifests, `data/ds=…/` layout, zstd codec | Same layout and codec read from table properties | **Honours**: Spark's `.files` lists repark's 4 live data files; `EXCEPT ALL` both ways empty | No |

### C-008 — the offline fixture

`python/repark-parity/fixtures/torture/data/ice_spark_table_1/` — 36,612 bytes, under
the ~300 KB ceiling: two zstd data files (`ds=2026-09-13/14`), `v1…v3.metadata.json`,
two manifests + two manifest lists, `version-hint.text`, `truth.json` (the 20 seed
rows verbatim), `map.md`. Materialized at `/tmp/repark-ice-spark-table-1/ns/facts`
(the path baked into its metadata) under `_DirLock`, copied with `shutil.copy` so file
mtimes are always fresh — the `remove_orphan_files` 24-hour floor then behaves
identically on every run date.

### C-009 — mutation proof

Mutant: `_repark_write_phase` runs only `merge_source_one` (one MERGE instead of two).

```
MUTANT RED
[{'deleted_data_files_count': 0, 'deleted_position_delete_files_count': 0,
  'deleted_equality_delete_files_count': 0, 'deleted_manifest_files_count': 0,
  'deleted_manifest_lists_count': 0, 'deleted_statistics_files_count': 0}]
```

Red at `_assert_maintenance_outputs`: with one MERGE the table holds 3 snapshots
pre-expire, `retain_last => 3` deletes 0 manifest lists vs the pinned 1 — the
maintenance pin catches the mutation before the row pin would. The pin is not
vacuously green.

### C-010 — co-collected live run

```
REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=/tmp/i-spark/.ivy2 pyspark-shell" \
.venv/bin/python -m pytest python/repark/tests/test_v3e3_fixtures.py \
  python/repark/tests/test_ice_spark_table_1.py -q -p no:cacheprovider
.........                                                                [100%]
9 passed in 15.43s
```

`pgrep -x java` was empty before the run; one JVM for the whole co-collected session;
`SparkSession.getActiveSession()` recorded first and never stopped (the suite's
`_shared_oracle_context_guard` forbids stopping the shared context).

### C-011 — gate results

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_ice_spark_table_1.py -q` | `2 passed, 2 skipped in 1.25s` |
| Co-collected live (C-010 command line) | `9 passed in 15.43s` |
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
      evidence: Clauses C-001..C-011 walked one by one against measured behavior — the InMemoryCatalog dead end was tried first and recorded, the hadoop loop measured end to end, every pin asserts measured values, the mutation went red, and the co-collected live run is pasted.
      artifacts: [task/ledgers/staging/ice-spark-table-1-ledger.md, python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised: two partitions, four after merge; an overlapping-id dedup row that must drop; a transform sort order refusal; the stale version-hint reread; a 0-row CALL result; Spark-written metadata with stamped properties repark never asked for.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal path is pinned — the bucket[4] MERGE raises the exact transform message and the Spark-side snapshot ids are byte-identical before and after, proving nothing committed. The stale-hint reread pins the other silent-failure shape (20 vs 35).
      artifacts: [python/repark/tests/test_ice_spark_table_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: One JVM for the whole live cell; SparkSession.getActiveSession() recorded before getOrCreate and never stopped (the conftest guard also fails any test that stops the shared context); the fixture copy sits under a cross-process directory lock; repark sessions are created and stopped per test.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py, python/repark/tests/conftest.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, network, injection or deserialization surface — a local Hadoop-catalog Iceberg table and a memory catalog.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned (int64/string/decimal128(12,2)/timestamptz); EXCEPT ALL in both directions is the read-back oracle, not a row count alone; every divergence found is named in C-007 and the registry row rather than absorbed.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: The fixture is 36,612 bytes against the ~300 KB ceiling; the module adds no code paths to product crates (evidence-only unit); file-size gates hold at the default ceiling.
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
      evidence: Red-first held by the mutation proof — the merge-once mutant reds the maintenance-output pin (0 vs 1 deleted manifest lists) before the row pin; the live legs genuinely ran (9 co-collected passes on one JVM), not skips.
      artifacts: [python/repark/tests/test_ice_spark_table_1.py, task/ledgers/staging/ice-spark-table-1-ledger.md]
```
