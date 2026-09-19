# Night report: run 23c, 2026-09-18 21:33 → 2026-09-19 (the Spark–Iceberg parity inventory)

**Session:** one Opus orchestrator (night-23c). **Orchestrator:** Claude (claude-opus-5). **Lane:** `nc-build` (`/tmp/nc-build`).
- **Actors:** none. The inventory writes no product code; I wrote the probe harness and the cell families myself.
- **Critic:** one read-only Claude Sonnet critic (fresh context, over a copy of the evidence at `/tmp/nc-critic`). No Opus critic, no Grok spend.
- **Beside:** runs 23a (fork lane) and 23b (RePark and pin bumps), coordinated through `claims.txt`.
- **Machine:** one release native build (the only cargo job of the run), one JVM at a time under `_lib/jvm-lock.sh`, everything else inside `repark.slice`.

**In one paragraph.** Owner direction 20:55: "1 to 1 parity with spark's integration with iceberg … NO STONES LEFT".
- **Enumeration.** I listed the whole surface of Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13` 1.11.0 from two sources:
  - the runtime jar via `javap`: 20 procedures with every declared argument, 27 read options, 25 write options, 30 SQL properties, 16 metadata tables, 12 metadata columns, 10 extension statements and 124 table-property keys;
  - the 1.11 docs: views and the `<catalog>.system` SQL functions.
- **Measurement.** 842 cells, each run on Spark and on RePark main `6a140eb3`: 424 EQUAL, 57 DIFFERENT, 126 unregistered refusals, 28 not parsed, 87 registered refusals, 120 Spark-cannot.
- **Output.** The findings are ranked into 56 units in `task/roadmap/mid-term/ice-parity-inventory-2026-09-19.md`, PR **#713**, merged 22:58 as `e6ec5531`.
- **Checks outside the per-cell verdicts:**
  - Cross-engine interop: 13 of 13 shapes equal in both directions.
  - Filter pushdown: 0 silent differences in 369 predicate evaluations.
  - v3 lineage after DML: equal on 12 of 14 cells.
  - Stability: RePark 0 of 736 cells changed on a second run.
- **Error contract.** RePark matches Spark's error condition on only 3 of the 118 cells where both engines refuse.

## 1. The matrix (measured on RePark main `6a140eb3`, fork pin `18ab9761`)

| group | EQUAL | DIFFERENT | REFUSED-UNREGISTERED | NOT-PARSED | REFUSED-REGISTERED | SPARK-CANNOT |
|---|---|---|---|---|---|---|
| PROPS | 35 | 11 | 2 | 0 | 2 | 5 |
| DDL | 105 | 9 | 19 | 19 | 21 | 32 |
| PUSHDOWN | 0 | 0 | 9 | 0 | 0 | 0 |
| WRITE | 107 | 11 | 10 | 2 | 24 | 24 |
| READ | 73 | 12 | 24 | 3 | 16 | 18 |
| PROC | 54 | 9 | 31 | 0 | 15 | 30 |
| EDGE | 15 | 3 | 2 | 0 | 0 | 1 |
| V3-LINEAGE | 12 | 2 | 0 | 0 | 0 | 0 |
| FUNCTIONS | 0 | 0 | 15 | 0 | 0 | 0 |
| VIEWS | 1 | 0 | 0 | 3 | 5 | 1 |
| TYPES | 16 | 0 | 7 | 0 | 4 | 5 |
| CATALOG | 6 | 0 | 7 | 1 | 0 | 4 |

**Totals:** 842 cells.

| verdict | cells |
|---|---|
| EQUAL | 424 |
| DIFFERENT | 57 (including RePark accepting what Spark refuses) |
| REFUSED-UNREGISTERED | 126 |
| NOT-PARSED | 28 |
| REFUSED-REGISTERED | 87 |
| SPARK-CANNOT | 120 |

**Evidence** is durable under `/tmp/oc-worker/nc-inventory/`, i.e. `~/repark-lanes/campaign/oc-worker/nc-inventory/`:
- `matrix.json` and `matrix.md`: every cell, with both engines' answers.
- `slate.json`: the unit slate.
- `out/` and `out2/`: raw observations from the two runs.
- `xeng-*.json`: the cross-engine runs.
- `stability.json`.
- `jar/`, `procedure-params.txt`, `tableprops.txt`: the enumeration.
- `harness.py` and `cells_*.py`: the replayable probes.
- `briefs/IPI-*.md`: oracle briefs for the eleven top units (IPI-01 to -07, -19, -52, -55, -56), each with the cell code and both engines' recorded answers.

**Not re-measured.** Main moved to `3e6a172c` (RP-31) after measurement. IPI-15 and IPI-34 are marked "held" and should be re-measured on the bump.

## 2. Per-PR table

| PR | what | tier | rounds | reviewer | state |
|---|---|---|---|---|---|
| #713 | `docs(ice-parity-inventory)`: the inventory doc + map row | orchestrator (docs only) | 2 commits (`d27eb0b7`, then deepened to `5acf65df`) | Sonnet read-only critic: 0 artefacts among 21 silent units; 2 P1s folded in | **MERGED 22:58 `e6ec5531`, tree-equal** (driver `merge-713b`, try 1); guards clean: docs-links, map-sync, map-md, docs-compaction, manifest, typos 1.47.2, comment-ban 0 |

## 3. The top of the slate (full table in the doc, §2)

1. **IPI-01** (silent): reader options `versionAsOf` / `timestampAsOf` are ignored; RePark reads the current snapshot. Re-measured standalone.
2. **IPI-02** (silent): `TIMESTAMP AS OF <expression>` reads current; `TIMESTAMP AS OF <int>` is treated as milliseconds.
3. **IPI-03** (silent): the static-mode `INSERT OVERWRITE … PARTITION (col)`, and the `overwrite-mode=dynamic` option, overwrite the wrong partition set. This contradicts DML-1, which is marked FIXED.
4. **IPI-05** (silent): `spark.wap.id` / `spark.wap.branch` are ignored, so writes land on main; `publish_changes` is missing.
5. **IPI-52**: a partition column name that is not a valid Avro name produces a table no one can read back.
6. **IPI-04** (silent): `DROP NAMESPACE` on a non-empty namespace drops it and orphans its tables. Spark refuses on both InMemory and Hadoop catalogs, CASCADE included. Re-measured standalone.
7. **IPI-06 / IPI-07** (silent): the `output-spec-id` option is ignored; branch reads use the snapshot schema where Spark uses the table schema.
8. **Highest-usage refusals:**
   - IPI-19: schema evolution on write (DataFrame `mergeSchema`, `MERGE WITH SCHEMA EVOLUTION`);
   - IPI-56: DataFrame `mergeInto` rejects Spark's own qualifier form;
   - IPI-32: `USE catalog`, `SHOW CATALOGS`, `current_catalog()`, `REFRESH` / `CACHE TABLE`;
   - IPI-21: `DROP TABLE … PURGE` does not parse;
   - IPI-25: `REPLACE TABLE` does not parse;
   - IPI-22: incremental and changelog reads.

## 4. What the measurements overturned

- **DML-1 (FIXED).** It does not hold for `PARTITION (col)` in static mode (IPI-03). The re-rating's V2-24b "EQUAL on three doors" measured different shapes, so both statements are true.
- **ICE-WRITE-OPTIONS-1 (FIXED).** `output-spec-id` is still ignored.
- **V3-COV-4 is not v3-only.** The whole-file MoR DELETE writes a delete file on v2 as well.
- **CONF-WAP-1 / REF-3.** They describe WAP as "stores silently". In fact the write lands on main, which is a silent wrong result, not a storage quirk.
- **Two first readings were wrong.**
  - My first matrix (Spark `local[4]`) showed 88 WRITE cells DIFFERENT. It was file layout: 3 rows became 3 Spark files. Re-running with `local[1]` and separating layout keys from logical keys dropped this to 8.
  - My first procedure fixtures deleted whole files, so every compaction cell inherited IPI-08. Rebuilt with partial-file deletes.
- **The critic's P1s.**
  - Layout-only differences were hidden as EQUAL. They are now IPI-57: MERGE and UPDATE write 2–3 data files per commit where single-core Spark writes 1.
  - The codec cell could not see the codec. Parquet footers now show RePark ignores the `spark.sql.iceberg.compression-codec` session conf (folded into IPI-14); the table property and the writer option are EQUAL.

## 5. Rulings (G-2)

- **Q-23c-1.** Spark oracle catalog: Iceberg InMemoryCatalog for `sc`, since it serves views, rename and register the way RePark's memory catalog does. A HadoopCatalog `hc` covers cells that need files on disk (orphans, path loads, `register_table`, `add_files`, codec footers). Spark metadata JSON is read through `TableMetadataParser` because the InMemory FileIO keeps files in memory.
- **Q-23c-2.** Spark runs `local[1]` so logical snapshot summaries compare. File and delete-file counts are recorded as layout and do not decide a verdict alone; they surface as IPI-57.
- **Q-23c-3.** No re-measure on `3e6a172c`: the lane's rule is one cargo job. RP-31's two units are marked held.
- **Q-23c-4.** I dequeued #713 at 22:29 to deepen: pushdown, codec, lineage, interop, the critic. Requeued at 22:41.
- **Q-23c-5.** The critic ran on Sonnet through the Agent tool: never Opus, and no Grok spend.
- **Q-23c-6.** Early findings went to `claims.txt` (22:13 and 22:40), so 23a/23b can take them after their lists.

## 6. Owner questions (with recommendations) — also in the doc, §8

1. **Leniency** (IPI-18, IPI-53): RePark accepts statements Spark refuses. Recommendation: refuse the reader options Iceberg 1.11 removed (`snapshot-id`, `as-of-timestamp`, `tag`) with Spark's message; keep the rest lenient and document them.
2. **IPI-17:** Spark's `merge-schema` + `INSERT … VALUES` adds `col1..colN`. Recommendation: DECLARED, do not copy.
3. **IPI-04:** refuse `DROP NAMESPACE` on a non-empty namespace, CASCADE included, as Spark's Iceberg catalogs do.
4. **IPI-16:** stamp `owner` as Spark does.

## 7. STATUS.md

No edit. STATUS.md makes no claim these findings contradict. If the orchestrating session keeps a "Known correctness issues" line for silent Iceberg gaps, IPI-01, -02, -03, -04, -05 and -52 are its candidates.

## 8. Rust-first roll-call

No product code. The slate names each unit's likely home (fork, RePark Rust, parser); every home is Rust except the thin option and conf plumbing on the facade.

## 9. Machine, lanes and disk

- **Lane:** one, `/tmp/nc-build` (4.3 GB, with its release native — the only cargo job); removed after the merge. The harness replays under any lane's venv: `<lane>/.venv/bin/python harness.py --engine repark …`.
- **Spark runs:** all under the one-JVM lock, about 2–5 minutes per family.
- **Scratch:**
  - `/tmp/nc-critic` (4 MB) is removable after reading.
  - The warehouse dirs under `nc-inventory/wh/` are probe output (small) and are kept as evidence.
- **Live checkout:** untouched. Its `git status` matches session start, and the `spark-warehouse` in it dates from 2026-08-27.
