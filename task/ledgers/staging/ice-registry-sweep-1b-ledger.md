# Unit ledger — ICE-REGISTRY-SWEEP-1 part B · registry agrees with merged main

**Date:** 2026-09-18 · **Branch:** `docs/ice-registry-sweep-1b` · **Base:** `6cd9ee06` (origin/main at pickup)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** READING. Docs-only unit: no Rust, no Python committed, no cargo, no JVM, no build.
Every row state below was measured on merged main the hour it was written, with
`.venv/bin/python` (the release native built at `6cd9ee06`, repark 1.4.2).
Probe scripts live OUTSIDE the clone at `/tmp/oc-worker/lb-sweep/probes/`
(`sweep1b_a.py`, `sweep1b_a2.py`, `sweep1b_a3.py`, `sweep1b_b.py`) and are never
committed. Full probe outputs are quoted in §Measurements; raw logs at
`/tmp/oc-worker/lb-sweep/out_*.log`.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 2026-09-16 measured Iceberg rating
(`/tmp/oc-worker/ice-rating/report.md`) found rows and repository claims the
registry `docs/spark-sql-iceberg-parity.md` lacked or stated wrongly. Runs
19–22 fixed or declared most in their own PRs. This unit makes the registry
agree with merged main for the remaining twelve — runs
19–22 fixed or declared most in their own PRs. Merge state at pickup
(`git log origin/main`): ICE-NESTED-EVO-1 (`6cd9ee06`), ICE-WRITE-OPTIONS-1
(`4ee83522`, incl. ORC/Avro DECLARED), ICE-MIXED-CASE-1 (`f43d522d`),
ICE-V3-WRITE-DEFAULT-1 (`93606656`), ICE-SORTED-INSERT-1 (`3bd667e6`),
ICE-DYN-OVERWRITE-1 (`50740088`), ICE-PROMOTE-READ-1 (`433a8352`) all merged;
V3-MULTIARG-1 (#700) NOT merged (no match in `git log origin/main`).

**Not in this unit:** product code; `STATUS.md`; any Spark re-measurement (no
row needed Spark's answer re-derived — every row already carries its oracle);
push; PRs; `gh`.

## PROPOSITION LEDGER — ICE-REGISTRY-SWEEP-1B — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | V2-10d: `ICE-NESTED-EVO-1` / `ICE-NESTED-DDL-1` state the measured merged-main behavior; no edit needed. | `grep -n V2-10d` leaves only the historical mention; fresh probe of adopted Spark-evolved struct read + nested DDL write/read on merged main. | **PROVEN** | `V2-10d` occurs once, `docs/spark-sql-iceberg-parity.md:3454`, inside `ICE-NESTED-EVO-1`'s "Before the fix (rating row V2-10d)" history. Probe `sweep1b_a2.py` on the committed `st_add_v2` fixture at its canonical root: `SELECT id, s.a, s.b` → `[(1,1,None),(2,2,'y')]`, `WHERE s.b IS NULL` → `[1]`. Probe `sweep1b_a3.py`: nested CREATE + `ADD COLUMN s.c BIGINT` + named_struct INSERT + read → `(1,1,'x',7)`, DESCRIBE `struct<a:int,b:string,c:bigint>`. The array-column INSERT refusal is the known fork finding pinned in `test_ice_nested_evo_1.py` (`_FORK_WRITE_REASON`), not this row. |
| C-002 | V2-15: ORC/Avro writes refuse typed on both the option and the table property; the `ICE-WRITE-OPTIONS-ORC-AVRO` repark half gains the property clause. | Refusal class + message for `writeTo.option("write-format", orc|avro|ORC)` and for `write.format.default=orc` + INSERT; snapshot count unchanged; before/after lines of the row. | **PROVEN** | Probe `sweep1b_a.py item2`: orc, avro, ORC each → `ERR UnsupportedOperationException: … write-format "orc" has no RePark Iceberg writer … (ICE-WRITE-OPTIONS-1 ORC/AVRO declared 2026-09-17)`; snapshots still 1. `TBLPROPERTIES ('write.format.default'='orc')` CREATE ok, INSERT → `ERR UnsupportedOperationException: FeatureUnsupported => File format orc is not supported for insert_into yet!`, `.files` empty — never a silent parquet file. Row extended at `docs/spark-sql-iceberg-parity.md:3033-3035` (was ending :3031). DECLARED 2026-09-17 kept. |
| C-003 | V2-24b: `DML-1B` says FIXED with pins; one measurement confirms it. | Dynamic PARTITION-less `INSERT OVERWRITE` replaces only the touched partition; summary carries `replace-partitions=true`. | **PROVEN** | Probe `sweep1b_a.py item3`: seed `(1,'a'),(2,'b'),(3,'c')`, conf dynamic, `INSERT OVERWRITE … SELECT 20,'b'` → rows `[(1,'a'),(20,'b'),(3,'c')]`; summary `replace-partitions=true, changed-partition-count=1, operation` overwrite-shaped. `DML-1B` (`docs/spark-sql-iceberg-parity.md:474`) already says FIXED 2026-09-17 with pins — no edit. |
| C-004 | V2-29: `snapshot-property.k` lands on `writeTo().option(…).append()`; `ICE-WRITE-OPTIONS-1` says FIXED — confirmed. | Option-carrying append commits; latest summary contains the stripped key. | **PROVEN** | Probe `sweep1b_a.py item4`: `.option("snapshot-property.sweep_k","sweep_v").append()` commits; summary contains `sweep_k=sweep_v` beside engine keys. Row `ICE-WRITE-OPTIONS-1` (`docs/spark-sql-iceberg-parity.md:2879`) already FIXED 2026-09-17 — no edit. |
| C-005 | V3-05: multi-argument `bucket(4, id, name)` refusal measured and recorded HERE, cross-referencing `V3-MULTIARG-1`; no registry row written (#700 not merged, would conflict). | Refusal class + message at CREATE and ALTER; single-arg control succeeds; `git log` shows #700 unmerged; no `V3-MULTIARG` string in the registry. | **PROVEN** | Probe `sweep1b_a.py item5`: CTAS `PARTITIONED BY (bucket(4, id, name))` → `ERR AnalysisException: … CTAS PARTITIONED BY \`bucket(…)\` expects (numBuckets, column), got 3 argument(s): [4, id, name]`; `ADD PARTITION FIELD bucket(4, id, name)` refuses the same way; single-arg `bucket(4, name)` control commits. `grep -c V3-MULTIARG docs/spark-sql-iceberg-parity.md` = 0; `git log origin/main` has no multiarg unit. State recorded in this ledger only; PR #700 (`V3-MULTIARG-1`) owns the row. |
| C-006 | ENC-1: the file is plain cleartext Parquet and no error is raised; row tightened to say so; DECLARED kept. | CREATE + INSERT + SELECT with `encryption.key-id` succeed; data file magic `PAR1`; before/after lines of the row. | **PROVEN** | Probe `sweep1b_a.py item6`: v3 CREATE with `encryption.key-id=k1` ok; INSERT ok; SELECT returns `(1,'secret')`; `.files` `file_format=PARQUET`; first bytes `PAR1`. Row `docs/spark-sql-iceberg-parity.md:879-881` now reads "ordinary unencrypted Parquet (byte-magic `PAR1`, readable with no key — measured 2026-09-18) with no error raised". DECLARED (owner 2026-08-24) kept; no product change. Owner question Q1 below records the shape-rule tension (silent cleartext vs LOUD-refusal shape) instead of changing code. |
| C-007 | variant / geometry / geography: CREATE refusals measured on v2 + v3 and the facade column refusal; `V3-GEO-1` / `V3-VARIANT-SHRED-1` match — no edit. | Refusal class + message naming the type on every measured surface. | **PROVEN** | Probe `sweep1b_b.py item7`: `(v VARIANT)` v2 + v3, `(g GEOMETRY)`, `(g GEOGRAPHY)` → `ERR UnsupportedOperationException: … column type \`VARIANT\|GEOMETRY\|GEOGRAPHY\` is not supported yet for Iceberg tables`; facade `StructType([StructField("g", GeometryType(4326))])` + `createDataFrame` → `ERR UnsupportedOperationException: repark does not support the geometry(4326) column type … (V3-GEO-1; FNP-16-geospatial)`. Rows at `docs/spark-sql-iceberg-parity.md:1654` (`V3-GEO-1`) and `:1684` (`V3-VARIANT-SHRED-1`) state exactly this — no edit. The SELECT-of-adopted-variant half stays on the fork pins the rows cite (no engine surface reaches a variant value). |
| C-008 | Puffin stats: no writer, absent-on-plain tables, carry-forward on adopted stats tables — the corrected `ICE-SPARK-TABLE-1` sentence verified; no edit. | `compute_table_stats` refusal; plain-table metadata keys absent; INSERT over synthetic stats entries carries them with the owning snapshot id. | **PROVEN** | Probe `sweep1b_b.py item8`: plain table newest metadata `statistics=None partition-statistics=None`; `CALL …compute_table_stats` → `ERR UnsupportedOperationException: … not supported. Supported procedures: apply_partitioning, cherrypick_snapshot, expire_snapshots, …`. Adopted copy carrying synthetic `statistics` + `partition-statistics` entries (owning snapshot id, stated as synthetic in §Measurements): INSERT commits a second snapshot and the new metadata carries both entries with the owning snapshot id (`00003-….metadata.json`). The rating's real-Spark carry-forward (`p_meta_stats`, `p_stats_expire`) is cited in the row; its warehouse is unusable on this box (manifest paths point at the dead `/tmp/ice-rate` root), so the commit-path machinery — the part the sentence claims — is what this probe measures. Sentence at `docs/spark-sql-iceberg-parity.md:8697` already corrected 2026-09-17 — no edit. |
| C-009 | C-2: the north-star v3-types row truly claims the write-default fill; measured on the committed fixture; row now cites the FIXED unit. | Adopted `defaults` table (column `c` write-default 5): seed rows carry 5; omitted-column INSERT fills 5; before/after of north-star line 62. | **PROVEN** | Probe `sweep1b_b.py item9` (fixture copied to its canonical `/tmp/repark-ice-v3-write-default-1` root under the test's lock protocol): seed `[(1,'a',5),(2,'b',5),(3,'c',5)]`; `INSERT INTO d (id,name) VALUES (11,'k')` → `(11,'k',5)`. North-star `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md:62` now reads "append fills an omitted column from a schema-carried `write_default` (FIXED 2026-09-18, ICE-V3-WRITE-DEFAULT-1 — registry `ICE-V3-WRITE-DEFAULT-1`)". |
| C-010 | C-7: a plain `INSERT INTO` into a `WRITE LOCALLY ORDERED BY id` table sorts and stamps; `V3-COV-5` + `WRITE-ORDER-SORTED-INSERT-1` exact — no edit. | 2000 descending ids → sorted file(s) with `sort_order_id` = the default order id. | **PROVEN** | Probe `sweep1b_b.py item10`: `ALTER TABLE … WRITE ORDERED BY (id)` then one 2000-row descending INSERT → `.files` `record_count=2000, sort_order_id=1`; parquet read: 1 file, head 0, `each_sorted=[True]`. `V3-COV-5` (`docs/spark-sql-iceberg-parity.md:5928`: "a declared default order sorts each writer's stream") and `WRITE-ORDER-SORTED-INSERT-1` (`:8556`, FIXED 2026-09-17 incl. round-3 lineage sort) already state this — no edit. RDF / COW-UPDATE residuals keep their own OPEN rows. |
| C-011 | C-10: range predicates on a promoted `int→bigint` / `float→double` column keep pre-promotion rows; `V3-COV-2` note + `ICE-PROMOTE-READ-1` verified — no edit. | Mixed-era table: `id < 2`, `id > 1`, `f < 2.0D`, `id = 1` answer Spark's rows. | **PROVEN** | Probe `sweep1b_b.py item11`: after promote + post file: `id < 2` → `[(1,'old1')]`; `id > 1` → `[(2,'old2'),(3,'new3')]`; `f < 2.0D` → `[(1.5,'old1')]`; `id = 1` → `[(1,'old1')]`. `V3-COV-2` note (`docs/spark-sql-iceberg-parity.md:1731`) and `ICE-PROMOTE-READ-1` (`:5718`, FIXED 2026-09-16) already state this — no edit. |
| C-012 | C-11: unquoted mixed-case resolves on the Spark door; twin references refuse `42704`; ID-1/ID-1a/ID-1b verified — no edit. | Backticked-`userId` table: unquoted SELECT + WHERE resolve; twin-frame `SELECT id` refuses with the Spark sentence. | **PROVEN** | Probe `sweep1b_b.py item12`: `SELECT userId, eventName … ORDER BY userId` → both rows with stored-case output names; `WHERE EVENTNAME='a'` → `[u1]`; `SELECT id FROM (SELECT 1 AS id, 2 AS \`ID\`)` → `ERR AnalysisException: … [AMBIGUOUS_REFERENCE] Reference \`id\` is ambiguous … SQLSTATE: 42704`. Rows `ID-1` (`:996`, FIXED on the Spark door), `ID-1a` (`:1053`), `ID-1b` (`:1117`) already state this — no edit. (`CREATE TEMP VIEW` with twins is unsupported on this path — orthogonal, pinned elsewhere.) |

VERDICT: 12 clauses, 12 PROVEN, 0 OPEN, 0 REJECTED.

## §Measurements — probe commands and outputs (abridged to the measured cells)

Probes: `/tmp/oc-worker/lb-sweep/probes/sweep1b_a.py` (items 1–6),
`sweep1b_a2.py` (item 1 adopted-read redo at the canonical fixture root),
`sweep1b_a3.py` (item 1 struct write/read),
`sweep1b_b.py` (items 7–12). Each run: `cd /tmp/lb-build &&
.venv/bin/python /tmp/oc-worker/lb-sweep/probes/<file> <items>`.
Raw logs: `/tmp/oc-worker/lb-sweep/out_a123.log`, `out_a2.log`, `out_a3.log`,
`out_a456.log`, `out_b78c.log`, `out_b8d.log`, `out_b8e.log`, `out_b912.log`, `out_b10.log`.

```text
[ITEM 1][adopt] OK rows=[{'current_snapshot_id': 7771008477944482480, 'total_records_count': 2, 'total_data_files_count': 2}]
[ITEM 1][evolved-struct-read] OK rows=[{'id': 1, 'sw_i1b.ns.t_nest.s[a]': 1, 'sw_i1b.ns.t_nest.s[b]': None}, {'id': 2, 'sw_i1b.ns.t_nest.s[a]': 2, 'sw_i1b.ns.t_nest.s[b]': 'y'}]
[ITEM 1][evolved-struct-star] OK rows=[{'id': 1, 's': {'a': 1, 'b': None}}, {'id': 2, 's': {'a': 2, 'b': 'y'}}]
[ITEM 1][evolved-leaf-filter] OK rows=[{'id': 1}]
[ITEM 1][struct-create] OK rows=[{}]
[ITEM 1][struct-add] OK rows=[{}]
[ITEM 1][struct-insert] OK rows=[{'count': 1}]
[ITEM 1][struct-read] OK rows=[{'id': 1, 'sw_i1c.ns.t_s.s[a]': 1, 'sw_i1c.ns.t_s.s[b]': 'x', 'sw_i1c.ns.t_s.s[c]': 7}]
[ITEM 1][describe] OK rows=[{'col_name': 'id', ...}, {'col_name': 's', 'data_type': 'struct<a:int,b:string,c:bigint>', ...}]
[ITEM 2][write-format-orc] ERR UnsupportedOperationException: This feature is not implemented: write-format "orc" has no RePark Iceberg writer — only parquet is written (ICE-WRITE-OPTIONS-1 ORC/AVRO declared 2026-09-17)
[ITEM 2][write-format-avro] ERR UnsupportedOperationException: (same shape for "avro")
[ITEM 2][write-format-ORC] ERR UnsupportedOperationException: (same shape for "ORC")
[ITEM 2][snapshots-after] OK rows=[{'n': 1}]
[ITEM 2][tblprop-insert] ERR UnsupportedOperationException: FeatureUnsupported => File format orc is not supported for insert_into yet!
[ITEM 2][tblprop-files] OK rows=[]
[ITEM 3][rows-after-dyn] OK rows=[{'id': 1, 'p': 'a'}, {'id': 20, 'p': 'b'}, {'id': 3, 'p': 'c'}]
[ITEM 3][summary-op] summary includes ('replace-partitions', 'true'), ('changed-partition-count', '1'), ('deleted-records', '1'), ('added-records', '1')
[ITEM 4][opt-append] OK committed
[ITEM 4][summary] summary includes ('sweep_k', 'sweep_v') beside engine keys
[ITEM 5][ctas-multi-bucket] ERR AnalysisException: Error during planning: CTAS PARTITIONED BY `bucket(…)` expects (numBuckets, column), got 3 argument(s): [4, id, name]
[ITEM 5][alter-multi-bucket] ERR AnalysisException: Error during planning: Error during planning: PARTITION FIELD `bucket(…)` expects (numBuckets, column), got 3 argument(s): [4, id, name]
[ITEM 5][single-bucket-control] OK rows=[{}]
[ITEM 6][create] OK rows=[{}]
[ITEM 6][insert] OK rows=[{'count': 1}]
[ITEM 6][read] OK rows=[{'id': 1, 'name': 'secret'}]
[ITEM 6][files] OK rows=[{'file_format': 'PARQUET'}]
[ITEM 6][magic] first-bytes=['PAR1'] (PAR1=plain parquet)
[ITEM 7][create-variant-v2/v3, create-geometry-v2, create-geography-v2] ERR UnsupportedOperationException: … column type `VARIANT|GEOMETRY|GEOGRAPHY` is not supported yet for Iceberg tables
[ITEM 7][facade-geometry-col] ERR UnsupportedOperationException: repark does not support the geometry(4326) column type … (V3-GEO-1; FNP-16-geospatial)
[ITEM 8][plain-stats-keys] statistics=None partition-statistics=None
[ITEM 8][compute_table_stats] ERR UnsupportedOperationException: … CALL system.compute_table_stats is not supported. Supported procedures: apply_partitioning, cherrypick_snapshot, expire_snapshots, …
[ITEM 8][synth-ins] OK rows=[{'count': 1}]
[ITEM 8][carried] newest committed metadata carries statistics + partition-statistics with the owning snapshot id (verified by direct metadata read: 2 snapshots, both entries present)
[ITEM 9][seed-rows] OK rows=[{'id': 1, 'name': 'a', 'c': 5}, {'id': 2, 'name': 'b', 'c': 5}, {'id': 3, 'name': 'c', 'c': 5}]
[ITEM 9][rows-after] OK rows=[…, {'id': 11, 'name': 'k', 'c': 5}]
[ITEM 10][files] OK rows=[{'record_count': 2000, 'sort_order_id': 1}]
[ITEM 10][sorted] files=1 heads=[0] each_sorted=[True]
[ITEM 11][range-lt] OK rows=[{'id': 1, 'name': 'old1'}]
[ITEM 11][range-gt] OK rows=[{'id': 2, 'name': 'old2'}, {'id': 3, 'name': 'new3'}]
[ITEM 11][range-float] OK rows=[{'f': 1.5, 'name': 'old1'}]
[ITEM 11][eq] OK rows=[{'id': 1, 'name': 'old1'}]
[ITEM 12][unquoted-select] OK rows=[{'userId': 'u1', 'eventName': 'a'}, {'userId': 'u2', 'eventName': 'b'}]
[ITEM 12][unquoted-where] OK rows=[{'userId': 'u1'}]
[ITEM 12][twin-ref] ERR AnalysisException: … [AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`t`.`id`, `t`.`id`]. SQLSTATE: 42704
```

Item-8 synthetic-entry note: the adopted table's metadata was hand-extended
with `statistics` / `partition-statistics` entries (paths
`metadata/synth.stats`, `metadata/synth-part.parquet`, owning snapshot id)
because the rating's real Spark-stats warehouse
(`/tmp/oc-worker/ice-rating/scratch/wh/p_meta_stats/…`) is unusable on this
box — its manifest-list and data paths are absolute to the dead
`/tmp/ice-rate/.scratch` root. What is claimed in the registry is the
commit-path carry machinery (entries preserved across INSERT and expire
commits), which the synthetic entries exercise exactly; no claim is made here
about reading a real Puffin blob.

Item-1 array note: `INSERT … array(named_struct(…))` into a list-of-struct
column refuses in the fork writer (`column types must match schema types …
PARQUET:field_id`) — the `_FORK_WRITE_REASON` finding pinned in
`test_ice_nested_evo_1.py`, outside this row's scope.

## §Gates

- `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/lb-build origin/main` →
  `comment-ban hits=0`, exit 0 (this unit touches Markdown only, probes live
  outside the clone).
- No cargo / make / facade suite per the brief RULE 2 (docs-only lane; the
  release native in `.venv` is the measurement instrument, not a build).

## Owner questions

- Q1 (ENC-1 shape rule): the rating calls `encryption.key-id` "stored, never
  applied, silently cleartext" — CREATE and INSERT succeed with no error and no
  warning, and the bytes are plain Parquet. The row is kept DECLARED under the
  owner ruling 2026-08-24 as explicitly documented behaviour, and the pin holds
  it so a later encryption landing reds it. If the shape rule is read strictly
  (a DECLARED row must describe a LOUD refusal), this row is in tension with it.
  Options: (a) keep DECLARED as documented behaviour (this unit's lean — the
  ruling already excludes it from the v1.0 gate); (b) add a CREATE-time loud
  warning; (c) refuse `encryption.key-id` outright. No product code changed;
  ruling asked, not assumed.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-registry-sweep-1b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against the rating row or §9 claim plus a fresh merged-main probe whose command and output are quoted in §Measurements. The three edited rows carry only the measured fact plus the dated tag; the nine verify-only rows are byte-identical in git diff.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, task/ledgers/staging/ice-registry-sweep-1b-ledger.md]
    - id: AT-2
      status: N/A
      justification: Docs-only sweep unit. No code path, no input, no boundary to exercise.
    - id: AT-3
      status: N/A
      justification: Docs-only sweep unit. No runtime behavior, no failure path, no retry.
    - id: AT-4
      status: N/A
      justification: Docs-only sweep unit. No shared state, no ordering, no concurrency.
    - id: AT-5
      status: N/A
      justification: Docs-only sweep unit. No privileged action, no secret, no AWS call, no JVM.
    - id: AT-6
      status: ATTACKED
      evidence: Row ids kept on every touched row; dispositions changed nowhere (three tightenings, nine verifications, one deliberate non-row for V3-05 owned by PR #700). Compatibility is the diff itself: two registry repark-halves, one north-star cell, the ledger, and three map entries.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-7
      status: N/A
      justification: Docs-only sweep unit. No growth, loop, or leak surface.
    - id: AT-8
      status: ATTACKED
      evidence: No state copied from a doc, report, or PR description: every clause cites the probe command and its output on merged main (base 6cd9ee06). The two approximations are labeled where they stand (item-8 synthetic stats entries with the dead-warehouse reason; item-7 SELECT half on the rows' fork pins).
      artifacts: [task/ledgers/staging/ice-registry-sweep-1b-ledger.md]
    - id: AT-9
      status: N/A
      justification: Docs-only sweep unit. Provenance travels in the rows: dated measured tags on all three edits.
    - id: AT-10
      status: ATTACKED
      evidence: Every clause carries before/after line numbers checkable by git diff; the comment-ban gate output is quoted in §Gates.
      artifacts: [task/ledgers/staging/ice-registry-sweep-1b-ledger.md]
  complete: true
```
