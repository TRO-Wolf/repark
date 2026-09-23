# The v1.5.0 remainder — every open gate cell, specified

**Filed:** 2026-09-23 by the orchestrating session on the owner's steer of 09-22 ("close out, then spec
what's left for 1.5"). **Source:** the gate scoreboard of 2026-09-23 05:0x on main `88b6f59f`
(`/tmp/oc-worker/scoreboard/2026-09-23/matrix.json`, recorded Spark 4.1.2 + Iceberg 1.11 legs, RePark legs
re-run on a release build). Every Spark answer quoted below is the recorded one; every RePark divergence is
what that run observed. Nothing here is a design ruling — where a unit needs one, the table says whose.

## 0. The numbers

| | 09-21 | 09-22 | **09-23** |
|---|---|---|---|
| main | `6cff128f` | `8de204f6` | **`88b6f59f`** |
| EQUAL | 537 | 573 | **607** |
| SPARK-CANNOT (Spark itself fails; not in the gate) | 117 | 117 | 121 |
| gate cells (842 − SPARK-CANNOT − 3 streaming, carve-out C-1) | 722 | 722 | **718** |
| **EQUAL / gate** | 74.4 % | 79.4 % | **84.5 %** |
| open (in the gate) | 185 | 149 | **111** |
| regressions vs the previous morning | 0 | 0 | **1** (REG-1 below) |

09-22 → 09-23: +35 cells EQUAL, 4 moved to SPARK-CANNOT (the legacy reader options, #800 — Spark 4.1.2
itself refuses them and RePark now refuses identically), 1 regression, 1 shift to a truer verdict
(`V-SHOW-TBLPROPERTIES` NOT-PARSED → REFUSED-REGISTERED after views PR1).

**REG-1 — `R-TT-TIMESTAMP-AS-OF-EPOCH`** (`SELECT * FROM t TIMESTAMP AS OF <epoch seconds>`): EQUAL on 09-21
and 09-22, DIFFERENT on 09-23 and again on a second READ-only run the same morning — Spark answers the
three rows of the snapshot at that second, RePark answers the two rows of the snapshot after it. A
deterministic reproduction, so a regression from one of 09-22's merges that touched snapshot resolution
(#802 metadata tables AS OF is the first suspect, #804 branch-read schema the second). **First task of the
next READ lane: bisect on the harness, then fix; no other unit touches the time-travel resolver until it is
green.**

## 1. The units

Tier: `opus` = Opus 5.5 high executor (owner 09-22), `terra` = GPT-5.6 Terra ≤ max, `muse` = Muse 1.3 max,
`devin` = Devin SWE-2. Size: S ≤ half a day-lane, M = a day-lane, L = more than one. "Ruling" names who must
decide something before the unit can close: **none**, **orch** (the orchestrating session, logged), **owner**.

### U0 — already in flight (22 cells): land what is open, nothing new to design

| cells | where it is | next step |
|---|---|---|
| `P-ORPHAN-*` (10) | repark#807 (narrowed guard + `file_list_view`), critic r1 findings in hand | remediate, replay on the lane head, queue. After #807: `YOUNG` EQUAL; the other 9 need U1 (path layout) |
| `R-DF-LOAD-META`, `R-DF-LOAD-META-FILES-VERSIONASOF` | repark#805, critic r1 (V-001..V-003 test-side) | remediate, queue |
| `R-MT-DESCRIBE` | repark#806 | critic, queue |
| `V-DESCRIBE`, `V-SHOW-TBLPROPERTIES`, `V-ALTER-UNSET`, `D-VIEW-ALTER-PROPS` | views PR2 (`xb-views2`, DESCRIBE), PR3 (`xb-views3`, ALTER VIEW), PR4 (`xb-views4`, SHOW TBLPROPERTIES) — built, measured, stacked on the merged PR1 | rebase PR2 onto main, add the V-009 red-first pin (`Expr::Struct` / `Expr::Named` in the walker battery), gate, critic, queue; then PR3, PR4 |
| `R-MT-FILES` | fork #345 merged (position-delete files carry no field counts) | RP-48 pin bump (work order drafted, `wo/rp48-pin.md`) |
| `R-MC-ROW-ID-V3`, `L-INSERT-OVERWRITE` | fork #346 merged (session-driven commit-order hook); RePark `java_bucket_order` slice relocated into its own module per Q-55-5, on `xo55-rid` | finish the relocation round, RP-48, then the N ≥ 6 determinism proof on both cells (ruling Q-10-1) |
| `V-SHOW-CREATE`, `D-TEMP-VIEW` | views PR5 / PR6 — not started | after PR4; Spark legs recorded |

### U1 — MEM-LAYOUT: the memory catalog's table layout (ruling **owner** to confirm Q-55-7; 14 cells fully or partly)

Spark's hadoop catalog puts a table at `<warehouse>/<ns>/<t>` and names metadata files `v<N>.metadata.json`;
RePark's memory catalog puts every table under `<root>/repark_ctas/<catalog>/<ns>/<t>` with uuid-named
metadata files. Cells that print a location or a metadata file name cannot read EQUAL until the layout
matches: the 8 orphan listing cells and `P-ORPHAN-FILE-LIST-VIEW`, `P-TABLE-STATS-*` (3), `P-PART-STATS-*`
(2), `P-RTP-DEFAULT` (Spark: `v4.metadata.json` — the versioned name is the hadoop layout, not a harness
artefact). Design: default create location `<warehouse>/<ns>/<t>`; the CTAS fallback root only for the
case #198/#217 built it for; versioned metadata names + `version-hint.text` for hadoop-style catalogs.
List every cell that prints a location before and after. **Size M, tier opus.** Flagged to the owner on
09-22: it moves where a memory-catalog table's files land (ephemeral catalog, nothing persisted).

### U2 — PROC-NORMALISE: harness artefacts, not code (ruling **owner**; 5 cells + the file-name halves of 6)

- `P-ANCESTORS-OF`, `P-ANCESTORS-OF-ID`, `P-POS-ANCESTORS`: same columns, same `S0..Sn` order, same row
  count; the only difference is the commit wall-clock in epoch ms (`1789784958797` vs `1790154916441`).
- `P-CHERRYPICK-WAP`, `P-PUBLISH-CHANGES`: the harness labels RePark's snapshot ids `S3` but the Spark leg was
  recorded raw (`8904112443147844626`); behaviour is the same (xo-muse10's replay, 09-22).
- `P-TABLE-STATS-*`, `P-PART-STATS-*`: after U1 the remaining difference is the random uuid in the statistics
  file name, which Spark's own run cannot reproduce either.

Proposal: `overrides.json` rules that compare a timestamp-valued column by monotonicity and count, a
snapshot-id column by label after labelling both legs, and a statistics-file column by shape
(`<prefix>/<snapshot>-<uuid>.stats`). Each override cites the cell's measured rows. **Size S, no code; the
owner rules because the parity goal reads "zero non-EQUAL" and an override is a declared equivalence.**

### U3 — PD-BATTERY: the nine pushdown batteries (ruling **none**; 9 cells)

Each `PD-*` cell runs 41 predicates over one partition transform; the matrix says "no silent predicate
difference — every predicate both engines answer returns the same ids". RePark refuses two shapes:
`startswith()` as a filter function, and a decimal literal against a `DOUBLE` column holding NaN/Infinity
(`ICE-NAN-DECIMAL-LITERAL-1`; #786 widened decimal literals but this leg still refuses). Spark itself raises
`INTERNAL_ERROR` on `cat IN (…)` over the identity-partitioned MoR table — that sub-predicate becomes
SPARK-CANNOT per cell, the rest must be EQUAL. **Size S, tier devin or muse.** Closes 9 cells for one
function and one literal rule.

### U4 — DDL-SHOW-DESCRIBE: the inspection statements (ruling **orch**; 10 cells)

| cell | Spark answers | RePark today |
|---|---|---|
| `D-SHOW-CREATE`, `D-SHOW-CREATE-PLAIN` | the CREATE statement with `USING iceberg`, `PARTITIONED BY`, `LOCATION`, `TBLPROPERTIES` | refused |
| `D-SHOW-TBLPROPERTIES`, `-KEY` | `[key, value]` rows; the key form one row; a missing key is a row `Table … does not have property: k` | refused (`DBT-TBLPROPS-1` registered) |
| `D-SHOW-TABLE-EXTENDED` | `[namespace, tableName, isTemporary, information]` | refused |
| `D-DESCRIBE-COLUMN`, `-PLAIN` | `[info_name, info_value]`: `col_name`, `data_type`, `comment` | not parsed |
| `D-DESCRIBE-EXTENDED` | key rows `# Partition Information` / `# col_name` / `Owner` / `Table Properties` … | RePark prints `# Partitioning` / `Part 0` and omits `Owner`; measured key list in the matrix diff |
| `D-DESCRIBE-VERSION` | **refuses** `DESCRIBE t VERSION AS OF` | accepts — must refuse with Spark's class |
| `D-CREATE-DEFAULT-PROPS` | stamps `owner=<os user>` beside the codec | stamps the codec only |

The registered rows `DBT-TBLPROPERTIES-1` / `DBT-COLCOMMENT-1` were dbt-era declarations, not owner
carve-outs — they retire with the unit. **Size M, tier opus (the DESCRIBE EXTENDED row grammar is a design
read of Spark's `DescribeTableExec`), terra for the parser halves.**

### U5 — DDL-ALTER-NESTED and the DDL long tail (ruling **orch**, one **owner** item; 14 cells)

- Parser: `D-ALTER-TYPE-NESTED` (`ALTER COLUMN st.a TYPE bigint`), `-ARRAY-ELEM` (`arr.element`),
  `-MAP-VALUE` (`m.value`), `D-UNSET-PROPS-IF-EXISTS`, `D-NS-ALTER-PROPS` (`SET DBPROPERTIES`) — five
  NOT-PARSED shapes, all in the sqlparser dialect layer (packet 9's method).
- Semantics: `D-ADD-COL-STRUCT`, `D-X-ADD-COL-MAP-KEY-STRUCT` (nested ADD COLUMN through the fork's schema
  update), `D-ALTER-COMMENT` (`DBT-COLCOMMENT-1` retires), `D-CREATE-V1` (format-version 1 create; the fork
  writes v1 already for RP-4x), `D-CREATE-PART-TRUNC-BIN` (`truncate(1, b)` on BINARY), `D-WRITE-ORDERED-TRANSFORM`
  (`V3-COV-5` registered: `WRITE ORDERED BY bucket(4, id), days(ts)` — the fork's transform sort orders landed
  with 25d, the DDL half is missing), `D-REF-BRANCH-ON-EMPTY` (CREATE BRANCH before any snapshot: Spark answers,
  RePark refuses), `D-X-PARTITIONED-COLDEF` (hive-style `PARTITIONED BY (cat STRING)`: Spark **refuses** on
  Iceberg — match the class), `D-RENAME-TABLE` (Spark **refuses** `RENAME TO` on the hadoop catalog; RePark
  renames — match the refusal for hadoop-type catalogs, keep the rename for the others, measured per catalog).
- Owner item: `D-NS-NESTED` (`NS-2`): nested namespaces `sc.a.b` + `SHOW NAMESPACES IN sc.a` — Spark answers on
  the hadoop catalog; the registered row says RePark's catalogs are flat. **Owner decides whether nested
  namespaces enter 1.5.0 or carve out with a date.**

**Size L as one unit, or S (parser) + M (semantics) as two, tier terra (parser) + opus (semantics).**

### U6 — WRITE-REFUSALS: RePark accepts what Spark refuses (ruling **owner** on one; 7 cells)

`W-MERGE-SCHEMA-EVOLUTION`, `-PROP`, `-PROP-BIGINT`, `W-DF-OPT-MERGE-SCHEMA`, `W-DF-OPT-MERGE-SCHEMA-ICEBERG`,
`W-ACCEPT-ANY-INSERT-VALUES`, `TP-ACCEPT-ANY-SCHEMA-DF`: in every one Spark 4.1.2 + Iceberg 1.11 **refuses**
(the recorded leg is an error) and RePark performs the schema evolution. #761 (IPI-19/56/37) narrowed EX-DF-9
and left these. The fix is refusal parity — the same error class and message shape — not the removal of the
capability behind a flag. `TP-ACCEPT-ANY-SCHEMA-DF` is on the owner's open list since 09-20; the other six
are the same rule. `W-INSERT-MERGE-SCHEMA-CONF` is the one that Spark answers: with merge-schema on, the SQL
INSERT lands the new columns **first** (`[null, null, null, 1, "a", "x"]`) — a column-order rule to measure
and match. **Size S–M, tier muse.**

### U7 — WRITE-DF: the DataFrame writer shapes (ruling **none**; 7 cells)

| cell | Spark answers | RePark today |
|---|---|---|
| `W-DF-SAVE-NAME`, `W-DF-SAVE-OVERWRITE-NAME` | `format('iceberg').save('cat.ns.t')` treats the path as a table name | refused |
| `W-DF-SAVEASTABLE-OVERWRITE` | **replaces** the table: a fresh table, snapshot history reset | overwrites in place (`deleted-data-files 1` in the second snapshot) |
| `W-DF-V2-OPTION-BRANCH` | `writeTo.option('branch', …).append` | refused (`EX-W2-3`) |
| `W-DF-V2-OVERWRITE-COND-PART`, `-ROWS` | `writeTo.overwrite(col('cat') == 'x')`, row-level on unpartitioned | refused (`EX-W2-1`) |
| `W-DF-V1-BUCKETBY-ERR` | refuses `bucketBy` with Spark's class | refused with another (`IO-BUCKET-1`) |
| `W-DF-OPT-OUTPUT-SPEC-ID` | the write lands under the **old** spec (`specs` column) | lands under the current spec |

**Size M, tier opus (the v2 writer overwrite-by-filter is a design read), devin for the two name forms.**

### U8 — WRITE-SQL: the SQL write long tail (ruling **none**; 5 cells)

`W-INSERT-PARTITION-CLAUSE` (`INSERT INTO … PARTITION (cat='q') SELECT`), `W-INSERT-OVERWRITE-WHERE`
(`INSERT INTO … REPLACE WHERE`, Spark 4 syntax, NOT-PARSED), `W-INSERT-BUCKETED` (INSERT into a
bucket-partitioned table refuses — the hash-distribution write path), `W-MERGE-NESTED` (MERGE updating a
struct field), `W-UPDATE-NESTED-FIELD` (`UPDATE SET st.a = …`). The two nested ones share the struct-field
assignment plan with U5's nested ALTER. **Size M, tier terra (parser) + opus.**

### U9 — TYPES (ruling **owner** on two; 7 cells)

`TY-TIMESTAMP-NTZ`, `TY-TIMESTAMP-NTZ-V3` (`TZ-6` registered — the owner's TZ-9 question is the same
family), `TY-TIMESTAMP-LTZ`, `TY-MAP` (`map<string,int>` end to end — read, write, DESCRIBE), `TY-VARIANT-V3`
(`V3-VARIANT-SHRED-1` registered: the fork has no variant writer — fork work), `TY-UNKNOWN-VOID` (the v3
`unknown` type), `TY-UUID-READ` (a uuid column created through the Iceberg API reads as string in Spark).
**Owner decides TZ-6/TZ-9 (timestamp without zone semantics) and whether variant is 1.5.0 or a dated
carve-out; the other five are none.** **Size L, tier opus + fork lane.**

### U10 — READ-REST (ruling **none**; 5 cells)

REG-1 first (above). Then `R-MC-DELETED` (the `_deleted` metadata column = a scan mode that returns deleted
rows; packet 10's named hard step, fork D-1 third arm), `R-INPUT-FILE-NAME` (`input_file_name()` on an
Iceberg scan — `_file` exists, the function alias is missing), `R-DF-LOAD-PATH` and `R-DF-LOAD-METADATA-JSON`
(`spark.read.format('iceberg').load(<table location>)` and `.load(<metadata.json>)` — catalog-less loads
through the fork's static table). **Size M, tier opus.**

### U11 — PROPS, EDGE, CATALOG (ruling **owner** on three; 8 cells)

| cell | Spark answers | RePark today | ruling |
|---|---|---|---|
| `TP-MANIFEST-MIN-MERGE` | `commit.manifest.min-count-to-merge=2` leaves **1** manifest after the merge | 3 | none — fork: manifest merging on commit |
| `TP-DELETE-GRANULARITY-FILE` | `write.delete.granularity=file` writes **2** delete files for 2 touched data files | 1 | none — the file-scoped delete writer |
| `TP-FORMAT-V1-DELETE` | on a v1 table the CoW DELETE works and MoR is refused | refuses the CoW leg | none |
| `E-CASE-SELECT` | `SELECT ID` returns the column **as written** (`ID`), `Data` as `data` | the stored case | none — output column case follows the query |
| `E-CASE-PARTITION-FIELD` | **refuses** `ADD PARTITION FIELD` with an upper-case source column | accepts | none — match the refusal |
| `E-CATALOG-LISTDATABASES` | `listDatabases()` after a catalog-qualified call lists the **session** catalog (`[]`) | lists the Iceberg catalog (`["ns"]`) | none |
| `E-TZ-TIMESTAMP-AS-OF` | `TIMESTAMP AS OF` a wall-clock string under `America/New_York` | refused | **owner** (TZ-9) |
| `CAT-TYPE-MEMORY` | **refuses** `spark.sql.catalog.X.type=memory` (not a Java catalog type) | accepts — it is RePark's own catalog | **owner**: a deliberate RePark extension (dated carve-out) or refuse-with-alias |

**Size S–M each, tier muse; the two fork items on the fork lane.**

### U12 — S3-PATH-WRITE: plain Parquet, CSV and JSON path writes to `s3://` (ruling **owner, made 2026-09-23**; cells to be recorded)

Filed on 09-22 as a v1.5.1 card and moved into the 1.5.0 target by the owner on 09-23
([s3-path-write-1-5-0.md](s3-path-write-1-5-0.md)). Today `DataFrameWriter.parquet/csv/json` is a local
staging-and-rename protocol that fails on an `s3://` destination before the `COPY` runs, while reads and
Iceberg tables on S3 already work. Step 0 records the Spark oracle on the tier-2 scratch prefix (four save
modes × three formats, `partitionBy`, empty frames, `_SUCCESS`-only and foreign-object destinations,
`s3://` vs `s3a://`, what Spark leaves behind) as **`W-PATH-S3-*` cells that join the gate inventory** —
the only unit in this spec that grows the denominator. Then a `Session::write_path` seam in Rust owning
the save mode, staging and commit for every scheme (the local scheme bit-for-bit as today), the S3 commit
shape the oracle decides (direct `part-*` + `_SUCCESS` vs stage-copy-delete), save modes by LIST/DELETE,
the read side's credential and region path reused, a live leg in the tier-2 workflow under the scratch
prefix the role already grants (a bronze-style target is a separate IAM grant). **Size M, tier opus (the
seam) + devin (the Python writer's retreat to a forwarder); AWS spend for the oracle and the live leg
well under the $25 cap; the 3 GB table flag does not apply (no tables).**

### Carved out (3 cells, owner ruling C-1, 2026-09-19)

`R-STREAM-READ`, `R-STREAM-READ-SKIP`, `W-STREAM-WRITE-FILESRC` — structured streaming, v1.6.0
([ice-streaming-1-6.md](ice-streaming-1-6.md)). Not in the 718.

### Count

U0 22 · U1 (overlaps U0/U2; counted once under U0/U2) · U2 5 · U3 9 · U4 10 · U5 14 · U6 7 · U7 7 · U8 5 · U9 7
· U10 5 · U11 8 · plus the six stats/RTP cells that need both U1 and U2 = **111** on today's inventory;
U12 adds its `W-PATH-S3-*` cells to the inventory when step 0 records them.

## 2. The owner's decisions, collected

1. **Q-55-7** memory-catalog layout `<warehouse>/<ns>/<t>` + versioned metadata names (U1) — confirm or keep the layout and carry 14 cells as a dated residue.
2. **Harness overrides** for wall-clock, snapshot-label and statistics-file-name columns (U2) — declared equivalences under the zero-non-EQUAL goal.
3. **`D-NS-NESTED`** — nested namespaces in 1.5.0 or a dated carve-out.
4. **`TP-ACCEPT-ANY-SCHEMA-DF`** and the six refusal-parity siblings (U6) — refusal parity is the proposal.
5. **TZ-6 / TZ-9** — timestamp-without-zone semantics (`TY-TIMESTAMP-NTZ*`, `E-TZ-TIMESTAMP-AS-OF`).
6. **`TY-VARIANT-V3`** — fork variant writer in 1.5.0 or a dated carve-out.
7. **`CAT-TYPE-MEMORY`** — RePark's own catalog type stays as a dated extension, or refuses like Spark.

Decisions 3, 6 and 7 are the only ones that can *shrink* the gate; every other item is buildable. The owner's
09-23 decision on S3 path writes (U12) is the one that *grows* it.

## 3. Sequencing for the next run

Day 1 (four lanes, Opus 5.5 high orchestrators, `opus`/`terra`/`muse`/`devin` executors): **A** REG-1 then U0's
#805/#806 and U10 (READ, one lane); **B** U0's views PR2–PR6 (one lane); **C** #807 + U1 + U2's evidence for
the owner (one lane); **D** U3 + U4 (one lane). Day 2: U5, U6, U7, U8. Day 3: U9, U11, **U12 (S3 path writes —
the oracle recording first, on the tier-2 scratch prefix)** and the fork items (manifest merge,
file-granularity deletes, variant) on a fork lane. The owner's seven decisions gate nothing
on day 1 and everything with a name on days 2–3, so they are the morning-of-day-2 ask.

## Rules that bind every unit

The four-gate path (local gate zeros, critic PASS on the current head, CI green, comment gate clean,
tree-equal squash), measure before ruling, the comment ban on every actor, the file-size ceilings only
ratchet down, fork `Cargo.toml` edits wait for the owner, and the claims file is append-only.
