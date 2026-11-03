# CORPUS-1: remaining-work estimate (2026-11-03)

CORPUS-1 output 3, first half. The hours rest on one measured pilot, Hadoop metadata discovery,
run on 2026-11-03 against RePark main `63bbb1b6` with the Spark 4.1.2 oracle (Iceberg 1.11.0).
The inventories are the `*-inventory-2026-11-03.md` files beside this one; the pilot is
[hadoop-discovery-rule-2026-11-03.md](hadoop-discovery-rule-2026-11-03.md) and
[hadoop-discovery-cells-2026-11-03.jsonl](hadoop-discovery-cells-2026-11-03.jsonl).

## What the pilot cost, by fixture kind

All hours are one agent lane's wall clock, measured from the lane's own timestamps. They are not
human hours. The pilot reused an existing pattern (the audits' `spark_util.py` session and the
AUD-015 compare script), so a family without such a pattern costs more; the multipliers below
say how much.

| Fixture kind | Pilot work | Lane hours | Machine time | Cells |
|---|---|---|---|---|
| Reading and porting the rule | `HadoopTableOperations`, the parser codec, the catalog listing, the hadoop tests | 0.15 | none | none |
| Constructed table | seven seed writers (four Spark, three RePark), one script each side | 0.05 | 13 s | the 1890 fixtures' sources |
| Metadata manipulation | one generator over writer x name form x cleanup x hint, the Spark and RePark cell runners, a Spark judge for writes | 0.35 | 476 s Spark, 53 s RePark, 137 s judge | 3820 |
| SQL only | catalog-door controls on 21 fixtures, both RePark doors | 0.03 | inside the runs above | 84 |
| Rule check | a model of the ported rule run against every Spark answer | 0.05 | 2 s | agrees on 1910 of 1910 |
| Triage | classification with the AUD-015 classes, grouping by axis, card lookup | 0.15 | none | 3904 |
| **Total** | | **0.78** | **about 11 minutes** | **3904** |

The cost is per family and per fixture kind, not per cell: once the generator exists a further
axis value is a line, and the machine time grows by about one second per Spark cell.

## Per family

| Family | Cells to build (by fixture kind) | Basis | Lane hours | Date range |
|---|---|---|---|---|
| 1 Discovery | 0 to finish the pilot; about 150 constructed for the metadata tables over a path read (`TestIcebergSourceHadoopTables`) | pilot x 1 | 0.5 to 1 | 2026-11-04 |
| 2 Names | about 1130 SQL only: the double-quoted-identifiers config on (about 570), `IDENTIFIER()` (about 240), SHOW TABLES, SHOW TABLE EXTENDED, DESCRIBE, ALTER VIEW heads (about 220), global temp views and two-part catalog names (about 100) | AUD-015 harness exists; SQL only x 1.5 | 1.5 to 3 | 2026-11-04 to 2026-11-05 |
| 3 Nested | about 1150 constructed (6 types x 8 access operations x 3 formats x 8 evolution steps) and about 50 expression evaluation over literal maps and structs | no harness; constructed x 3 | 3 to 6 | 2026-11-05 to 2026-11-07 |
| 4 Delete files | about 384 constructed on Parquet (4 delete kinds x 6 evolution steps x 8 key types x 2 doors); about 384 EXCLUDED Avro/ORC cells recorded as follow-ups | fork delete suites exist; constructed x 3 | 2.5 to 5 | 2026-11-06 to 2026-11-09 |
| 5 Commit outcome | about 144 constructed with fault injection (12 heads x 6 failure points x 2 doors) | Rust fault harness exists (`post_publish_outcome.rs`); each round is a crate build | 2.5 to 5 | 2026-11-07 to 2026-11-11 |
| **All** | about 3000 cells beyond the pilot | | **10 to 20** | **2026-11-04 to 2026-11-12** |

The range assumes one lane at a time (owner ruling 2026-11-02), one Spark JVM on the box, and
that no family stops for an owner ruling. Each family's own pilot is the first hour of its row;
if a family's first fixture kind costs more than three times the pilot's, its row is re-estimated
before the rest is built.

## Defects the pilot's cells expose

Severity follows the AUD-015 legend: S1 data-deleting or wrong stored values, S2 other state
changes or wrong reads, S3 refusal text and over-refusals. The pilot found no panic and no S1.
Groups are assigned in this order; a cell counts in the first group that matches.

| Group | Cells | Class (AUD-015) | Severity | Existing card | Note |
|---|---|---|---|---|---|
| D1 a catalog-style `NNNNN-<uuid>.metadata.json` wins the no-hint listing and a deleted row comes back | 84 | ANSWER_DIFFERS | S2 (wrong read) | HADOOP-HINT-CATALOG-STYLE-NAME-1 (AUD-017; ledger clause C-081 rules it not a label blocker) | every writer, every cleanup state, hints missing, empty, non-numeric, beyond `int` |
| D3 a negative hint lists where Java refuses | 126 | REPARK_ACTS_SPARK_REFUSES_NOOP | S2 | HADOOP-HINT-NEGATIVE-1 | 105 cells answer the newest rows; 21 cells, with a catalog-style file present, answer a deleted row (D1's mechanism) |
| D4 the codec is chosen from the bytes, not the name | 504 | 388 REPARK_ACTS_SPARK_REFUSES_NOOP, 116 REFUSE_TEXT | S2 and S3 | ledger lead C-012 of ICE-HADOOP-GZIP-METADATA-1 (open) | only fixtures whose name lies about the payload; no Java writer produces one |
| D2 an unreadable hint, or a hint that is a directory, refuses where Java lists | 252 | 168 REPARK_REFUSES_SPARK_ACTS, 84 REFUSE_TEXT | S3 | HADOOP-HINT-UNREADABLE-FALLBACK-1 (AUD-018; C-082) | the directory shape is new to this pilot and has the same mechanism |
| D5 a zero hint: Spark `TABLE_OR_VIEW_NOT_FOUND`, RePark names `v0.metadata.json` | 84 | REFUSE_TEXT | S3 | none: card candidate | both refuse |
| D6 a hinted version with no file: Java's `Metadata file for version %d is missing under %s` against RePark's own text | 148 | REFUSE_TEXT | S3 | none: card candidate | both refuse |
| D7 a path append to an existing hadoop table | 1764 | 944 REPARK_REFUSES_SPARK_ACTS, 820 REFUSE_TEXT | S3 | ICE-WRITE-DF-SAVE-1 states the refusal | Spark appends on 964 fixtures; proposed as a label exclusion |
| D8 the `type=hadoop` catalog does not read a table already on disk (both RePark doors) | 84 | REPARK_REFUSES_SPARK_ACTS | S3 | HADOOP-FACADE-FRESH-SESSION-1 (recorded design) | proposed as a label exclusion |

Two seed observations are not cells: a RePark write under
`write.metadata.compression-codec=gzip` keeps plain `v<N>.metadata.json` names, where Java writes
`v<N>.gz.metadata.json` (card candidate, S3: Java reads the plain names, so the read stays
equal); and the RePark writer under `write.metadata.delete-after-commit.enabled` keeps every
version (AUD-014, ICE-HADOOP-METADATA-CLEANUP-1, confirmed).

Twenty-five cells are controls (`severity: control`): fixtures that keep Hadoop's stale checksum
sidecar, where Spark's hint read fails and lists. They reproduce the artefact the parity matrix
records against the second audit's cleanup case and are not defects.

## Defects the inventories already expose

Open findings the inventories map to rows, not re-measured here: AUD-011 (native-door CTAS,
TRUNCATE and ALTER report failure after committing; NATIVE-DOOR-POST-PUBLISH-1, S2), AUD-013
(Avro/ORC maintenance residue, excluded with Avro/ORC delete files), AUD-014 (above). The
inventory gaps (no tracked post-publish pin for ALTER or TRUNCATE on the Spark door, the
`_deleted` metadata column, the `position_deletes` table, `IDENTIFIER()`) are unmeasured, not
defects, until a cell measures them.
