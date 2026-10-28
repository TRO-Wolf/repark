# META-TABLE-UNQUOTED-READ-1: an unquoted current-snapshot metadata-table read fails to parse on the Rust door (P3 card)

**Filed:** 2026-10-27. **Filed by:** the RP-60 records clerk (`claude-haiku-5-5`), from the RP-60 verifier's round-1 measurement (V-RP60-2) at `f4f753f8` and at the base pin `d1304a46`. **Status:** filed, not dispatched. The owner charters it.

**Grade:** not assigned. The first step is a measurement; the fix follows the owner's charter.

**Severity:** P3. The quoted spelling and the `VERSION AS OF` spelling answer. Only the unquoted current-snapshot spelling fails to parse, on the in-crate `ReparkSession` with `SparkDialect` and `SparkExtension`.

**Why:** the metadata-table rewrite in `crates/repark-spark/src/metadata_tables.rs` writes the table as a bare `t$suffix` word through `dollar_ident_token`. The verifier's reading, not yet proven: the Databricks parser dialect of `SparkExtension` (FNP-4b, 2026-09-15) splits the `$suffix` off at the reparse, so the statement ends before the suffix.

**Measured (RP-60 verifier, round 1, 2026-10-27):** through the in-crate entry, `SELECT count(*) FROM cat.ns.t.delete_files` fails with `ParserError("Expected: end of statement, found: $delete_files at Line: 1, Column: 30")`. `.data_files`, `.files` and `.position_deletes` fail the same way, and `SELECT summary FROM cat.ns.t.snapshots` fails at Column 29. The spellings that answer: `` SELECT count(*) FROM `cat`.`ns`.`t`.`delete_files` `` (1 row), `` cat.ns.`t`.delete_files `` (1 row), and `cat.ns.t.delete_files VERSION AS OF 1` (0 rows). The failure is the same at both pins, so it is not a regression of RP-60. The MT-1 pin `metadata_tables_asof.rs` is green.

**Open question (first step):** whether the Python facade's unquoted current-snapshot read answers. Not measured in round 1.

**Fix as specified:** the `t$meta` token reparse under the Databricks dialect, so the unquoted read parses to the same relation as the quoted one.

**Pin:** the unquoted current-snapshot read answers the same rows as the quoted read. The pin runs on the in-crate door, and on the facade too if the first step shows the facade is affected.

**Out of scope:** the `VERSION AS OF` spellings, which answer; the `all_*` tables, whose refusal is registry row MT-1; the time-travel read path.

**Dispatch:** none. The owner charters it or declines it. Linked from the registry row MT-1 and from [the RP-60 card](rp-60-posdel-repin-1-card-2026-10-27.md).

Up: [map.md](map.md).
