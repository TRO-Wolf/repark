# Iceberg format-v2 label: audited sign-off against the corpus (ICE-V2-LABEL-REAUDIT-1, 2026-11-05)

**Verdict: LABEL DOES NOT STAND.** Blocking findings: AUD-021 (S1), AUD-022 (S1), AUD-023 (S2), AUD-024 (S1, unless the owner rules it an oracle-class group), AUD-025 (S2).

Each blocker is a wrong write or a wrong read measured at main `dd9298b0` against Spark 4.1.2 with Iceberg 1.11.0. None of them is a panic. Two of them sit inside a stated exclusion or an owner ruling, but go past what that exclusion or ruling says it covers (AUD-021: names D2, the double-quoted-identifiers configuration; AUD-022 and AUD-023: owner ruling 1, the D10 and D13 expression answers). The gate statement decides these: "A panic, a wrong read or a wrong write blocks the label whether or not the inventory listed it; cards are for refusal-text, over-refusal and refuse-with-hint classes only."

The coverage check holds. Every count the label cites matches its cells file or ledger row, with five precision notes (AUD-029), and every cited pin runs green at head. The AUD-015 and AUD-016 fixes hold under re-probe: the same-named-temporary-view heads all refuse or act on the view, and the struct-key subscript answers as Spark with no panic.

- Auditor: Claude Opus 5.5 (`claude-opus-5-5`), read-only. No product file was changed, and nothing was committed or pushed.
- Clone: a scratch clone at `dd9298b0`, clean tree.
- Build: `uv sync --locked --extra record --extra polars --no-install-package repark`, then `uvx maturin@1.14.1 develop`, with `CARGO_BUILD_JOBS=8` under `taskset -c 0-25` and `systemd-run -p MemoryMax=40G`.
- Probe interpreter: the scratch clone's own virtual environment, which holds pyspark 4.1.2 and the head RePark wheel. A second scratch environment was ruled out because its RePark wheel was built at an older head.
- Spark JVMs ran one at a time with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, using the runtime jar `iceberg-spark-runtime-4.1_2.13-1.11.0`.
- Cells file: `docs/corpus/reaudit-cells-2026-11-05.jsonl`. It has 450 cells in the AUD-015 schema, plus a `family` field and an optional `note`.

## 1. Coverage check per claim

Counts were measured with `python3 -I` over the jsonl files on main. Pins were run with `cargo test --locked` (and pytest) at head; the pin logs are held by the audit and not committed.

| Claim (label section) | Cited | Measured | Pin status at head |
|---|---|---|---|
| F1 cells file composition | 3904: 1910 path reads, 1910 path appends, 84 catalog-door, 42 native | 3904: `path_select` 1910, `path_insert` 1910, catalog door 84 (42 facade plus 42 native) | n/a. The 42 native cells are inside the 84 (AUD-029a). |
| F1 D1 catalog-style name never in the no-hint listing | 84 of 84 EQUAL (post-fix, ledger) | cells file: 84 `path_select` ANSWER_DIFFERS (pre-fix column, as the label says). Ledger C-001: "84 of 84 EQUAL". | `repark-core` `iceberg_path::` 103 passed (`corpus_cells_tests::path_read_cells_match_the_spark_column`, `discovery_rule_tests::*`) |
| F1 D3/D6 missing-version refusals | 126 and 148 | ledger C-002: "D3: 126 cells; D6: 148 cells; all 274 refuse on both sides" | same run, green |
| F1 D2 fall-to-listing | 168 of 252 | ledger C-003: "168 of 252 EQUAL; the other 84 are name-lying" | green |
| F1 CRLF hint | pin `crlf_hint_parses_and_walks_from_the_hinted_version` | present in `discovery_rule_tests.rs` | green |
| F1 controls | 818, 40 (15), D4 504, D5 84, D7 1764, D8 84; total 3904 | these add to 3904; ledger C-005 matches | green |
| F2 cells file composition | 2362: 1196 facade, 678 native | `view` False 1196, native 678, True 488 (view pass) | n/a |
| F2 view wins: 166 view-pass cells EQUAL or EQUAL_REFUSE (66 DROP, 24 INSERT, 14 ALTER, 42 reads) | 166 | 166 (EQUAL 100, EQUAL_REFUSE 66). DROP TABLE 66, INSERT 24, ALTER 14, SELECT 22 + 20 = 42; the other 20 are DROP VIEW 8, CREATE TABLE 4, ANALYZE 4, DESCRIBE 4 | AUD-015 pins: see section 1a |
| F2 columnresolution-views 1, 3 to 8 EQUAL; 2 is D10 | as stated | facade: as stated. Native door: every statement refuses (AUD-029d). | n/a |
| F2 SHOW TABLE EXTENDED 15 to 21 EQUAL_REFUSE | 15 to 21 | facade 15 to 21 EQUAL_REFUSE on both catalogs | n/a |
| F3 composition | 2973: 2304 A-read, 173 A-ddl, 384 A-write, 112 B-expr; Parquet 1325, Avro 768, ORC 768 | exact; the 112 B-expr cells carry no format | n/a |
| F3 no panic | 0 in 2973 | 0 | n/a |
| F3 full-row reads 280 of 280 | 280 | 280 cells (4 types × 2 heads × 2 formats × 7 steps × 2 doors, plus 56 writer-axis cells): EQUAL 270, EQUAL_REFUSE 6, REFUSE_TEXT 4 | AUD-029b |
| F3 MAP-KEY-ALTER-1 refusals | C-001, C-002, C-004 | probe `map_key_field_rename_refused` refuses on the facade with Spark's body. The field ids differ (Spark 5, 6; RePark 4, 5) because the two engines assign ids at CREATE in a different order. | `repark-spark tests::map_key_` 17 passed; `repark-sql --test alter_map_key` 6 passed |
| F3 SUBSCRIPT-DOT-ACCESS-1 | Spark door answers | probes `map_subscript_then_field`, `subscript_then_field` answer on the facade (the second answers null where Spark raises, which is owner ruling 1) | `repark-spark tests::subscript_dot_access` 6 passed; python `test_sql_door_from_csv_dot_access_cells`: see section 1a |
| F4 composition | 6464: 2880, 1232, 1152, 672, 528 | exact | n/a |
| F4 2736 of 2880 A-read EQUAL; 144 O2 | 2736 / 144 | 2736 EQUAL; 144 REPARK_ACTS_SPARK_REFUSES_NOOP, all with Spark's "Job aborted" (O2) | n/a |
| F4 writer axis: 1152, none S1 or S2; non-equal = D2, D3, O2 | as stated | 1152 cells, no S1 or S2. The non-equal cells include 32 `eqw_boolean` ANSWER_DIFFERS that the rule files under **O1**, not O2 (AUD-029c). | n/a |
| F4 456 of 528 A-catalog | 456; D1 72 | 456 EQUAL, 72 REPARK_REFUSES_SPARK_ACTS S3 | n/a |
| F5 0 of 182 Spark-door FAILURE_REPORTED_COMMITTED | 0 | 0 (6 FAILURE_REPORTED_COMMITTED_ON_REOPEN are D2, fixed) | n/a |
| F5 84 post-publish Spark-door cells, 81 EQUAL, 3 DROP leftovers | 84/81/3 | listing, load and refresh after publish: 84; EQUAL 81; D3 3 | n/a |
| F5 staged publish D2 12 of 12 EQUAL after PR #1261 | 12 | cells file: 12 D2 (pre-fix). Ledger: "12 of 12 on both doors"; commit-rule dated line: re-measured at `20389129` | `repark-spark --test staged_publish_cleanup` 10 passed; `repark-sql --test staged_publish_cleanup` 10 passed |
| F5 exclusions D1 32, D3 8, R1 13, G1 12 | as stated | D1 32 (native), D3 8 (5 Spark-door, 3 native), R1 13, G1 12 | n/a |

### 1a. The AUD-015, AUD-016 and RP-65 fix pins at head

All of these are green at `dd9298b0`:

| Pin | Fix | Result |
|---|---|---|
| `python/repark/tests/sql/test_ident_dialect_1.py` and `test_ident_dialect_doors.py` (the AUD-015 cells replayed at head), `functions/test_map_struct_key_subscript_1..3.py`, `iceberg/test_ice_views_1.py`, `functions/test_fnp_gen_1_s34_critic.py::test_sql_door_from_csv_dot_access_cells` | IDENT-DIALECT-1/1B, MAP-STRUCT-KEY-SUBSCRIPT-1, SPARK-DOOR-TEMP-VIEW-DDL-1, SUBSCRIPT-DOT-ACCESS-1 C-004 | 1409 passed (`pin-py.log`) |
| `repark-sql --test map_struct_key_subscript --test map_struct_key_subscript_2` | AUD-016 (#1232) | 29 + 13 passed |
| `repark-spark --lib tests::temp_view_alter` | AUD-015 (#1233) | 14 passed |
| `repark-core --test temp_view_doors` | AUD-015 (#1233) | 10 passed |
| `repark-sql --test idless_toplevel_door`; `python/repark/tests/iceberg/test_ice_idless_toplevel_1.py` | RP-65 (#1246) | 9 passed; 2 passed |

These replays of the AUD-015 table are the post-fix re-measure that open item 6 asks for. They replay the cells file's recorded Spark column against RePark at head for the unit's spellings. The probes in section 2 add new view-shadow heads (DELETE, UPDATE, MERGE, TRUNCATE, INSERT OVERWRITE, DROP PURGE, RENAME, CREATE and CTAS) measured on a live Spark. None of them is a wrong write.

## 2. Probes outside the inventory, inside the five families

There are 450 cells. A probe is one scenario. Each probe runs on Spark 4.1.2 and on RePark through the facade, and where the door can reach the table, through the native door too. "Native-path" means a `read_iceberg_path` view over the table Spark wrote, because the native door cannot parse Spark's Iceberg DDL. For writes, a Spark path read of the table RePark wrote is also recorded (`spark-path-read-of-repark-write`).

| Family | Probes (in-family + exclusion + ruling) | Cells | Classes (after normalising the facade's dict representation of structs) |
|---|---|---|---|
| 1 discovery | 21 + 16 | 70 | EQUAL 40, REFUSE_TEXT 18, REPARK_REFUSES_SPARK_ACTS 10 (D7/D8 exclusions), REPARK_ACTS_SPARK_REFUSES_NOOP 2 (C-008 leniency, as stated) |
| 2 names | 24 + 28 (incl. ruling 3) | 104 | REPARK_REFUSES_SPARK_ACTS 52, REFUSE_TEXT 21, EQUAL 14, ANSWER_DIFFERS 8, EQUAL_REFUSE 5, REPARK_ACTS_SPARK_REFUSES_NOOP 2, REPARK_ACTS_SPARK_REFUSES_CHANGED 2 |
| 3 nested | 18 + 9 + 8 (ruling 1) | 125 | EQUAL 48, REPARK_REFUSES_SPARK_ACTS 32, REFUSE_TEXT 24, ANSWER_DIFFERS 8, REPARK_ACTS_SPARK_REFUSES_NOOP 8, REPARK_ACTS_SPARK_REFUSES_CHANGED 4, EQUAL_REFUSE 1 |
| 4 deletes | 24 + 7 | 123 | EQUAL 100, ANSWER_DIFFERS 15, REPARK_REFUSES_SPARK_ACTS 4, REFUSE_TEXT 2, EQUAL_REFUSE 1, REPARK_ACTS_SPARK_REFUSES_CHANGED 1 |
| 5 commit | 27 | 28 | EQUAL 13, REFUSE_TEXT 8, EQUAL_REFUSE 4, REPARK_REFUSES_SPARK_ACTS 1, REPARK_ACTS_SPARK_REFUSES_NOOP 2 (AUD-025) |

What was probed, by family:

- **Family 1, path read.**
  - Hints: leading zeros, a `+` sign, a multi-line hint, a UTF-8 BOM, trailing whitespace.
  - Version gaps (v3 or v2 removed) with hint 2, hint 4, hint 1 and no hint.
  - A corrupt newest metadata file under hint 4, hint 3 and no hint.
  - `.bak` and `.tmp` neighbours, an emptied metadata directory, a forward walk from hint 1, a trailing slash on the path, and the `snapshot-id` option.
  - All are EQUAL or refuse on both sides. Spark 4.1.2 refuses `snapshot-id` and RePark refuses time travel on a path read. Neither side reads stale data.
- **Family 2, names.**
  - The temporary-view-shadow heads, each against a same-named table: DELETE, UPDATE, MERGE, TRUNCATE, INSERT OVERWRITE, DROP PURGE, RENAME, a qualified DELETE, a back-quoted bare name, and CREATE, CTAS, IF NOT EXISTS and qualified CREATE.
  - CTE shadowing (select and insert), DROP VIEW and DROP VIEW IF EXISTS over a table, a temporary view over a table read after an INSERT, a mixed-case name, dotted and spaced back-quoted names, a two-part name after `USE hd`, `system.session`, and SHOW TABLES with a view.
  - No wrong write in this group. Every view-shadow DML refuses or acts on the view, and the table is untouched.
- **Family 3, nested.**
  - Evolution: a map-value struct add, an inner-struct rename, drop then re-add of the same name, a nested int to bigint promotion, a list-element struct add, a reorder (FIRST), and a filter on a renamed nested field.
  - Writes: a misordered `named_struct` insert, a nested UPDATE, a MERGE that sets a struct, empty and null collections, and a list of maps of structs.
  - Reads: null-struct projection, the map struct-key subscript (AUD-016), the subscript-then-field forms, a map-value promote under a struct key, and a map-key rename.
- **Family 4, deletes, on merge-on-read v2 Parquet.**
  - Partitioned tables (identity and bucket): delete, whole-partition delete, an update that moves a row's partition, and a full MERGE.
  - Successive deletes then an update, a no-op delete, null semantics (`NOT (k = 30)`, `<>`, `<=>`), MERGE cardinality, copy-on-write, a v3 deletion-vector double delete (path read only; RePark does not create v3 by default), delete all, an IN-subquery delete, and NaN, `-0.0` and Infinity predicates.
- **Family 5, commit.**
  - The REPLACE TABLE and RENAME TO spellings, CTAS into an existing table, CREATE IF NOT EXISTS on an existing table, and failed INSERT statements (cast, NOT NULL, overflow).
  - Failed UPDATE, DELETE and MERGE, a failed RTAS, a failed CTAS on a new table, DROP then recreate, and DROP with and without PURGE.
  - Static, dynamic and PARTITION-clause overwrite; DataFrame `writeTo().append()`, `overwritePartitions()` and `create()` on an existing table; `insertInto` with an arity mismatch; TRUNCATE; ADD COLUMN; and a format-version 3 upgrade.
  - Apart from AUD-025, every failure commits nothing on either side.

## 3. The exclusions table

Two or more probes per row, unless the row says why none ran.

| Exclusion | Probes | Result |
|---|---|---|
| Avro and ORC delete files | `avro_delete_file_read`, `orc_delete_file_read` | Spark wrote Avro and ORC position deletes. RePark read them EQUAL on both doors, and its own delete on those tables reads back EQUAL in Spark. No wrong value. The case is outside the label and is not a refusal, but nothing is wrong in it. |
| D8 `type=hadoop` catalog door over a foreign table | SELECT, SHOW TABLES, INSERT, DELETE, on facade and native (8) | All refuse loudly (`table not found`, `SCHEMA_NOT_FOUND`, `NamespaceNotFound`). No file changed. |
| D7 path append to an existing hadoop table | `mode(append)`, `mode(overwrite)` | Both refuse `TABLE_OR_VIEW_NOT_FOUND`. No file changed. |
| D4 name-lying metadata | v4 holding v2's payload (hint 4), v4 holding v3's payload (no hint) | EQUAL. Spark reads the payload too. No state change. |
| C-012 non-ASCII hint digits | Arabic-Indic `٤`, fullwidth `４` | EQUAL. Java's `Integer.parseInt` reads both, so this is parity, not a leniency (AUD-029e). |
| C-008 leading-integer listing | catalog-style names only, with no hint and with hint 4 | No hint: RePark reads where Spark says not found, which is the stated leniency, with no state change. Hint 4: both refuse. See AUD-025 for a RePark writer that produces such a directory. |
| D5 zero hint without v0 | `0`, `0\r\n` | Both refuse. Refusal text only. |
| C-013 unlistable metadata directory | mode 0300 with no hint, and with hint 4 | No hint: both refuse, with different texts. Hint 4: EQUAL. |
| `IDENTIFIER()` | SELECT, INSERT, DELETE through a shadowing view | All refuse loudly. The table is unchanged. |
| Owner ruling 3: statement 39 CREATE TABLE in the view pass | statement 39 under `USE hd.ns`; plain CREATE, CTAS, IF NOT EXISTS and qualified CREATE with a same-named temporary view; the session-catalog spellings | Measured on the `hd` catalog, Spark 4.1.2 itself creates the table when a same-named temporary view exists, in all four spellings, and RePark matches it file for file. The four corpus cells compare the golden file's session-catalog answer (`TABLE_OR_VIEW_ALREADY_EXISTS` on `spark_catalog.default`) with RePark on `hd`. Nothing changes beyond the stated limit. |
| `doubleQuotedIdentifiers` accepted and not honoured (names D2, D3) | 12 probes | **Not a refusal: AUD-021.** D3 (`AS "x"` with the configuration off) is accepted with no state change, as stated. |
| Global temporary views | CREATE, read | Both refuse loudly. |
| CREATE VIEW persisting on hadoop (D9) | CREATE VIEW, then SELECT | The view persists and reads, as stated (a card). No table metadata is touched. |
| One-part CREATE and DROP SCHEMA | CREATE SCHEMA, DROP SCHEMA | The facade refuses both loudly. The native door accepts CREATE SCHEMA with no Iceberg state change. |
| Session variables and EXECUTE IMMEDIATE | DECLARE; EXECUTE IMMEDIATE with INSERT; EXECUTE IMMEDIATE with DELETE | All refuse. No write. |
| ORC nested reads | SELECT *, SELECT s.a, DELETE WHERE s.a = 1 | All refuse loudly (`FeatureUnsupported`). The DELETE commits nothing, and a Spark read shows the table unchanged. |
| Empty-struct reads on Avro | a struct emptied by DROP COLUMN, read on the facade and native-path | Refuses loudly (`DataInvalid`). |
| `STRUCT<>` DDL and literals | `STRUCT<>` in CREATE; `named_struct()` | Both refuse. No table is created. |
| Native-door semantics D5 and D6 | `l[1]`, `element_at(m, 'k')` on native-path | **Not a refusal.** `l[1]` answers the first element and `element_at` answers a list (AUD-026; outside the label's door scope). |
| Native field access after subscript (L1) | `l[0].a`, `m['k'].b` on native-path | Refuses loudly ("Dot access not supported"). |
| Native post-commit reporting (D1), kill at publish (G1), native INSERT OVERWRITE refusal (R1) | none re-probed | The commit fault harness (`FaultCatalog`) is untracked, and the native door cannot address facade-created tables. Coverage is the cells file plus the `staged_publish_cleanup` pins, which are green on both doors. |
| DROP TABLE leftovers (commit D3) | DROP; DROP PURGE; DROP then recreate | RePark leaves the data files (Spark removes the directory), as stated. A recreated table reads empty, with no resurrected rows. |
| Deletes D1 (`_deleted` over a dropped equality key), D3 (MERGE after an equality-key promote) | D3 neighbour only (position deletes, promote, MERGE delete): EQUAL | Equality deletes need the corpus's constructed-table harness. No Spark SQL path writes them. Not re-probed. |
| Deletes D2 BINARY key in a DML predicate | DELETE, UPDATE | Both refuse loudly, and a Spark read shows the table unchanged. |
| Deletes D4 identifier-field drop | one and two identifier fields | Both refuse. Refusal text only. |
| Owner ruling 1 (D10, D13 expression answers) | 8 probes | The expression answers match the stated limit. **Past that limit, the same expressions write: AUD-022 and AUD-023.** `map(NULL, 1)` refuses on both sides. |
| Owner ruling 2 (RP-65 resolved C-085) | the RP-65 pins | Green: `idless_toplevel_door` 9 passed, `test_ice_idless_toplevel_1.py` 2 passed. The owner ruling closes open item 7. No new probe reached the id-less top-level shape, because Spark SQL does not write id-less files. |

## 4. Findings

Severity follows the AUD-015 legend: S1 is data-deleting or wrong stored values; S2 is other state changes or wrong reads; S3 is refusal text and over-refusals; a panic is a blocker regardless.

All repro commands run from the audit's scratch `probes/` directory, which is not committed. `probes/repro.py` reproduces the RePark side of AUD-021, AUD-022, AUD-024 and AUD-025 in one run: `python3 -I probes/repro.py` with the scratch clone's interpreter. `bash probes/runset.sh <set>` reruns a whole family's three phases: Spark, RePark, then a Spark read of RePark's writes. For families 1 and 2 the drivers are `f1.py`, `f1x.py`, `f2.py` and `f2b.py`.

### AUD-021: S1, blocks the label

**Statement.** `SET spark.sql.ansi.doubleQuotedIdentifiers=true` is accepted and not honoured, and the DML that follows writes wrong data. With the configuration on, Spark reads `"s"` as the column `s`. RePark reads it as the string literal `'s'`.

**Spark answer** (table `t(id INT, s STRING)` with rows `(1,'a'), (2,'s'), (3,'b')`):

- `DELETE FROM t WHERE "s" = 's'` deletes row 2.
- `UPDATE t SET id = 10 WHERE "s" = 'a'` updates row 1.
- `INSERT INTO t SELECT id + 10, "s" FROM t` copies `a, s, b`.
- `INSERT INTO t VALUES (4, "x")` refuses `UNRESOLVED_COLUMN`.
- `SELECT "id" FROM t` answers `1, 2, 3`.
- `WHERE "s" = 'a'` answers row 1.

**RePark answer (facade):**

- The DELETE **deletes every row**.
- The UPDATE changes nothing.
- The INSERT stores the literal `'s'` three times.
- `VALUES (4, "x")` stores `'x'`.
- `SELECT "id"` answers `'id'` three times.
- `WHERE "s" = 'a'` answers no rows.

Some forms refuse loudly: `WHERE "id" = 2` on an INT column (a cast error), and the qualified `"t"."s"`.

**Why it blocks.** The label's exclusion row says "A card, not a wrong write" because the corpus cells for this configuration touched no table data. The measured probes do touch table data: wrong writes and wrong reads. The gate statement blocks these whether the inventory listed them or not.

**Reproduce.** `bash probes/runset.sh f2b` (driver `f2b.py`), or `repro.py` line A.

### AUD-022: S1, blocks the label

**Statement.** An out-of-range array subscript inside DML writes. Owner ruling 1 covers the expression answer (null where Spark raises under ANSI). It does not cover what that null does inside a statement that writes.

**Spark answer:** `DELETE FROM t WHERE l[10] IS NULL`, `UPDATE t SET id = id + 100 WHERE l[5] IS NULL` and `INSERT INTO t SELECT id + 10, array(l[7]), m FROM t` each raise `INVALID_ARRAY_INDEX`, and nothing changes.

**RePark answer (facade):** the DELETE removes **every row** (it answers count 2, and Spark's read of the table is empty), the UPDATE rewrites every row, and the INSERT stores `[null]` arrays. A Spark path read confirms each change.

**Reproduce.** `bash probes/runset.sh f3set`; `repro.py` line B.

### AUD-023: S2, blocks the label

**Statement.** A duplicate-field struct lookup in an INSERT source writes a row. This is the D13 class from owner ruling 1, past its stated limit.

**Spark answer:** `INSERT INTO t SELECT 8, named_struct('a', named_struct('a', 1, 'a', 2).a, 'b', 'd')` refuses `AMBIGUOUS_REFERENCE_TO_FIELDS`.

**RePark answer (facade):** it stores row 8 with `a = 1`, and a Spark path read shows the row. It is a write Spark refuses, using a value taken from an ambiguous reference.

**Reproduce.** `bash probes/runset.sh f3set` (head `D13_duplicate_field_insert`).

### AUD-024: S1, blocks unless the owner rules it oracle-class

**Statement.** NaN in DML predicates on Iceberg tables. On Spark 4.1.2 with Iceberg 1.11, a pushed filter excludes NaN from `d > 0` and from `NOT (d < 2.0)`. Spark's own SQL semantics agree with RePark (NaN is largest): `CAST('NaN' AS DOUBLE) > 0` is true on a VALUES relation and on a projection of the same table. The exclusion comes only from the Iceberg scan filter, which appears in the plan as `IcebergScan filters=d > 0.0` (`nanchk.log`).

**Spark answer** (on Spark's own Iceberg table):

- `SELECT id ... WHERE d > 0` answers `1, 5` (Infinity), without the NaN row.
- `DELETE WHERE d > 0` keeps NaN, under merge-on-read and under copy-on-write.
- `UPDATE ... WHERE d > 0` leaves NaN.
- `DELETE WHERE d >= CAST('NaN' AS DOUBLE)` refuses: "Cannot create expression literal from NaN".

**RePark answer:**

- The SELECT includes NaN (S2).
- DELETE and UPDATE delete or update the NaN row that Spark keeps, under merge-on-read and copy-on-write (S1). A Spark path read confirms it.
- `d >= NaN` deletes the NaN row where Spark refuses.

**Why it blocks.** Family 4 claims that RePark's DELETE, UPDATE and MERGE "write deletes that Spark reads with the same survivors". The survivors differ here.

**Owner decision needed.** This is the deletes rule's O1/O2 shape: the oracle disagrees with Spark's own semantics. The owner can file it as an oracle group (O3) or as a parity fix. Until then it blocks.

**Reproduce.** `bash probes/runset.sh f4bset`; `probes/nanchk.py`; `repro.py` line C.

### AUD-025: S2, blocks the label

**Statement.** A CTAS whose query fails at execution, creating a new table on the facade's `type=hadoop` catalog, leaves `<table>/metadata/00000-<uuid>.metadata.json`.

**Spark answer:** Spark's failed CTAS leaves no table directory. Spark's path read of RePark's leftover location refuses `TABLE_OR_VIEW_NOT_FOUND`.

**RePark answer:** the path read answers an empty table with the CTAS schema on both doors. It reaches the file through the C-008 leading-integer listing, so a reader takes a table that never committed for a committed one. The catalog door of a new session does not see it.

**Why it blocks.** Family 5's staged-publish claim covers failures at publish time only. This failure happens before publish, and its leftover is read as a table. Separately, the C-008 leniency's premise ("no Java or RePark writer produces such names" holds for D4) does not hold for this RePark writer.

**Reproduce.** `bash probes/runset.sh f5set` (head `ctas_failing_select_new`), then `probes/ctasleft.py`; `repro.py` line D.

### AUD-026: S2, does not block (outside the label's door scope)

**Statement.** The native-door exclusions D5 and D6 answer different values silently instead of refusing. On a native path view, `l[1]` answers `10` (1-based) where Spark's 0-based answer for the same row is `20`, and `element_at(m, 'k')` answers `[1]` (a list) where Spark answers `1`.

**Why it does not block.** The label's scope line names the Spark 4.1.2 door and the facade, not the native door. The owner ruled these DataFusion semantics outside the parity claim on 2026-11-03. This finding records that they are silent values, not refusals.

**Reproduce.** `bash probes/runset.sh f3set` (native-path rows).

### AUD-027: S3, card, does not block

**Statement.** A map column with struct keys cannot be collected into Python on either door. `SELECT *` raises `TypeError: unhashable type: 'dict'`, where Spark answers. The error is loud. These map-with-struct-key columns (`mapkey` in the nested corpus) are outside the 280-cell claim. The subscript itself answers as Spark, so AUD-016 holds.

### AUD-028: S3, cards, do not block

These are over-refusals. Each is loud, and none changes state:

- `DROP VIEW IF EXISTS t` over a same-named table refuses `VIEW_NOT_FOUND`, where Spark does nothing.
- `WITH t AS (...) INSERT INTO ...` refuses, where Spark inserts the CTE rows.
- A DELETE with an IN-subquery refuses (a deliberate guard).
- An INSERT of `array(map('k', ...), map())` refuses on array coercion.
- `CREATE TABLE ... TBLPROPERTIES ('format-version'='3')` and `SET TBLPROPERTIES ('format-version'='3')` refuse unless `repark.sql.allowCreateFormatVersion3` is set.
- Many failure refusals carry DataFusion text where Spark names `CAST_INVALID_INPUT`, `NOT_NULL_ASSERT_VIOLATION`, `CAST_OVERFLOW_IN_TABLE_INSERT` or `INSERT_COLUMN_ARITY_MISMATCH`.

### AUD-029: S3, record precision, does not block

- **a.** The family-1 composition reads as if the 42 native cells were additional. They are inside the 84 catalog-door cells.
- **b.** "280 of 280" is 270 EQUAL, 6 EQUAL_REFUSE and 4 REFUSE_TEXT. The 4 are native list `drop`/`rename` column cells with a different refusal text.
- **c.** The writer-axis non-equal cells also include O1: 32 `eqw_boolean` cells, where RePark keeps the row deleted and Spark resurrects it.
- **d.** The columnresolution-views claim holds on the facade only. The native door refuses statements 1 to 17.
- **e.** C-012 is parity, not a leniency.
- **f.** Facade DML answers a count row, and DDL answers one empty row, where Spark answers no rows. This is the names D14 class, and it reaches past INSERT to DELETE, UPDATE, MERGE and DDL.

## 5. Not covered

- The commit fault points D1, G1 and R1 were not re-run, because the harness is untracked.
- Equality-delete fixtures (deletes D1, D3) were not re-run; they need the constructed-table harness.
- Concurrent writers through one hadoop location were not probed. RePark's `type=hadoop` is in-process (D8), so no second process can reach the same catalog state.
- Native-door DDL is unreachable for Spark's Iceberg DDL. The native door was probed through path views only.

Cells file: `docs/corpus/reaudit-cells-2026-11-05.jsonl`. Drivers, held by the audit and not committed: the scratch `probes/` directory (`rh.py`, `gen.py`, `runset.sh`, `f1.py`, `f1x.py`, `f2.py`, `f2b.py`, `f3set.py`, `f4set.py`, `f4bset.py`, `f5set.py`, `nanchk.py`, `ctasleft.py`, `repro.py`).
