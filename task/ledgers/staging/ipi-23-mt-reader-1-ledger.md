# Charter ledger — IPI-23-MT-READER-1 · the DataFrame reader loads Iceberg metadata tables

**Date:** 2026-09-22 · **Branch:** `fix/ipi-23-reader-metadata-tables` · **Base:** `743f1be9` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** row `MT-1` in `docs/spark-sql-iceberg-parity.md` gains the served reader leg (rd-r3).

**Retires:** in flight.

**Scope:** `spark.read.format("iceberg").load("<cat>.<ns>.<t>.<meta>")` (and `.table(...)`)
answers what SQL `SELECT * FROM <cat>.<ns>.<t>.<meta>` answers, and with
`.option("versionAsOf", v)` / `.option("timestampAsOf", ts)` what SQL
`SELECT * FROM …​.<meta> VERSION AS OF v` / `TIMESTAMP AS OF ts` answers. New file
`crates/repark-core/src/time_travel/metadata_at.rs` (the ONE metadata AS OF decision:
`provider_for_spec` is the #802 `prepare_metadata_as_of` body moved down from
`repark-spark`; `read_metadata_path_at` routes four-part reader names past the
three-part loader; `read_sql` quotes the un-pinned read); one-line-each wiring in
`crates/repark-core/src/session.rs` (holds 1000 lines) and
`crates/repark-core/src/time_travel.rs`; net deletion in
`crates/repark-spark/src/time_travel.rs`; the Rust pin in
`crates/repark-spark/src/tests/metadata_tables_asof.rs`; the facade pins in
`python/repark/tests/test_ice_mt_reader_1.py`; the MT-1 reader paragraph; five `map.md`
files; and this ledger. The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md`,
`crates/repark-spark/src/wap.rs` and `crates/repark-sql/**` are untouched; no code
comment added anywhere.

**Follow-up (2026-09-22, WO rd-r4fix, critic r1):** three findings plus a class
sweep. V-001 (P1): `quoted_ident` emitted double quotes where the SQL door emits
backticks — now backticks, doubling embedded ones, exactly like the Python
expander's `quote_ident`. V-002 (P2): `prepare_metadata_as_of` loaded the base table
before the `all_*` refusal — the refusal is first again, kept in
`provider_for_spec` for the reader. V-003 (P2) SKIPPED: no facade API creates a
multi-level namespace (SQL DDL is two/three-part only; `create_namespace` and
`testing_oob_create_table` build single-level idents; dotted spellings name one
namespace), so the fixture is unconstructible without new catalog features, which
the work order forbids. Sweep: every SQL-door metadata spelling paired against the
reader; the quoted-`$` form with AS OF differs (reader `TableNotFound`, SQL serves)
and is reported, not fixed — `$` is not a reader spelling and the reader matches
Spark there. No existing expected value moved.

**Follow-up (2026-09-23, WO rd-r5fix, critic r2):** four findings plus a class
sweep, all test-side. V-001 (P2): Arrow types and nullability were unpinned —
`test_load_snapshots_equals_sql` now asserts the reader frame's
`select("operation")` field as `("operation", "string", True)` and
`test_load_version_as_of_equals_sql` asserts `select("record_count")` as
`("record_count", "bigint", False)` with rows `[[2]]`, the exact Spark 4.1.2
probe answers; the measured RePark schema matched, so no product change.
V-002 (P2): the `metadata_at.rs` real-table-wins branch was untested —
`metadata_asof_nested_namespace_real_table_wins` builds a two-level
`sales.sub` namespace through the catalog API with a real `snapshots` table
(row 77) alongside base table `sales.sub`, and asserts both the ordinary
four-part read and the AS OF read at the real table's own snapshot id refuse
loudly rather than serve `sales.sub` snapshot metadata (DataFusion caps
table references at three parts, so the real table is unreachable through
either door — the check turns a silent wrong answer into a loud refusal;
mutation-proven: deleting the branch makes `read_metadata_path_at` route to
the `sales.sub` snapshots provider). V-003 (P2): the live leg picked
`sorted(snapshot ids)[0]`, which is not commit order — now
`ORDER BY committed_at, snapshot_id LIMIT 1`, with the absolute answers
asserted on the live engine (operations `["append","append","delete"]`
sorted, `record_count` rows `[[1],[1]]`, both schema fields). The live
engine answers differ from the recorded probe cells on the same
statements — the probe's `sc` catalog was `InMemoryCatalog` under
`local[1]` (DELETE commits `overwrite`; one file of two records) while
the live leg's `livemt` catalog is `hadoop` under `local[2]` (DELETE
commits `delete` — a COW-shaped commit, verified via the snapshot
summary's `deleted-data-files` with zero position deletes, even with
explicit `write.delete.mode=copy-on-write`; two files of one record
each). V-004 (P3): the "thirteen
pins" counts here and in `map.md` now read twenty-two with C-001..C-022.
Sweep: C-002 pins `record_count` as `bigint` non-nullable on the reader
files frame, C-003 and C-016 pin the absolute operation multiset, C-005 pins
the timestamp-scoped `record_count` rows `[[1],[2]]`, C-007 pins the empty
frame's `record_count` field, C-009 pins the absolute selector rows, and
C-020 pins the uppercase-suffix AS OF `record_count` rows `[[2]]`.

**Follow-up (2026-09-23, WO rd-r6fix, critic r3):** three findings plus a class
sweep, all test/ledger-side. V-001 (P1): the live leg's `DELETE … id = 1`
depended on how many files Spark's first INSERT wrote (one file → `overwrite`,
two → `delete` — both observed); it now deletes id 3, a whole single-row file,
so Spark always commits `delete`, and the first-snapshot `record_count` pin
`[[1],[1]]` is replaced by `sum == 2`; the sorted operations pin, both
reader/SQL equalities and both schema pins are unchanged. V-002 (P1): C-018
asserted only reader/SQL equality — the loop now also pins each `all_*`
column list verbatim against the Spark 4.1.2 measurement (21 names for
`all_files`/`all_data_files`/`all_delete_files`, 6 for `all_entries`, 14 for
`all_manifests`); row counts stay unpinned as layout-dependent. C-019's
double-quoted `"t$snapshots"` spelling is one Spark 4.1.2 refuses on both
doors (reader `IllegalArgumentException` `Cannot parse identifier`; SQL
`ParseException [PARSE_SYNTAX_ERROR]`, SQLSTATE 42601) while RePark serves it —
the test stays as router equivalence and the clause is REJECTED as a parity
claim; the same applies to C-015, whose three-spelling refusal equality covers
two quoted-dollar spellings Spark parse-refuses. Both are pre-existing
divergences, not introduced by this PR, filed for follow-up. V-003 (P3): the
IPI-23-MT-READER-1 note in `crates/repark-spark/src/tests/map.md` sat after the
`normalize`, `local_fs_ddl` list line with a stray `nullability).` and a
duplicated list item; it now closes the `metadata_tables_asof` entry.
Sweep: every other clause's pin classification is recorded in the hand-back;
no pin count moved (still twenty-two facade pins, C-001..C-022).

**Follow-up (2026-09-23, WO rd-r7fix, critic r4):** one finding plus a class
sweep, all test-side. V-001 (P1): the shared `_seeded` fixture deleted id 1,
so the third snapshot's `operation` was `overwrite` when the first INSERT
wrote one file and `delete` when it wrote two, and the per-file
`record_count` rows depended on the same split. The fixture now deletes id 3
— the second INSERT's own single-row file, always a whole-file delete — and
RePark records `delete` for the third snapshot (measured on the facade;
Spark 4.1.2 records `delete` for a whole-file delete, the r6 live-leg
measurement). The four `overwrite` pins now read `append, append, delete`
(C-001's ordered list and the C-001/C-003/C-016 sorted multisets), the
first-snapshot `record_count` pins read `sum == 2` (C-004, C-020), the
second-snapshot pin reads `sum == 3` (C-005), and C-008's current rows are
`[[1,"a","x"],[2,"b","y"]]` under the new delete. Sweep: the remaining
Python pins are row values, schema fields, column lists or reader/SQL
equalities — none file-split dependent; the Rust pin file's per-snapshot
`count(*)` rows over `files`/`data_files`/`entries`/`manifests` and its
tag/branch/timestamp file counts are the same class, stable today only
because each INSERT writes one file — reported to the orchestrator, not
edited per the work order. Pin count unchanged: twenty-two facade pins,
C-001..C-022.

**Follow-up (2026-09-23, WO rd-r8fix, critic r5):** one finding plus two class
sweeps, all test/ledger-side. V-001 (P1): the offline fixture deletes id 3, so
snapshot 3 (current) answers the same rows as snapshot 1 — every AS OF /
selector pin at the first snapshot equalled a current read and could not tell
a historical read from an ignored option; the second snapshot (rows id 1, 2,
3) is the discriminating point. C-008's `versionAsOf` pin moved to the second
snapshot id with the three-row answer `[[1,"a","x"],[2,"b","y"],[3,"c","x"]]`
(the un-pinned current pin stays `[[1,"a","x"],[2,"b","y"]]`); C-009's `tag_t0`
and `snapshot_id_<id>` moved to the second snapshot with the same three-row
answer (`branch_b0` already pinned the second); C-004's reader/SQL equality
loop now runs at both the first and the second id over the four suffixes and
its `record_count` schema-plus-sum pin sits at the second id (`sum == 3`);
C-020's `versionAsOf` moved to the second id (`sum == 3`). C-004, C-005, C-008,
C-009 (tag and snapshot_id) and C-020 each also assert the pinned reader's
rows differ from the un-pinned reader's rows, so dropping the option fails the
test. Class sweep (a time-travel pin whose answer equals the current answer)
over the whole file: C-001/C-002/C-003/C-010/C-016/C-018/C-019 are un-pinned
reads, C-006/C-007/C-011/C-012/C-014/C-015/C-017/C-021/C-022 are refusal or
error-text pins outside the class — no other survivor; the live leg C-013
(first id on its own engine) and the Rust pin `metadata_tables_asof.rs` (`s1`
count/sum equalities) are report-only per the work order, not edited. Ledger
literal sweep (every expected literal in C-001..C-022 against the test at this
head): two rows disagreed and are corrected here — C-001's `append, append,
overwrite` (the offline fixture, delete id 3, pins operations
`append, append, delete`; the recorded Spark cell `R-DF-LOAD-META` is a
separate live measurement that still records `append, append, overwrite`, the
`InMemoryCatalog` probe's DELETE committing `overwrite`) and C-008's
`[[2,"b","y"],[3,"c","x"]]` current / first-snapshot pinned rows (current is
`[[1,"a","x"],[2,"b","y"]]` and the pinned read now answers the three rows
above at the second snapshot); C-004's `<first id>` is widened to both ids per
the fix above. Pin count unchanged: twenty-two facade pins, C-001..C-022.

**Follow-up (2026-09-23, WO rd-r9fix):** two findings, all test/ledger-side.
V-001 (P1, the r5 class's live survivor): C-013 still pinned the FIRST
snapshot, whose `files` answer after the leg's `DELETE … id = 3` equals the
current answer — the pin could not tell a historical read from an ignored
option. The live leg now takes the SECOND snapshot id (`ORDER BY
committed_at, snapshot_id`, index 1): `versionAsOf` on the reader and
`VERSION AS OF` SQL stay row-equal, `record_count` sums to 3 (measured live
`[1, 1, 1]` against the un-pinned current `[1, 1]`, sum 2), the pinned
reader's `record_count` rows are asserted different from the un-pinned
reader's on the same session, and the `("record_count", "bigint", False)`
schema pin and the `append, append, delete` operations pin are unchanged.
The class sweep (a time-travel pin whose answer equals the current answer)
has no remaining survivor in the Python file. r6 V-001 (P2): every
`pytest.raises` in the file now pins `getSqlState()` — the paired refusals
compare it across the reader and SQL doors, C-015 compares it pairwise
across its three SQL spellings (no reader arm), C-011 pairs only the
SQLSTATE beside a literal text pin, and C-012 stands alone on literal
texts plus `getSqlState() is None`. Where a pair pins a recorded Spark
answer the literal measured state is pinned too — every measured state is
`None` on live Spark 4.1.2 + Iceberg 1.11.0 (IllegalArgumentException for
`versionAsOf 'nope'`, `timestampAsOf '2000-01-01'` and the three legacy
options; UnsupportedOperationException — a pre-existing class divergence
from RePark's AnalysisException, reported — for the `all_*` AS OF
refusals), so the literal pins read `is None`. Pin count unchanged:
twenty-two facade pins, C-001..C-022.

**Follow-up (2026-09-23, WO rd-r10fix, critic r7):** two findings, all
ledger/prose-side — no assertion moved. V-001 (P2, a repeat of the r5
class): the table rows still carried pre-r7 literals. C-001 now states
`append, append, delete` plus the `("operation", "string", True)` field,
with `R-DF-LOAD-META`'s `append, append, overwrite` kept in Evidence as a
separate recorded probe replay; C-004 now spans both snapshot ids with the
second-id `("record_count", "bigint", False)` / `sum == 3` /
pinned!=current pins, `R-DF-LOAD-META-FILES-VERSIONASOF`'s `[2]` likewise
marked a separate replay; C-008 now reads current
`[[1,"a","x"],[2,"b","y"]]` with the three-row second-snapshot pin. The
same pass rewrote the other under-stated rows: C-002 and C-007's
`record_count` field pins, C-003 and C-016's `append, append, delete`
multiset, C-005 and C-020's second-point `sum == 3` and pinned!=current
guards, C-009's absolute selector rows and second-id selector, C-011's
literal-text + paired-SQLSTATE shape, C-012's standalone literal texts +
`is None`, C-013's second-snapshot live pins, C-014/C-017/C-021's SQLSTATE
pairs, C-018's absolute column lists and C-022's `Cannot select snapshot`
sentence plus SQLSTATE `None`. V-002 (P2): the module docstring, the r9fix
paragraph above and the `python/repark/tests/map.md` entry all claimed
every refusal/`pytest.raises` pairs class and text (or SQLSTATE) across
doors — C-011 pairs only `getSqlState()` beside a literal text, C-012 is
standalone literal text plus `is None`, and C-015 compares three SQL
spellings pairwise; all three texts now say so, and the closing paragraph
and AT-2 now scope their "every refusal"/"every clause" sentences to the
paired pins. Pin count unchanged: twenty-two facade pins, C-001..C-022.

**Follow-up (2026-09-23, WO rd-r11fix, critic r8):** three findings plus a
full quantifier sweep, all prose-side — no assertion moved. V-001 (P1): the
module docstring and the tests `map.md` entry claimed every answering test
compares rows and column names against the SQL door — C-010 pins the
reader's rows standalone with no SQL arm and the live leg C-013 compares the
selected rows only (`_live_rows` collects row values, not column names)
beside its absolute field and sum pins; both texts now name the exceptions.
V-002 (P1): the MT-1 reader bullet in `docs/spark-sql-iceberg-parity.md`
claimed reader refusals carry the SQL door's texts verbatim — now scoped to
the paired refusals, with C-011's literal-text-plus-SQLSTATE shape and
C-012's standalone literals named. V-003 (P2): the C-014 row claimed the
"full backticked" text and the test docstring "backticks included" — both
now state relative equality (the reader's class, text and `getSqlState()`
equal the SQL door's on the same spelling); the red-first quoting difference
stays in Evidence. The same sweep narrowed C-007's docstring ("the SQL
schema" → the SQL door's column names, what `_frame_cols` asserts), AT-1's
"each answering clause" (C-010 and C-013 named) and AT-9's "errors verbatim"
(the unknown-suffix refusal keeps the reader's own spelling). The full sweep
table is in the rd-r11fix hand-back. Pin count unchanged: twenty-two facade
pins, C-001..C-022.

## Measurements (decide-then-build evidence)

**M-1 — the oracle is recorded, not re-derived.** The two replay cells were recorded
against live PySpark 4.1.2 + Iceberg 1.11.0 and carried in
`/tmp/xo-xo-opus55/sb-mt/out/spark-core.json`: `R-DF-LOAD-META`
(`load(t.snapshots).select("operation")` rows `append, append, overwrite`) and
`R-DF-LOAD-META-FILES-VERSIONASOF` (`versionAsOf` on `load(t.files)` rows `[2]`).
On RePark main the first fails with
`ParseException: SQL error: ParserError("Expected: end of statement, found: $snapshots …")`
and the second with
`AnalysisException: Error during planning: time travel requires a three-part
catalog.namespace.table identifier, got …​.files`.

**M-2 — why SQL answers and the reader does not.** Python `spark.sql` rewrites FROM
references through `_sql_table_ref`, so the engine sees the quoted
`SELECT * FROM "sc"."ns"."t"."snapshots"`; the metadata rewrite preserves the quoting
(`"t$snapshots"`) and the Databricks-dialect parse succeeds. The reader's internal
`self.sql("SELECT * FROM sc.ns.t.snapshots")` is unquoted, so the rewrite emits a bare
`t$snapshots` the Databricks tokenizer splits at `$`. The un-pinned reader arm now
quotes the same shape and rides the identical router path; probed on main before the
fix (`SELECT *` == `SELECT operation` == quoted form on `spark.sql`, reader red).

**M-3 — the red-first pins.** Six facade pins fail on main with the M-1 texts
(`C-001`/`C-002` the `$snapshots`/`$files` ParserError; `C-004`/`C-005`/`C-006`/`C-007`
the three-part error); the Rust pin fails with
`Plan("time travel requires a three-part … got ice.sales.m.files")`. The near-miss
pins (`C-003`, `C-008`–`C-012`) pass on main and after. The unknown-suffix text pinned
is the reader's own (`'…​.nope'. Expected 1, 2 or 3 parts, got 4`), which differs from
the SQL door's backticked spelling by construction (unquoted internal SQL, unchanged).

## PROPOSITION LEDGER — IPI-23-MT-READER-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `load("mt.ns.t.snapshots")` answers the same rows and schema field names as `SELECT * FROM mt.ns.t.snapshots`; the commit-ordered operations read `append, append, delete`, the sorted `operation` multiset is the same, and the `select("operation")` field is `("operation", "string", True)`. | `test_load_snapshots_equals_sql` green. | **PROVEN** | Reader/SQL equality on rows and columns plus the absolute operation and field pins on this fixture; the recorded cell `R-DF-LOAD-META` — a separate live replay on the `InMemoryCatalog` probe whose DELETE committed `overwrite` — still reads `append, append, overwrite`. pins: ipi-23-mt-reader-1/C-001 |
| C-002 | `load("mt.ns.t.<meta>")` equals the SQL door (rows and columns) for `files`, `history` and `refs`, and the reader `files` frame carries `("record_count", "bigint", False)`. | `test_load_files_history_refs_equal_sql` green. | **PROVEN** | Same seed; per-suffix reader/SQL equality plus the absolute `record_count` field pin. pins: ipi-23-mt-reader-1/C-002 |
| C-003 | `read.table("mt.ns.t.snapshots")` equals the SQL door (rows and columns), with the sorted `operation` multiset `append, append, delete`. | `test_table_api_snapshots_equals_sql` green. | **PROVEN** | Already green on main via `session.table`; pinned against regressions with the absolute multiset. pins: ipi-23-mt-reader-1/C-003 |
| C-004 | `.option("versionAsOf", <id>).load("mt.ns.t.<meta>")` equals SQL `VERSION AS OF <id>` (rows and columns) for `files`, `snapshots`, `entries` and `manifests` at both the first and the second snapshot id; at the second id the `files` `record_count` field is `("record_count", "bigint", False)`, the scoped rows differ from the un-pinned read, and `record_count` sums to 3. | `test_load_version_as_of_equals_sql` green. | **PROVEN** | Reader/SQL equality at both ids plus the second-id absolute pins; the recorded cell `R-DF-LOAD-META-FILES-VERSIONASOF` (`record_count` rows `[2]`) is a separate probe replay whose inventory differs from this fixture. pins: ipi-23-mt-reader-1/C-004 |
| C-005 | `.option("timestampAsOf", <second commit instant>).load("mt.ns.t.files")` equals SQL `TIMESTAMP AS OF '<ts>'` (rows and columns); `record_count` sums to 3 and the scoped rows differ from the un-pinned read. | `test_load_timestamp_as_of_equals_sql` green. | **PROVEN** | Second-snapshot commit instant on both doors; reader/SQL equality plus the absolute sum and the pinned!=current guard. pins: ipi-23-mt-reader-1/C-005 |
| C-006 | Reader AS OF refusals carry the SQL door's class and exact text: `versionAsOf 'nope'` on `.files` → `IllegalArgumentException("Cannot find matching snapshot ID or reference name for version nope")`; `timestampAsOf` before the first snapshot → `IllegalArgumentException("Cannot find a snapshot older than 2000-01-01T00:00:00+00:00")`; `versionAsOf` on `.all_files` → the SQL door's `AnalysisException` text containing `Cannot select snapshot in table: ALL_FILES` — with `getSqlState()` equal across doors and `None` on each. | `test_reader_as_of_refusals_match_sql` green. | **PROVEN** | Each reader refusal asserted equal to the SQL door's class and text plus the paired SQLSTATE; the #802 sentences verbatim. pins: ipi-23-mt-reader-1/C-006 |
| C-007 | Unknown numeric `versionAsOf` on `.files` answers empty with the SQL door's schema field names, the reader frame carrying `("record_count", "bigint", False)`. | `test_reader_unknown_numeric_as_of_answers_empty` green. | **PROVEN** | `try_new_empty` through the reader; zero rows, equal columns, absolute field pin. pins: ipi-23-mt-reader-1/C-007 |
| C-008 | Near miss: `load("mt.ns.t")` with and without `versionAsOf` keeps its rows — `[[1,"a","x"],[2,"b","y"]]` current, `[[1,"a","x"],[2,"b","y"],[3,"c","x"]]` at the second snapshot — each equal to the SQL door, and the pinned read differs from the current. | `test_load_plain_and_version_as_of_unchanged` green. | **PROVEN** | Absolute rows plus reader/SQL equality and the pinned!=current guard; green on main and after. pins: ipi-23-mt-reader-1/C-008 |
| C-009 | Near miss: `load("mt.ns.t.branch_b0")`, `.tag_t0` and `.snapshot_id_<second id>` equal the SQL door (rows and columns) and answer `[[1,"a","x"],[2,"b","y"],[3,"c","x"]]`; the tag and snapshot-id reads differ from the un-pinned read. | `test_load_branch_tag_snapshot_id_selectors_unchanged` green. | **PROVEN** | Selector routing untouched; reader/SQL equality plus the absolute second-snapshot rows per suffix. pins: ipi-23-mt-reader-1/C-009 |
| C-010 | Near miss: a real table `mt.ns.snapshots` reads its own rows (`[[1],[2]]`) under `load`. | `test_load_table_named_snapshots_reads_real_table` green. | **PROVEN** | Three-part names never route to metadata. pins: ipi-23-mt-reader-1/C-010 |
| C-011 | Near miss: `load("mt.ns.t.nope")` keeps today's `AnalysisException` text as a literal pin (`Error during planning: Unsupported compound identifier 'mt.ns.t_load_nope.nope'. Expected 1, 2 or 3 parts, got 4`); only `getSqlState()` is compared against the SQL door's on the same spelling. | `test_load_unknown_suffix_keeps_error_text` green. | **PROVEN** | The reader's own literal, measured on main (M-3); SQLSTATE paired, text literal. pins: ipi-23-mt-reader-1/C-011 |
| C-012 | Near miss: the legacy `snapshot-id` / `as-of-timestamp` / `tag` options on `load("mt.ns.t.files")` refuse standalone with the #800 `IllegalArgumentException` literal texts and `getSqlState()` `None` — no SQL arm. | `test_legacy_options_on_metadata_keep_refusal_texts` green. | **PROVEN** | Python-layer refusals fire before any engine routing; literal texts plus `is None`. pins: ipi-23-mt-reader-1/C-012 |
| C-013 | Live leg: on Spark 4.1.2 itself the reader answers what SQL answers for `load(t.snapshots)` and `versionAsOf` on `load(t.files)` at the second snapshot id — sorted `operation` multiset `append, append, delete`, field `("operation", "string", True)`, `record_count` sum 3 and field `("record_count", "bigint", False)`, the pinned read differing from the un-pinned. | `test_live_spark_reader_matches_sql` green under `REPARK_PARITY_LIVE=1`. | **PROVEN** | Goal premise re-measured on the live oracle in the gate's live leg. pins: ipi-23-mt-reader-1/C-013 |
| C-014 | V-001: `load("mt.ns.T_CASETWIN.snapshots")` (stored lowercase) fails with the same class, text and `getSqlState()` as the SQL door on the same spelling. | `test_load_case_twin_table_matches_sql_door` green. | **PROVEN** | Red-first: double-quoted vs backticked identifier; equal after the fix. pins: ipi-23-mt-reader-1/C-014 |
| C-015 | V-002: `SELECT count(*) FROM mt.ns."missing$all_files" VERSION AS OF 1` gives the `ALL_FILES` refusal (class, sqlstate, full message), equal to the existing-table dollar and dotted spellings — implicitly the Spark refusal contract. | `test_quoted_dollar_missing_table_refuses_all_files` green. | **REJECTED** (ROUTER-EQUIVALENCE ONLY — DIVERGES FROM SPARK) | The three-spelling equality holds on RePark and stays pinned as router equivalence (red-first `TableNotFound` before the refusal-first fix); on Spark 4.1.2 both quoted-dollar spellings parse-refuse (`PARSE_SYNTAX_ERROR`, the C-019 measurement) before any `ALL_FILES` check, so the pinned equality is RePark-internal. Pre-existing divergence filed for follow-up. pins: ipi-23-mt-reader-1/C-015 |
| C-016 | Sweep: `load("mt.ns.t.SNAPSHOTS")` equals the SQL door (rows and columns), with the sorted `operation` multiset `append, append, delete`. | `test_load_uppercase_suffix_equals_sql` green. | **PROVEN** | Uppercase suffix serves on both doors. pins: ipi-23-mt-reader-1/C-016 |
| C-017 | Sweep: missing-table and missing-namespace un-pinned loads fail with the SQL door's class, exact text and `getSqlState()`. | `test_load_missing_parent_matches_sql_door` green. | **PROVEN** | Red-first on quoting with C-014; equal after. pins: ipi-23-mt-reader-1/C-017 |
| C-018 | Sweep: every `all_*` table un-pinned equals the SQL door (rows and columns), and each column list equals the recorded Spark 4.1.2 inventory. | `test_load_all_types_without_as_of_equals_sql` green. | **PROVEN** | Rows relative (router equivalence); columns absolute (Spark 4.1.2, measured 2026-09-23) — all five serve via the fork on both doors. pins: ipi-23-mt-reader-1/C-018 |
| C-019 | Sweep: the quoted `t$snapshots` spelling un-pinned equals the SQL door (rows and columns) — implicitly a Spark-servable parity spelling. | `test_load_quoted_dollar_spelling_equals_sql` green. | **REJECTED** (ROUTER-EQUIVALENCE ONLY — DIVERGES FROM SPARK) | Door-equality holds and stays pinned as router equivalence; Spark 4.1.2 refuses the double-quoted `"t$snapshots"` spelling on both doors — reader `IllegalArgumentException: Cannot parse identifier: sc.db."t$snapshots"`, SQL `ParseException [PARSE_SYNTAX_ERROR] Syntax error at or near '"t$snapshots"'`, SQLSTATE 42601 (measured 2026-09-23). Pre-existing divergence filed for follow-up. pins: ipi-23-mt-reader-1/C-019 |
| C-020 | Sweep: uppercase `.FILES` with `versionAsOf` at the second snapshot id equals SQL `VERSION AS OF` (rows and columns); `record_count` sums to 3 and the scoped rows differ from the un-pinned read. | `test_load_uppercase_suffix_as_of_equals_sql` green. | **PROVEN** | Case-insensitive routing on both doors. pins: ipi-23-mt-reader-1/C-020 |
| C-021 | Sweep: missing parents and the unknown suffix with `versionAsOf` fail with the SQL door's class, exact three-part text and `getSqlState()`. | `test_load_missing_parent_as_of_matches_sql_door` green. | **PROVEN** | Fallthrough parity on both doors. pins: ipi-23-mt-reader-1/C-021 |
| C-022 | Sweep: the remaining four `all_*` refusals match the SQL door's class and text, each containing `Cannot select snapshot in table: <UPPER>`, with `getSqlState()` equal across doors and `None`. | `test_load_all_types_as_of_refuse_alike` green. | **PROVEN** | Per-type refusal sentences verbatim on both doors. pins: ipi-23-mt-reader-1/C-022 |

## Gates

| Command | Result |
|---|---|
| `build-slot.sh local-gate.sh xo55-rd "repark-core:--lib+time_travel,…" <6 test files>` | `CB=0 R=0 T=0 U=0 L=0` — rust 13+29+22 passed; unit 264 passed 2 skipped; live 266 passed |
| `make rust-clippy` | exit 0 (workspace, `-D warnings`) |
| `make rust-panic-ban` | exit 0 |
| `make check-rust-file-size` | exit 0 (808 files clean; `session.rs` holds 1000) |
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 at the final head |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo55-rd origin/main HEAD` | exit 0 (`hits=0`) after every commit |
| `cargo fmt --check` + `uvx ruff@0.15.22 check/format` on touched files | exit 0 |
| sb-mt replay `--only R-DF-LOAD-META,R-DF-LOAD-META-FILES-VERSIONASOF` | both cells equal `spark-core.json` rows and columns |
| `python3 scripts/check_ledger_grammar.py` | exit 0 |
| r4fix `build-slot.sh local-gate.sh xo55-rd <same 6 files>` | `CB=0 R=0 T=0 U=0 L=0` — rust 13+29+22 passed; unit 273 passed 2 skipped; live 275 passed |
| r4fix `make rust-clippy` / `make rust-panic-ban` | exit 0 / exit 0 |
| r6fix `build-slot.sh local-gate.sh xo55-rd <same 6 files>` | `CB=0 R=0 T=0 U=0 L=0` at `a4faadbc` — rust 13+30+22 passed; unit 273 passed 2 skipped; live 275 passed (the re-pinned live leg runs, not skips) |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ipi-23-mt-reader-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Answering clauses walk the reader to the SQL door on the same table — rows plus schema field names on snapshots/files/history/refs (C-001/C-002), the table API (C-003), versionAsOf on four types (C-004), timestampAsOf (C-005) — except C-010, which pins the reader's rows standalone, and the live C-013, which compares the selected rows only; the two recorded Spark cells replayed equal (rows append/append/overwrite, record_count [2]).
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The seed exercises a two-append history with a delete so pins sit mid-history; the Rust pin drives the moved provider_for_spec through read_table_at and the SQL door for one scoped case (one row, rendered batches equal) and one empty case (zero rows, schema names equal); paired refusals pin class plus full text on both doors, the standalone refusal pins carry their literal texts.
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py, crates/repark-spark/src/tests/metadata_tables_asof.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The 'nope'/too-old/all_* refusals assert the reader's class and text equal the SQL door's, with the #802 sentences verbatim; legacy options keep the #800 IllegalArgumentException texts; the unknown suffix keeps the reader's measured Analysis text.
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Resolution runs per query through Session state; the metadata provider registers one temp view per read with no shared mutable state or global registration, mirroring the existing read_table_at tail.
      artifacts: [crates/repark-core/src/time_travel/metadata_at.rs]
    - id: AT-5
      status: N/A
      justification: Read-only metadata scans over the session's own catalog; no auth, secret, or injection surface added — the quoted internal SQL only reorders identifier segments the parser already accepted.
    - id: AT-6
      status: ATTACKED
      evidence: Registry row MT-1 gains the served reader leg; every previously green behavior in scope (plain loads, selectors, real metadata-named tables, unknown suffix, legacy refusals) is pinned unchanged, and no existing test's expected value moved. Critic r3 re-marks C-015/C-019 router-equivalence-only — Spark 4.1.2 refuses the quoted-dollar spellings (pre-existing divergence filed for follow-up).
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-7
      status: N/A
      justification: The scan streams through the existing provider and temp-view tail with two catalog existence probes per pinned read; no added materialization, unbounded growth, or hot-loop pattern.
    - id: AT-8
      status: ATTACKED
      evidence: The moved decision calls the same fork inspect constructors at the pinned rev through the same repark-iceberg provider the SQL door uses; no dependency or pin move, no Cargo.toml change.
      artifacts: [crates/repark-core/src/time_travel/metadata_at.rs, crates/repark-spark/src/time_travel.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Paired reader refusals carry the SQL door's class and `getSqlState()`; the refusal text is asserted equal only where a test pairs it — C-006, C-014, C-017, C-021 and C-022 reader-vs-SQL, C-015 pairwise across its three SQL spellings — while C-011 pins the reader's own literal text beside the paired SQLSTATE and C-012 stands alone on literals. A real table wins over the metadata route when the probe answers it exists (the nested-namespace Rust pin `metadata_asof_nested_namespace_real_table_wins`); a `DataInvalid` probe on a multi-level namespace counts as "no such table", the same rule the SQL door has used since #219 (`metadata_tables.rs`), so reader and SQL door probe alike.
      artifacts: [crates/repark-core/src/time_travel/metadata_at.rs, crates/repark-spark/src/tests/metadata_tables_asof.rs, python/repark/tests/test_ice_mt_reader_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Two recorded cells replayed verbatim plus twenty-two facade pins and two Rust pins, every clause carrying a pins: citation in the test file and the touched map.md rows; C-001 asserts schema field names alongside values, and after critic r2 the recorded field type and nullability are pinned where the cells measured them.
      artifacts: [python/repark/tests/test_ice_mt_reader_1.py, python/repark/tests/map.md, crates/repark-core/src/time_travel/map.md]
```

Every clause above except C-015 and C-019 is PROVEN — the paired ones against
the SQL door on the same table, the standalone ones (C-010's literal rows,
C-012's literal refusal texts) against their pinned literals — and the two
replay cells against recorded PySpark 4.1.2 rows; C-015
and C-019 are REJECTED as parity clauses — each holds as router equivalence on
spellings Spark 4.1.2 refuses (pre-existing divergence filed for follow-up).
No clause is OPEN. Touched files
per the Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`, `STATUS.md`, `wap.rs`
and the ANSI door are untouched. `make verify` and the full facade suite were not
run per the work order's gate list; the gates table above is the proof.
