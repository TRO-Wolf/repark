# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.
## Contents
- [u8-write-sql-ledger.md](u8-write-sql-ledger.md) —
  **U8 WRITE-SQL PR1 (2026-09-24), in flight:** `INSERT INTO … REPLACE WHERE` as Spark's
  overwrite by filter, `INSERT INTO … PARTITION (…)` static and dynamic keys, and positional
  INSERT sources whose names repeat (the bucketed `range()` cell). Supersedes the refusal
  text of ice-overwrite-mode-1/C-013. Critic r1 remediation (2026-09-25): NULL partition keys
  under `<`, `<=`, `NOT` and `NOT IN`, repeated-name REPLACE WHERE sources, out-of-range
  literals. Critic r2 remediation: `<=>` and beyond-i64 constants, Spark's conjunct-split
  refusal texts, repeated-name arity naming, and the committed Spark oracle. Critic r3
  disposition: decimal literals on INT columns (C-022), Spark's data-column names (C-023), the
  suffix-literal rendering (C-024), residues R-11 (typed / DECIMAL / timestamp-string literals)
  and R-12 (a NULL key in the same INSERT). C-001..C-024 PROVEN.
  `risk_tier: standard`. Branch `feat/u8-write-sql`.
  pins: u8-write-sql/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-024
- [u6-write-refusals-ledger.md](u6-write-refusals-ledger.md) —
  **U6 WRITE-REFUSALS PR1 (2026-09-24), in flight:** refusal parity on
  `write.spark.accept-any-schema` tables — positional writes resolve by name and
  refuse `Field <name> not found in source schema`, the merge-schema conf
  unions on every write, and `MERGE WITH SCHEMA EVOLUTION` refuses a
  non-promotable type change with `Cannot change column type`. Supersedes
  ipi-19-56-37-schema-evolution-write/C-013. Critic r1 remediation: an added
  column keeps the source spelling, the `BY NAME` overwrite door, the upper-case
  refusal spelling, Iceberg's duplicate-name refusals, and routing that no
  longer hides a failed table load. C-001..C-014 PROVEN.
  `risk_tier: standard`. Branch `feat/u6-write-refusals`.
  pins: u6-write-refusals/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014
- [ipi-20-input-file-name-1-ledger.md](ipi-20-input-file-name-1-ledger.md) —
  **IPI-20-INPUT-FILE-NAME-1 (2026-09-23), in flight:** `input_file_name()` on the
  Spark door — a second trigger into `prepare_metadata_column_sql` rewrites a
  zero-argument call to `<alias>._file` inside a SELECT whose own single
  relation is the Iceberg table by written name (an alias match alone does not
  qualify — the CTE alias-collision pin, C-010), and collection itself is
  CTE-aware: a one-part name matching any CTE alias in the statement is never
  treated as the physical table (C-011) while qualified names stay collected
  (C-012); the aggregate-arg, arity,
  join, VALUES, metadata-table and UNION-ALL residues keep
  `UNRESOLVED_ROUTINE`. C-001..C-013 PROVEN.
  `risk_tier: standard`. Branch `fix/ipi-20-input-file-name`.
  pins: ipi-20-input-file-name-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012, C-013
- [dfload-1-ledger.md](dfload-1-ledger.md) —
  **DFLOAD-1 / U10 (2026-09-23), in flight:** `format("iceberg").load(<path>)` reads the
  table at a filesystem path like Spark's `IcebergSource` — the contains-slash route,
  `.metadata.json` direct, `version-hint.text` then highest-integer metadata resolution,
  static read-only provider that leaves no registration behind, pinned refusal for
  time-travel/incremental options beside a path, the catalog route for the pinned
  slash-free near-misses, the `file:` spellings Spark reads or refuses (`Wrong FS`
  for an authority, `URISyntaxException` for a relative path), and `v<N>` / hint versions
  bounded to the Java `int` range. C-001..C-010 PROVEN. `risk_tier: standard`. Branch
  `fix/u10-df-load-path`.
  pins: dfload-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [ice-procedures-1-ledger.md](ice-procedures-1-ledger.md) —
  **ICE-PROCEDURES-1 (2026-09-20), in flight:** the CALL declared-parameter
  binder, parser first (IPI-31 D-1) — one parameter table transcribed from the
  jar plus `bind` in `call_args.rs`; the mixed-args refusal is deleted and the
  RDF and RPD handlers bind against the declared lists, closing `P-POS-RDF`,
  `P-POS-RPD` and `P-CALL-MIXED-ARGS` with nothing else changing product
  behaviour. **PR1b (2026-09-21)** wires `add_files`, RPD `where` and the four
  expire arguments on the same branch. `risk_tier: standard`. Branch
  `fix/ipi-30-31-procedures`.
  pins: ice-procedures-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019,
  C-020
- [ice-views-1-ledger.md](ice-views-1-ledger.md) —
  **ICE-VIEWS-1 (2026-09-20), in flight:** IPI-40 views on the Spark door — WO-1
  (this PR) is the catalog door and the read path on the memory catalog:
  `CREATE [OR REPLACE] VIEW`, `DROP VIEW`, `SHOW VIEWS`, `SELECT` from views,
  the D-9 error contract, and the A-9 viewless refusals. C-001..C-016 PROVEN;
  C-017 (PR2–PR5: DESCRIBE / SHOW CREATE / SHOW TBLPROPERTIES / ALTER VIEW)
  PROVEN, narrowed (2026-09-24); C-018 PROVEN by IPI-40 PR6 (2026-09-24): SQL session temporary views,
  temp-first DROP, temp DESCRIBE and SHOW VIEWS, the dbt SQL door pins
  (DBT-TEMPVIEW-1 retired, DBT-INCREMENTAL-1 registered; dbt
  `materialized='view'` stays refused and is not claimed). Its COVERAGE_ATTESTATION AT-10
  cites the 69-row final mutation table (PR6 r2).
  `risk_tier: standard`. Branch `fix/ipi-40-views-1`.
  pins: ice-views-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018
- [ice-error-conditions-1-ledger.md](ice-error-conditions-1-ledger.md) —
  **ICE-ERROR-CONDITIONS-1 (2026-09-20), in flight:** IPI-51 PR1 — the native
  error-condition parser only: PyO3 exceptions report `getCondition`/`getErrorClass`/
  `getSqlState` by parsing an already Spark-shaped message (at most one known engine
  prefix, a column-0 `[CONDITION]` token, the last `SQLSTATE: XXXXX` anywhere);
  `attach_error_condition` instance binds still win. No catalogue, no raise-site
  sweep — C-010 through C-014 stay OPEN for later PRs.
  `risk_tier: standard`. Branch `fix/ipi-51-error-conditions`.
  pins: ice-error-conditions-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [map-pr-gate-1-ledger.md](map-pr-gate-1-ledger.md) —
  **MAP-PR-GATE-1 (2026-09-20), in flight:** the map.md lockstep unit moves from the commit to
  the pull request — `check_map_md.sh --base` diffs the branch in ci.yml's `map.md guard`
  (pull requests only), `make check-map-md` runs it over `BASE ?= origin/main`, the two hook
  paths keep a warn-only staged mode, `.gitattributes` sets `map.md merge=union`, and
  `sync_map_md.py` gains the unconditional duplicate-row rule for what a union merge leaves
  behind; 32 pre-existing first-link collisions cleaned. `risk_tier: standard`. Branch
  `chore/map-pr-gate-1`.
  pins: map-pr-gate-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012
- [ipi-21-25-42-small-parser-ledger.md](ipi-21-25-42-small-parser-ledger.md) —
  **IPI-21 + IPI-25 + IPI-42 (2026-09-20), in flight:** the three small parser shapes over
  behaviour that already worked — `IF [NOT] EXISTS` as an optional infix on snapshot-ref DDL,
  `REPLACE TABLE [AS SELECT]` as the first token rewrite of `parse_single_normalized` with the
  missing-table refusal Spark's spelling requires, and `DROP TABLE … PURGE` threaded from the
  Python expander through `Statement::Drop.purge` to the fork's `DeleteReachableFiles`, gated on
  `gc.enabled`. Nine inventory cells plus the run-25e `p3.json` probe; registry `REF-2` retired,
  `RTAS-OPS-1` widened, `ICE-DROP-PURGE-1` filed. `risk_tier: standard`.
  Branch `fix/ipi-21-25-42-small-parser`.
  pins: ipi-21-25-42-small-parser/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [ice-rdf-sort-parse-1-ledger.md](ice-rdf-sort-parse-1-ledger.md) —
  **ICE-RDF-SORT-PARSE-1 (2026-09-20), in flight:** `CALL rewrite_data_files`
  `strategy` and `sort_order` parse and route onto the fork's sort and z-order
  rewriters (IPI-43) — the shared identity grammar with direction-tied NULLS
  defaults, the z-order scan, Spark-measured in-file order pins over a 7-cell
  recorded oracle, and every refusal in Java's or the fork's text under Java's
  class; all eleven clauses OPEN pending mutation. Round 3 (2026-09-20) closes
  V-001..V-005: verbatim fork messages by equality, the re-pinned facade and
  maintenance rows, the retired `RDF-SORT-1` plus new `RDF-SORT-TRANSFORM-1`
  registry rows, and the cell sort-state and `replace` pins. `risk_tier: standard`.
  Branch `fix/ipi-43-rdf-sort-zorder`.
  pins: ice-rdf-sort-parse-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011
- [ice-wap-branch-1-ledger.md](ice-wap-branch-1-ledger.md) —
  **ICE-WAP-BRANCH-1 (2026-09-19), in flight:** the session conf `spark.wap.branch` redirects
  writes and the session's plain reads to an audit branch, as Spark does (IPI-05, RePark
  half) — one Rust resolver (explicit selector > the conf when `write.wap.enabled=true` >
  `main`), a `WapSessionConfig` carrier served through `conf.set` and SQL `SET`, the branch
  created by the write when absent, and Java's `Cannot set both WAP ID and branch` refusal;
  19-cell recorded Spark 4.1.2 oracle plus red-first pins (5/19 before, 19/19 after);
  registry `ICE-WAP-BRANCH-1` FIXED, REF-3 and CONF-WAP-1 narrowed to what is still open.
  `risk_tier: standard`. Branch `fix/ice-wap-branch-1`.
  pins: ice-wap-branch-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [ice-rm-deletes-1-ledger.md](ice-rm-deletes-1-ledger.md) —
  **ICE-RM-DELETES-1 (2026-09-20), in flight:** `CALL rewrite_manifests` rewrites delete
  manifests through the fork #318 opt-in and honours `spec_id` (IPI-11, RePark half) —
  per-leg work gating in `rewrite_if`, the lone-manifest no-op on both legs, unknown-spec
  refusal in Spark's recorded `Invalid spec id` text; 14-cell recorded Spark 4.1.2 oracle
  plus red-first replay pins; registry MANIFEST-1 FIXED, MANIFEST-2 spec half FIXED.
  `risk_tier: standard`. Branch `fix/ice-rm-deletes-1`.
  pins: ice-rm-deletes-1/C-001, C-002, C-003, C-004, C-005, C-006
- [ice-overwrite-mode-1-ledger.md](ice-overwrite-mode-1-ledger.md) —
  **ICE-OVERWRITE-MODE-1 (2026-09-19), in flight:** Spark's overwrite partition set on
  every overwrite door (IPI-03) — one Rust decision over the session
  `partitionOverwriteMode`, the typed `saveAsTable` / `overwritePartitions` intent and the
  `overwrite-mode` writer option; static-mode `PARTITION (k)` replaces the whole table,
  mixed lists run, `NON_PARTITION_COLUMN` refuses; 60-cell recorded oracle plus red-first
  pins. Round 2 (same day): 88 cells; an empty dynamic source commits nothing, `BY NAME`
  mixed lists run, static values are cast to the partition type, transform-source keys
  refuse `NON_PARTITION_COLUMN`. Round 3 (same day): 96 cells with the 8 empty-frame
  `writeTo` / `insertInto` cells, and the typed intent flags pinned at the PyO3 binding.
  `risk_tier: standard`. Branch `fix/ice-overwrite-mode-1`.
  pins: ice-overwrite-mode-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-013, C-014, C-015, C-016, C-017
- [range-tvf-id-1-ledger.md](range-tvf-id-1-ledger.md) —
  **RANGE-TVF-ID-1 (2026-09-18), in flight:** the `range(...)` table function
  names its column `id` like Spark 4.1.2 on both SQL doors — a RePark-owned
  `TableFunctionImpl` over DataFusion's series generator, 1–4 arguments, string
  coercion, loud zero-step refusal; recorded oracle plus red-first pins.
  `risk_tier: standard`. Branch `fix/range-id-1`.
  pins: range-tvf-id-1/C-001, C-002, C-003, C-004, C-005
- [range-tvf-id-2-ledger.md](range-tvf-id-2-ledger.md) —
  **RANGE-TVF-ID-2 (2026-09-19), in flight:** the `range(...)` argument edge
  cases answer Spark 4.1.2 — NULL bounds refuse with `UNEXPECTED_INPUT_TYPE`,
  every Spark-accepted width coerces with `CAST_INVALID_INPUT` on malformed
  strings, the owned `RangeTable` provider streams exactly the `i128` element
  count so overflow bounds emit one row, and `numPartitions` validates with
  `IllegalArgumentException` on non-positive counts; recorded 19-cell oracle
  plus red-first pins on both SQL doors. `risk_tier: standard`. Branch
  `fix/range-id-2`.
  pins: range-tvf-id-2/C-001, C-002, C-003, C-004, C-005, C-006
- [v3-multiarg-1-ledger.md](v3-multiarg-1-ledger.md) —
  **V3-MULTIARG-1 (2026-09-18), in flight:** multi-argument partition transforms
  (`source-ids`) DECLARED out of 1.x under owner ruling 2026-09-18 (rating row V3-05)
  — docs plus one pin: the SQL-door arity refusal, the recorded Spark 4.1.2 DDL
  refusal fixture, the foreign-metadata register refusal, registry row V3-MULTIARG-1,
  post-1.x card. `risk_tier: standard`. Branch `docs/v3-multiarg-1`.
  pins: v3-multiarg-1/C-001, C-002, C-003, C-004, C-005
- [ice-append-retry-1-ledger.md](ice-append-retry-1-ledger.md) —
  **ICE-APPEND-RETRY-1 (2026-09-18), in flight:** the INSERT-STORM row corrected to what
  Spark measures over repetitions (rating row V2-20a remaining distance) — the six-repetition
  Spark oracle on two catalogs, the recorder, the corrected registry row and test docstring,
  no product change under ruling Q-23b-1. `risk_tier: standard`. Branch
  `docs/ice-append-retry-1`.
  pins: ice-append-retry-1/C-001, C-002, C-003, C-004, C-005
- [ice-tsns-sql-1-ledger.md](ice-tsns-sql-1-ledger.md) —
  **ICE-TSNS-SQL-1 (2026-09-17), in flight:** `timestamp_ns` / `timestamptz_ns` on the SQL door
  (rating row V3-06 and the ns half of V3-04) answer the Iceberg v3 spec with a PyIceberg 0.12.0
  read-back, per ruling Q-21c-6 — string casts, lossless widening on every write path,
  `days`/`hours` partitions, lossless `CAST … AS STRING`, nanosecond predicates. `hours()` on ns
  is BLOCKED-ON-FORK F-TSNS-HOUR-1. Round 2 (2026-09-18, ruling Q-21c-8): the type name
  `TIMESTAMP` is always µs — `CAST(ns AS TIMESTAMP)` narrows, `TIMESTAMP` cells in `VALUES`
  floor, `EXPLAIN` lowers. `risk_tier: standard`. Branch `feat/ice-tsns-sql-1`.
  pins: ice-tsns-sql-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- [ice-mixed-case-1-ledger.md](ice-mixed-case-1-ledger.md) —
  **ICE-MIXED-CASE-1 (2026-09-17), in flight:** the Spark door resolves
  mixed-case columns case-insensitively under `spark.sql.caseSensitive=false`
  with Spark's `AMBIGUOUS_REFERENCE` shape, exact when true — Rust repair loop
  plus MERGE fragment scoping, both doors pinned against the recorded live
  oracle. `risk_tier: standard`. Branch `fix/ice-mixed-case-1`.
  pins: ice-mixed-case-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012,
  C-013, C-014, C-015, C-016, C-017, C-018 (run 21b), C-019, C-020, C-021, C-022 (run 22b: the
  debug-wheel deep-`UNION ALL` segfault, the grown-stack repair)
- [ice-registry-sweep-1a-ledger.md](ice-registry-sweep-1a-ledger.md) —
  **ICE-REGISTRY-SWEEP-1 part A (2026-09-17), in flight:** the cutover
  assessment corrections and three registry claims — twelve
  `production-iceberg-status-2026-09-14.md` rows rewritten in place to the
  2026-09-16 measured rating (run at `a92a68db`) plus the dated superseding
  note and the §9 audit trail, and registry claims C-3, C-8, C-9 rewritten
  (C-3, C-9 FIXED → OPEN). Docs-only reading unit, no product change.
  `risk_tier: standard`. Branch `docs/ice-cutover-corrections-1`.
- [ice-nested-evo-1-ledger.md](ice-nested-evo-1-ledger.md) —
  **ICE-NESTED-EVO-1 (2026-09-17), in flight:** adopt a Spark table whose struct, list element
  struct or map value struct gained a child (rating row V2-10d), and nested DDL on both doors
  (`CREATE TABLE` with nested columns, `ADD`/`RENAME`/`DROP COLUMN` on a nested path, the
  required-child refusal). Fork half F-NESTED-EVO-1 (fork PR #292). `risk_tier: high`.
  Branch `fix/ice-nested-evo-1`. **WO U5 PR1 (2026-09-24):** nested TYPE promotion,
  UNSET TBLPROPERTIES IF EXISTS, and namespace SET properties are recorded in C-024 through
  C-028. Round 2 (2026-09-24) narrows C-027 to the per-pair rule and adds C-029 (nested path
  resolution), C-030 (namespace property grammar and SCHEMA_NOT_FOUND) and C-031 (UNSET `IF`
  without `EXISTS`). Round 3 (2026-09-24) adds C-032 (TINYINT/SMALLINT/CHAR/VARCHAR targets
  decided before lowering), C-033 (parse-stage target refusals) and C-034 (C-027 narrowed to
  the pinned Spark type names). The non-primitive target residue is recorded with Spark's text.
  Round 4 (2026-09-24) widens C-033 to the sized STRING/BINARY/FLOAT(p,s)/DOUBLE(p,s)/
  TIMESTAMP_NTZ/DATE/BOOLEAN targets, names C-032's pinned pairs, and adds C-035 (bare DECIMAL
  as decimal(10,0) on the nested route) and C-036 (missing namespace as TABLE_OR_VIEW_NOT_FOUND),
  with residues R-U5-DECIMAL-DEFAULT and R-U5-NS-LEAK. **WO U5 PR2a (2026-09-24):** C-037
  (`ALTER COLUMN … COMMENT` lands the field doc on every Spark spelling, `DBT-COLCOMMENT-1`
  retired), C-038 (its refusals answer Spark's text) and C-039 (the TYPE and hive CHANGE
  routes are unchanged), with the fork-blocked cells recorded as questions. Round 2: C-040
  (a repeated column or a column with its field is `NOT_SUPPORTED_CHANGE_SAME_COLUMN`), C-041
  (action-then-`COMMENT` and single-quoted names are `PARSE_SYNTAX_ERROR`), C-042 (malformed
  list tails) and C-043 (one schema update; the mixed-list refusal). Residues
  R-U5-MIXED-COMMENT-LIST, R-U5-SPEC-LIST-NOT-COMMENT-FIRST, R-U5-OP-NOT-ALLOWED-WRAP,
  R-U5-ENGINE-PREFIX and R-U5-CHANGE-COLUMN-ACTION carry Spark's measured text. Round 3:
  C-044 (a field under a map key is `Cannot alter map keys`, on COMMENT and TYPE), C-045
  (element/value comments add no schema; C-037 narrowed), C-046 (unresolved, then repeat, then
  map key), C-047 (`extra input` only for a last token) and C-048 (unresolved names from parsed
  parts; the three-part name after `USE`). Residues R-U5-MAP-KEY-FIELD-IDS and
  R-U5-DOTTED-TOP-TYPE. Round 4 fold: C-049 (a COMMENT list after another change is the
  mixed-list refusal), C-050 (`IF EXISTS` and `PARTITION` wrappers are `PARSE_SYNTAX_ERROR`),
  C-051 (the named residual refusal), with C-048 naming the MOVE route. R-U5-SPEC-LIST-NOT-COMMENT-FIRST
  is reworded, since the leak followed from deleting the I6 refusal. New residues:
  R-U5-RESIDUAL-COMMENT-SHAPES, R-U5-ADD-COLUMNS-TRAILING-COMMENT, R-U5-IF-EXISTS-ALTER-TABLE and
  R-U5-PARTITION-CHANGE. **WO U5 PR2b (2026-09-24, C-052 through C-057):** format v1 creation,
  the v1 ref-write kernel, transform write orders and their refusals, CREATE BRANCH on an empty
  table, v1 merge-on-read and downgrade refusals. Round 4 (2026-09-25, critic r3): C-055 adds
  the INVALID_TYPED_LITERAL hex refusal, Spark's string unescape, by-value exponent and decimal
  renderings and the empty-segment refusal that commits nothing; C-053 records that the v1
  reader drops non-main refs, so the selector-door kernel call is belt-and-braces until
  F-V1-REFS-1; R-U5-PR2B-OTHER-MALFORMED and R-U5-PR2B-RETENTION-SECOND-COMMIT carry the dated
  V-005 and V-007 residues.
  **WO U5 PR3 (2026-09-25, C-058):** hive-style typed `PARTITIONED BY` columns become table
  columns with identity partition fields (D-X-PARTITIONED-COLDEF); residues R-U5-PR3-MIX-RENDER,
  R-U5-PR3-NON-PRIMITIVE, R-U5-PR3-CTAS-TYPED. **Ruling (2026-09-25, C-059):** `RENAME TO`
  reads its target inside the source catalog like Spark's InMemoryCatalog (D-RENAME-TABLE);
  supersedes ice-catalog-session-1/C-022's cross-catalog half and dbt-1-adapter/C-001's
  `S-RENAME` row. Residues R-U5-PR3-RENAME-CLASS, NESTED-NS-SQL, RENAME-MISSING-SOURCE and
  DBT-RENAME.
- [ice-array-insert-1-ledger.md](ice-array-insert-1-ledger.md) —
  **ICE-ARRAY-INSERT-1 (2026-09-18), in flight:** inserts into array columns answer Spark
  4.1.2 on every door at fork #295 (F-LIST-INSERT-1) — one pin per recorded cell (three
  shapes by five doors by v2/v3) for rows and footer field ids, the eight non-VALUES
  `map_list` cells verbatim under strict xfail (CAST-MAP-SPELL-1, BACKLOG) with
  substitute-source twins, the live Spark-adopts-RePark tier, registry row
  ICE-ARRAY-INSERT-1 FIXED. Test-only, no product change. `risk_tier: standard`.
  Branch `chore/rp-29-fork-pin`.
  pins: ice-array-insert-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [ice-write-options-rp-1-ledger.md](ice-write-options-rp-1-ledger.md) —
  **ICE-WRITE-OPTIONS-RP-1 (2026-09-18), in flight:** a caller-supplied
  `snapshot-property.replace-partitions` value wins on a replace-partitions commit at
  fork #298 (F-RP-SUMMARY-USER-1) — the five-cell Spark 4.1.2 oracle, the recorder, one
  pin per cell for the commit flag and the newest summary values, the live
  re-derivation of one cell, registry row ICE-WRITE-OPTIONS-1-R-RP FIXED. Test-only,
  no product change. `risk_tier: standard`. Branch `chore/rp-30-fork-pin`.
  pins: ice-write-options-rp-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [ice-list-null-1-ledger.md](ice-list-null-1-ledger.md) —
  **ICE-LIST-NULL-1 (2026-09-18), in flight:** DELETE and UPDATE with IS NULL on
  nested columns answer Spark 4.1.2 at fork #299 (F-LIST-NULL-ACCESSOR-1) — the
  128-cell Spark oracle, the recorder, one pin per cell for ok, ids and operation
  plus the delete-file / DV count parametrization, the `map_from_arrays` empty-map
  substitute seed, the sixteen copy-on-write compound-predicate cells verbatim
  under strict xfail (fork #299 residue), the sixteen merge-on-read IS NOT NULL
  cells pinning RePark's measured 1 file against Spark's 2, the live re-derivation
  of one cell per shape, registry row ICE-LIST-NULL-1 FIXED. Test-only, no product
  change. `risk_tier: standard`. Branch `chore/rp-31-fork-pin`.
  pins: ice-list-null-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [ice-count-fold-1-ledger.md](ice-count-fold-1-ledger.md) —
  **ICE-COUNT-FOLD-1 (2026-09-19), in flight:** `count(*)` folds from an exact Iceberg
  row count again — both integer-literal rules (`SparkIntegerLiteral`, the Spark door's
  `SparkIntegralLiteral`) keep a count-of-`1` argument as DataFusion's `Int64(1)`
  expansion through one shared walker; names, LongType and values unchanged on both
  doors. `risk_tier: standard`. Branch `perf/ice-count-fold-1`.
  pins: ice-count-fold-1/C-001, C-002, C-003, C-004, C-005, C-006
- [ice-list-null-2-ledger.md](ice-list-null-2-ledger.md) —
  **ICE-LIST-NULL-2 (2026-09-19), in flight:** copy-on-write DELETE with a compound
  predicate over a nested column answers Spark — the identity path declines
  non-primitive selections to the fork DELETE path on both SQL doors, the six-cell
  Rust door battery and the seven gate unit pins, the 128-cell Python pins flipped
  from strict-xfail to plain (the eight OR cells pinning RePark's measured
  `overwrite` operation), registry row ICE-LIST-NULL-1 corrected to RePark-side
  FIXED. Product-path valve plus pins. `risk_tier: standard`. Branch
  `fix/ice-list-null-2`.
  pins: ice-list-null-2/C-001, C-002, C-003, C-004, C-005, C-006
- [ice-rowid-order-1-ledger.md](ice-rowid-order-1-ledger.md) —
  **ICE-ROWID-ORDER-1 (2026-09-18), in flight:** one statement's v3 row ids are
  deterministic at fork #300 (F-ROWID-ORDER-1) — both Spark recordings (a/b/c
  twelve-run, eight-category six-run per configuration), the one recorder, twelve
  runs per a/b/c shape giving one mapping equal to Spark's recorded a:0, b:100,
  c:200, twelve eight-category runs giving one ascending mapping with Spark's
  default hash-partitioner order as DECLARED divergence, registry row V3-COV-3
  FIXED with the residual stated and V3-FILEORDER-1 true on every writer.
  Test-only, no product change. `risk_tier: standard`. Branch `chore/rp-31-fork-pin`.
  pins: ice-rowid-order-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [ice-evo-dml-1-ledger.md](ice-evo-dml-1-ledger.md) —
  **ICE-EVO-DML-1 (2026-09-17), in flight:** MERGE / UPDATE / DELETE after `ADD COLUMN` or
  `RENAME COLUMN` with no write since answer Spark 4.1.2 instead of refusing `Column … not
  found in table`, and a rename that swaps two names no longer writes one column's values under
  the other — the DML target scan plans the pinned snapshot and reads it under the current
  schema. v2/v3 × CoW/MoR, both doors, a Spark-created evolved table adopted. Registry rows
  ICE-EVO-DML-1, ICE-EVO-SWAP-1. `risk_tier: high`. Branch `fix/ice-evo-dml-1`.
- [ice-promote-read-1-ledger.md](ice-promote-read-1-ledger.md) —
  **ICE-PROMOTE-READ-1 (2026-09-16), in flight:** reads and DML after a legal
  `ALTER COLUMN … TYPE` promotion answer Spark 4.1.2 — range / long-`IN` filters, promoted
  partition sources, MERGE keyed on the promoted column, range UPDATE, single-era DML,
  promoted-identity-partition DML and overwrite, Spark-created adopted tables, v2 and v3.
  Fork half F-PROMOTE-READ-1 (manifest values read under the promoted type); RePark half
  the DML target-scan conform. Registry rows ICE-PROMOTE-READ-1, ICE-PROMOTE-DML-1,
  ICE-PROMOTE-PARTITION-1 (FIXED 2026-09-16). `risk_tier: high`. Branch `fix/ice-promote-read-1`.
- [ice-column-reorder-1-ledger.md](ice-column-reorder-1-ledger.md) —
  **ICE-COLUMN-REORDER-1 (2026-09-17), in flight:** `ALTER COLUMN … FIRST/AFTER`
  column moves on Iceberg tables (rating row V2-10b) — move as a schema update with
  unchanged field ids, or a typed Spark-shaped refusal with a dated registry row.
  `risk_tier: standard`. Branch `fix/ice-column-reorder-1`.
  pins: ice-column-reorder-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014
- [ice-page-prune-1-ledger.md](ice-page-prune-1-ledger.md) —
  **ICE-PAGE-PRUNE-1 (2026-09-19), in flight:** the RePark pins for page-level
  row selection (slate unit 1, run 24a) — the Spark 4.1.2 recorder, the five
  rewritten fixture warehouses plus compacted truth, both-door answer pins
  with lineage, RePark-written self-consistency and page-index pins, and the
  live re-derivation tier; the rewritten `del_v2` and the bare-decimal ranges
  pin loud divergences. `risk_tier: standard`. Branch `perf/ice-page-prune-1`.
  pins: ice-page-prune-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [never-oom-panic-1-ledger.md](never-oom-panic-1-ledger.md) —
  **NEVER-OOM-PANIC-1 (2026-09-16), in flight:** the tight-pool NLJ race — `inner future
  panicked during poll` versus the typed refusal — fixed at the root so the nested-loop
  join ends in `ResourcesExhausted` or spills on every scheduling, per owner ruling
  Q-17c-7. `risk_tier: standard`. Branch `fix/never-oom-panic-1`.
  pins: never-oom-panic-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [fnp-agg-1-ledger.md](fnp-agg-1-ledger.md) —
  **FNP-AGG-1 slice (d) (2026-09-21), in flight:** grouping_id plus the shared
  foundation (dispatch widening, refusal mapping, installer, oracles) ported onto
  current main; slices (a), (b) and (c) follow after it lands.
  `risk_tier: standard`. Branch `fix/fnp-agg-grouping`.
  pins: fnp-agg-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [registry-16b-1-ledger.md](../completed/registry-16b-1-ledger.md) —
  **REGISTRY-16B-1 (2026-09-15), in flight:** three BACKLOG registry rows with their pins —
  CONF-UNSET-1, CONF-WAP-1 (run 15c's P2 findings for the RuntimeConfig surface, measured on live
  PySpark 4.1.2) and IO-JDBC-FORMAT-1 (owner ruling Q-15B-4). No product change. Attested complete.
  `risk_tier: standard`. Branch `docs/registry-16b-1`.
  pins: registry-16b-1/C-001, C-002, C-003
- [ice-branch-ops-1-ledger.md](ice-branch-ops-1-ledger.md) —
  **ICE-BRANCH-OPS-1 (2026-09-17), in flight:** `fast_forward`, `cherrypick_snapshot`,
  `set_current_snapshot` and `rollback_to_timestamp` as RePark-side procedure wiring over
  the fork's `ManageSnapshots` / `Transaction::cherry_pick` (no fork change), with a
  recorded Spark oracle (`branch_ops_1_truth.json`), red-first pins on both doors, and
  registry rows REF-5–REF-8. `risk_tier: standard`. Branch `fix/ice-branch-ops-1`.
  pins: ice-branch-ops-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- [ice-procs-route-1-ledger.md](ice-procs-route-1-ledger.md) —
  **ICE-PROCS-ROUTE-1 (2026-09-19), in flight:** `ancestors_of`,
  `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path` as
  Spark-door routing over the fork's maintenance actions (no fork change), with
  a 27-cell recorded Spark oracle, red-first pins on both doors, registry row
  `ICE-PROCS-ROUTE-1`, and a strict-xfail pair plus fork ask for the
  incremental version range. `risk_tier: standard`. Branch `fix/ice-procs-route-1`.
  pins: ice-procs-route-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
- [df-rust-3-ledger.md](df-rust-3-ledger.md) —
  **DF-RUST-3 (2026-09-15), in flight:** `DataFrame.freqItems`,
  `DataFrameStatFunctions.freqItems`, and `DataFrame.transpose` implemented Rust-first —
  a `FreqItemCounter`-exact DataFusion UDAF and an eager `ResolveTranspose`-equivalent
  kernel in repark-core, `#[pyfunction]` shims in repark-python, thin facade bindings.
  Oracle: run-16b PySpark 4.1.2 cells (`freq_*`, `transpose_*`).
  `risk_tier: standard`. Branch `feat/df-rust-3`.
  pins: df-rust-3/C-001, C-002, C-003, C-004, C-005, C-006
- [df-subquery-1-ledger.md](df-subquery-1-ledger.md) —
  **DF-SUBQUERY-1 (2026-09-15), in flight:** `DataFrame.scalar` / `.exists` /
  `.lateralJoin` / `.asTable` over `Column.outer` — `Expr::ScalarSubquery`,
  `Expr::Exists`, `OuterReferenceColumn` and `LogicalPlan::Subquery` built in
  Rust (repark-python + repark-core), EXISTS-in-projection and
  lateral-projection-hoist rewrites in the core optimizer list, the
  `SCALAR_SUBQUERY_TOO_MANY_ROWS` execution guard, `TableArg` + the UDTF
  table-argument path, and the Spark-classic unqualified-resolution quirk
  pinned both ways. Round 3 (2026-09-16): the Grok reviews' four P2s — the
  hoist keeps the right side's `SubqueryAlias` qualifier, a correlated
  `LIMIT` is stripped into `__repark_any_row` / `__repark_single_row`, the
  `lateral_tvf_like` pin's claim is narrowed to what it reads with both
  residuals disclosed, and `how="left"` gains a red-capable qualified pin;
  P-201..P-206 / Y-1..Y-3 recorded as residue.
  `risk_tier: standard`. Branch `feat/df-subquery-1`.
  pins: df-subquery-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [subq-cells-1-ledger.md](subq-cells-1-ledger.md) —
  **SUBQ-CELLS-1 (2026-09-16), in flight:** the run-18b follow-up folding the six
  live-measured subquery cells (`lateral_inner_qualified_sql`,
  `lateral_left_qualified_sql`, `lateral_sql_outer_expr_aliased_filter`,
  `lateral_sql_outer_expr_aliased_on`, `lateral_sql_outer_expr_aliased_plain`,
  `scalar_limit1_correlated_sql`, recorded on PySpark 4.1.2 by
  `oracle-input/probe_subq_wanted.py`, 2026-09-16) into
  `facade_df_subquery_oracle.json` and repointing the round-3 shape-only pins —
  two repointed `_assert_frame` pins, a second assertion on the aliased-hoist
  pin, two new SQL-door qualified-lateral pins, and ruling S-1's shape
  assertions on the correlated `LIMIT 1` SQL door. No product code.
  `risk_tier: standard`. Branch `test/subq-cells-1`.
  pins: subq-cells-1/C-001, C-002, C-003, C-004, C-005
- [facade-5-ledger.md](facade-5-ledger.md) —
  **FACADE-5 step 0 (2026-09-14), in flight:** the display renderer's fetch/format
  split baseline (`display.py` bodies + `plan_collapse.py`/`polars_cells.py`
  formatters, the audit §6 UNMEASURED cell), the renderer × truncation-rule pin
  census, goldens for the pairs no §8 pin binds (recorded from base, mutation
  proven), and the step-1 target — a measured format wall and its Rust move, or
  the smallest byte-identical consolidation naming what stays for `eager.py`.
  No product change under `python/repark/src/` or `crates/`.
  `risk_tier: standard`. Branch `perf/facade-5-s0`.
  pins: facade-5/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [array-null-1-ledger.md](array-null-1-ledger.md) —
  **ARRAY-NULL-1 (2026-09-14):** `F.array_append`/`F.array_prepend` lower through
  `spark_array_append_udf`/`spark_array_prepend_udf` — a `ScalarUDF` that delegates
  to DataFusion's kernel and grafts the input array's outer null buffer back
  (route (a); the CASE route died at depth 40 and no lateral plan-level alias
  exists). One native call per facade level: depth-40 RSS ~2.0 MB vs 577 MB at
  depth 16 before; the SQL door answers the same arm in Spark `(array, element)`
  order, and the depth-40 memory pin runs by default on both functions.
  Measurement script: [array-null-1-spikes/](array-null-1-spikes/map.md).
  `risk_tier: standard`. Branch `fix/array-null-1`.
  pins: array-null-1/C-001, C-002, C-003, C-004, C-005, L-1, L-2, L-3, L-5, L-6,
  L-7, L-8, L-9, L-10, L-11, L-12, L-13, P2-1, P3-1
- [ice-catalog-cache-1-ledger.md](ice-catalog-cache-1-ledger.md) —
  **ICE-CATALOG-CACHE-1 (2026-09-19), in flight:** the session's Iceberg metadata and manifest
  caches reach the Glue and S3 Tables builders (fork PR #311's handles); the credential-context
  scope ruling (the fork's public selector derivation, else per instance); evictions surfaced
  through `iceberg_metadata_cache_report` and the bench. The before/after pair is
  `TBD-orchestrator`. `risk_tier: standard`. Branch `perf/ice-catalog-cache-1`.
  pins: ice-catalog-cache-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012
- [ice-footer-cache-1-ledger.md](ice-footer-cache-1-ledger.md) —
  **ICE-FOOTER-CACHE-1 (2026-09-19), in flight:** the session owns one Parquet footer cache
  (fork PR #316's `ParquetFooterCache`), sized by `repark.iceberg.footerCacheBytes` (default
  64 MiB, `0` disables) and handed to the memory, Glue and S3 Tables builders by `wire_caches`;
  `iceberg_footer_cache_stats()` and the bench report its counters. The before/after pair is
  `TBD-orchestrator`. `risk_tier: standard`. Branch `perf/ice-footer-cache-1`.
  pins: ice-footer-cache-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010
- [ice-read-perf-0-ledger.md](ice-read-perf-0-ledger.md) —
  **ICE-READ-PERF-0 (2026-09-19), in flight:** the Iceberg I/O counting layer (a counting
  `StorageFactory` whose counters the session owns; Glue and S3 Tables wrap exactly the fork
  default) and the `ice_read_perf` bench bed (setup, cold / warm / concurrent runs, the R-3
  3 GiB size flag with exit 3). No product behaviour change. Every clause PROVEN, attestation
  complete. `risk_tier: standard`. Branch `perf/ice-read-perf-0`.
  pins: ice-read-perf-0/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010
- [ice-bench-baseline-1-ledger.md](ice-bench-baseline-1-ledger.md) —
  **ICE-BENCH-BASELINE-1 (2026-09-19), in flight:** the read bench's own "before" switch —
  a bare `run --baseline` flag that builds every run session with the shared caches off and
  the fork's page-index row selection disabled, reports `baseline` / `baseline_switches` in
  the JSON and the baseline in the table header, plus the boolean `baseline` dispatch input
  on the `ice-read-perf-bench` job. No product behaviour change. Every clause PROVEN. What it
  cannot switch off (timestamp pushdown, the `count(*)` fold) is recorded in the ledger, so
  the number is never read as pre-campaign main. `risk_tier: standard`. Branch
  `perf/ice-bench-baseline-1`.
  pins: ice-bench-baseline-1/C-001, C-002, C-003, C-004, C-005
- [cast-ts-string-1-ledger.md](cast-ts-string-1-ledger.md) —
  **CAST-TS-STRING-1 (2026-09-19), in flight:** `CAST(<string> AS TIMESTAMP)` follows Spark
  4.1.2's `stringToTimestamp` on every door — one Rust kernel
  (`repark_functions::spark_string_timestamp`) behind `CAST`, `TRY_CAST`, `Column.cast`,
  one-argument `to_timestamp` and time travel; a 604-cell recorded oracle plus red-first pins;
  ICE-TT-RESOLVE-1's cast-gap strict xfails flipped. `risk_tier: standard`. Branch
  `fix/cast-ts-string-1`.
  pins: cast-ts-string-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012
- [cast-map-spell-1-ledger.md](cast-map-spell-1-ledger.md) —
  **CAST-MAP-SPELL-1 (2026-09-19), in flight:** `CAST(… AS MAP<…>)` and
  `.cast(MapType)` answer Spark 4.1.2 on every door — a cast-UDF plus token-rewrite
  feature (stock sqlparser has no `MAP<…>` type and DataFusion 54 plans
  `SQLDataType::Map` as unsupported); 21-cell recorded oracle plus red-first pins.
  Steps 3–5 (claude-opus-5): the `repark_functions::cast_map` kernel serves every door,
  the old refusal pins flip, every clause is PROVEN and the attestation is complete.
  `risk_tier: standard`. Branch `fix/cast-map-spell-1`.
  pins: cast-map-spell-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010
- [abs-expr-1-ledger.md](abs-expr-1-ledger.md) —
  **ABS-EXPR-1 (2026-09-13), in flight:** `F.abs` / `F.cbrt` / `F.nullif` lower to one
  native `call_scalar` each (`expr_fn::abs` / `cbrt` / `nullif`) — the facade `when(...)`
  rewrite embedded its child 3×/2× per level, so nested chains were exponential in native
  memory (run 9 OBS-R9-6 / INC-R9-1: aborting at depth ~14, 84 GB uncapped). The depth-40
  memory pin runs in a subprocess under `RLIMIT_AS` with a per-level bound exit. The
  `when(...)` audit table covers `nullif` (lowered), `array_append`'s `_glue_element` and
  the `DataFrame.replace` dict loop (2× embeds, no one-call native answer — each needs
  its own card; `dataframe/core.py` is fenced to another lane).
  `risk_tier: standard`. Branch `fix/abs-expr-1`.
  pins: abs-expr-1/C-001, C-002, C-003, C-004, C-005
- [ap-1-close-1-ledger.md](ap-1-close-1-ledger.md) —
  **AP-1-CLOSE-1 (2026-09-12), in flight:** `projected_files_at_target` re-read as an
  upper bound from the inputs' compressed bytes — the 20 % target retires (S2-27),
  no formula change (D-1), the note and frame `notes` say "upper bound", the pin
  reproduces the three AP-0 beds' RP-18 frame (bound ≥ live actual ≤ 2×, ranking
  identical, red first by doctoring the bound), residue AP-1-R-001 closes
  2026-09-12, the maintenance guide's S2-24 known-issues line retires, and the
  AP-1 remeasure ledger departs to `completed/` through the lifecycle tool.
  `risk_tier: standard`. Branch `chore/ap-1-close-1`.
  pins: ap-1-close-1/C-001, C-002, C-003, C-004, C-005
- [ap-3-ledger.md](ap-3-ledger.md) —
  **AP-3 step 1 (2026-09-12), in flight:** `projected_files_at_target` multiplies
  each item's footer `total_uncompressed_size` sum by `byte_ratio` — compression
  counted once (S2-23, Q-1 ruled); the 0.55 fallback estimates uncompressed as
  `file_size_in_bytes / 0.55`; the residue note, the frame `notes` and the
  maintenance guide's S2-24 known-issues line move with it.
  `risk_tier: standard`. Branch `feat/ap-3`.
  pins: ap-3/C-001, C-002, C-003, C-004, C-005, C-006
- [bl-11-numeric-binary-ledger.md](bl-11-numeric-binary-ledger.md) —
  **BL-11 (2026-09-16), in flight:** numeric to BINARY under runtime ANSI (batch-17
  oracle): ANSI-off big-endian encode of the integrals via the `IntToBinaryCast`
  analyzer rule, ANSI-on integral refusal with `CAST_WITH_CONF_SUGGESTION`, the
  never-castable sources refusing in both modes. `risk_tier: standard`.
  Branch `feat/bl-11-numeric-binary`.
  pins: bl-11-numeric-binary/C-001, C-002, C-003, C-004, C-005, C-006
- [cfg-1-ledger.md](cfg-1-ledger.md) —
  **CFG-1 step 1 (2026-09-09), in flight:** `repark.toml` discovery, profile merge and
  `${VAR}` interpolation — `discovery.rs` (`$REPARK_CONFIG` → `./repark.toml` →
  `~/.config/repark/repark.toml`, empty disables, named-but-missing refuses), `profile.rs`
  (`[default]` + `[<profile>]` deep merge, unknown keys refuse with the key path),
  `interpolate.rs` (missing variable refuses naming path and variable). All six clauses
  PROVEN, 24 pins in `config_file/tests.rs`; `sources.rs` / `redact.rs` stay placeholders
  for step 2. `risk_tier: standard`. Branch `feat/cfg-1`.
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006
- [df-colregex-1-ledger.md](df-colregex-1-ledger.md) —
  **DF-COLREGEX-1 step 1 (2026-09-11), in flight:** `colRegex`/`col_regex` reach the
  measured Spark contract — a backticked pattern returns the `RegexColumn` marker
  (`python/repark/src/repark/spark/dataframe/colregex.py`) that `select` expands to
  every full-matching column in frame order, case-insensitively, zero matches
  included; a bare pattern resolves as a literal column name at the call. `drop`
  no-ops the marker; `withColumn`/`groupBy`/`orderBy`/`.alias` on it refuse. Both
  pins flipped to parity (`test_colregex_backtick_spelling_parity`,
  `test_colregex_multi_match_expands`, `test_colregex_duplicate_names_expand_positionally`);
  EX-DF-1 FIXED both arms. Remediation round (ruling S2-21, review P2-1):
  `expand_col_regex` binds matches only — 16.7–23.5× on sparse matches, parity at
  500 — while overlay/duplicate-name frames keep the positional path. Facade-only —
  no engine change. `risk_tier: standard`. Branch `fix/df-colregex-1`.
  pins: df-colregex-1/C-001, C-002, C-003, C-004, C-005, C-006
- [df-describe-str-1-ledger.md](df-describe-str-1-ledger.md) —
  **DF-DESCRIBE-STR-1 (2026-09-11), in flight:** `describe`/`summary` answer Spark's
  ordered stat rows on string columns — `mean`/`stddev` over `try_cast(col AS DOUBLE)`
  (NULL for `"a"`/`"b"`, `6.0` for `"10","2","a"`), non-numeric non-string columns
  skipped by the bare forms and refused with `PySparkValueError` when named, UNION ALL
  legs ordered by a stat ordinal. §7 EX-DF-4 FIXED whole; EX-DF-15 narrowed to the
  bare-`summary()` percentile refusal. `risk_tier: standard`. Branch
  `fix/df-describe-str-1`.
  pins: df-describe-str-1/C-001, C-002, C-003, C-004
- [ex-29-class-remainder-ledger.md](ex-29-class-remainder-ledger.md) —
  **EX-29 (2026-09-11), in flight:** the v1.1 example backfill's class-surface
  remainder — the 29 non-`F.*` backlog names at base `a10062b8`. Re-measured on
  live PySpark 4.1.2 (ANSI on, UTC, zulu-17): zero names coverable, 23 stay with
  their existing §7 rows (EX-DF-1/2/3/4/7/8/17/19, EX-CAT-1/2, EX-W2-1,
  EX-IO-7), the six `Column.*` plumbing names stay pending an owner ruling on
  inventory narrowing. Pin gaps filled: `get_database` / `list_databases` snake
  legs, the `describe` string-column raise, the `colRegex` multi-match arm.
  `risk_tier: standard`. Branch `docs/ex-29-class-remainder`.
  pins: ex-29-class-remainder/C-001, C-002, C-003, C-004, C-005, C-006
- [ex-30-functions-remainder-ledger.md](ex-30-functions-remainder-ledger.md) —
  **EX-30 (2026-09-11), in flight:** the v1.1 example backfill's `F.*` remainder —
  the 99 `F.*` backlog names at base `f413241b`. Measured on live PySpark 4.1.2
  (ANSI on, UTC, zulu-17): 9 covered by two new `docs/examples/functions/` scripts
  (`bitmap.py`, `udf.py`), 89 stay with their §7 rows (87 pre-existing plus
  `from_xml` / `schema_of_xml` under the new EX-FN-22), `F.PythonUDFColumn` stays
  pending an owner ruling on inventory narrowing. New rows EX-FN-22 (XML E1 stubs)
  and EX-FN-23 (the `udf` / `pandas_udf` factory return-type arm, BACKLOG ARM on
  covered names); pins in `test_examples_functions_b.py`. Backlog 128 → 119.
  `risk_tier: standard`. Branch `docs/ex-30-functions-remainder`.
  pins: ex-30-functions-remainder/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [ex-31-inventory-plumbing-ledger.md](ex-31-inventory-plumbing-ledger.md) —
  **EX-31 step 1 (2026-09-12), in flight:** the example inventory drops the
  seven measured non-PySpark plumbing names — six `Column.*` select-boundary
  helpers plus `F.PythonUDFColumn` — through the gate's named
  `INVENTORY_EXCLUSIONS` list (owner ruling S2-22). Backlog 119 → 112,
  `inventory.txt` regenerated by `--write-inventory` (927 → 920 rows); the raw
  walk still reports the seven so the API-freeze register keeps them frozen.
  `risk_tier: standard`. Branch `docs/ex-31-inventory-plumbing`.
  pins: ex-31-inventory-plumbing/C-001, C-002, C-003, C-004, C-005, C-006
- [facade-1-ledger.md](facade-1-ledger.md) —
  **FACADE-1 step 1 (2026-09-12), in flight:** the Arrow C Stream boundary —
  `__arrow_c_stream__` capsules both ways, pyarrow optional at import and for
  polars/pandas capsule consumers, `pa_ipc.new_stream` kept as the version-skew
  fallback. `risk_tier: standard`. Branch `feat/facade-1`.
  pins: facade-1/C-001, C-002, C-003, C-004, C-005, C-006
- [facade-2-ledger.md](facade-2-ledger.md) —
  **FACADE-2 step 3 (2026-09-13), in flight:** Group-1 `PyColumn.sql` sites are typed
  native constructors (`lit_timestamp`/`lit_date`/`lit_time`/`lit_array_cast`/`pi`/`uuid`
  in `column/display/construct.rs`); the generic `name(args)` render moved to
  `PyColumnParts.call_scalar`; `F.expr` stays the sole parser caller. 39
  generic-builder goldens proven byte-identical to a base release re-record.
  C-017 release: depth-100 chains +1.2%, `lit(datetime)` −98% (no engine re-parse).
  S2-21 remediation: `call_scalar` drops the second `Expr` clone and extracts part
  lists in one `PyBackedStr` pass; cfa8ad2e→fix −17.5% on both deciding chains.
  Branch `feat/facade-2-s3`.
  pins: facade-2/C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021
- [facade-4-ledger.md](facade-4-ledger.md) —
  **FACADE-4 steps 0–1 (2026-09-14), in flight:** step 0 is measurement and pins
  only — release baseline cells isolating `repark_type_to_arrow` /
  `struct_type_from_arrow` / DDL parse+write per schema plus conversion's share
  of end-to-end walls, the three-table agreement census (facade `types.py` vs
  `_csv_smart` rungs vs the reader lattice + Rust
  `spark_ddl_type_name`/`arrow_type_key`), DDL/Arrow goldens for every F1 type
  class with a mutation proof, and the step-1 target list. Step 1 is the
  correctness consolidation — one Rust `type_table` owns Arrow ↔ Spark
  descriptor ↔ DDL semantics with every surface keeping its step-0 answer
  byte-for-byte — plus the S2-21 remediation findings table (P1-DTYPES, P2-DICT,
  P3-*), the critic-logic remediation chains (L-001..L-011 → C-020..C-033),
  and the round-2 census alignment (corrected D1–D24 pins, R2-P3
  dispositions marked).
  `risk_tier: standard`. Branch `perf/facade-4-s1`.
  pins: facade-4/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019
  No product change under `python/repark/src/` or `crates/`.
  `risk_tier: standard`. Branch `perf/facade-5-s0`.
  pins: facade-5/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  No product change under `python/repark/src/` or `crates/`.
  `risk_tier: standard`. Branch `perf/facade-5-s0`.
  pins: facade-5/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
  **JAVA-DOUBLE-FD-1 round 2 (2026-09-15), in flight:** the Q19 reviewer cells —
  `%F` refuses, NaN takes no sign prefix, `#` forces the point, suffix casts run
  over real columns through a shared kernel, and the float text paths render
  through reused batch buffers. L-004/L-001/L-002/L-003/P2-1/P2-4 PROVEN with
  oracle pins on both doors; P2-2 stays OPEN residue.
  `risk_tier: standard`. Branch `feat/java-double-fd-1`.
  pins: java-double-fd-1/C-001, C-002, C-003, C-004, C-005, C-006
- [orphan-s3tables-1-ledger.md](orphan-s3tables-1-ledger.md) —
  **ORPHAN-S3TABLES-1 step 1 (2026-09-12), in flight:** `remove_orphan_files` refuses loud
  on an `s3tables`-kind catalog before any IO — table buckets answer `ListObjectsV2` 405 —
  naming the table and the service's `unreferencedFileRemoval` maintenance as the remedy
  (D-1); `dry_run` refuses identically; `run_maintenance` keeps the orphan step in the plan
  but reports it `skipped` with that reason on the dry run and on apply (D-2); the refusal
  keys on `LocationPolicy::ServiceManagedLocation` so the pins need no AWS (D-3); the
  maintenance guide's S3 Tables paragraph and the parity registry row land with it (C-005).
  `risk_tier: standard`. Branch `fix/orphan-s3tables-1`.
  pins: orphan-s3tables-1/C-001, C-002, C-003, C-004, C-005
- [ice-hadoop-vn-1-ledger.md](ice-hadoop-vn-1-ledger.md) —
  **ICE-HADOOP-VN-1 (2026-09-17), in flight:** the stale Hadoop `vN` writer raises
  loud and loses nothing — fork #286 exclusive-creates `vN` names
  (`75da2b58`, in-tree via RP-21); the RePark side pins the `conc` / `conc2`
  shapes, the re-register recovery, the Spark stale-commit oracle, and the
  DataFrame-door stale writers, plus registry row ICE-HADOOP-VN-1.
  `risk_tier: standard`. Branch `fix/ice-hadoop-vn-1`.
  pins: ice-hadoop-vn-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [perf-describe-1-ledger.md](perf-describe-1-ledger.md) —
  **PERF-DESCRIBE-1 (2026-09-11), in flight:** `describe`/`summary` aggregate in one
  pass — one `AggregateExec` computes count/avg/stddev/min/max per column over the
  frame's own plan (native column API, no SQL text, no temp view), then the requested
  summary rows are projected as literal `VALUES` in Spark's order so the stat ordinal
  and its `ORDER BY` are gone. Casts are emitted only where engine formatting is
  load-bearing; measured medians improve on every harness shape (200k numeric 0.132 →
  0.098 s, 50×10k wide 1.164 → 0.993 s, 200k strings 0.194 → 0.191 s, 200k mixed
  0.197 → 0.139 s). `risk_tier: standard`. Branch `perf/describe-1`.
  pins: perf-describe-1/C-001, C-002, C-003, C-004
- [perf-unpivot-1-ledger.md](perf-unpivot-1-ledger.md) —
  S2-21 re-check of the step-2 remediation: 500-column describe 8.16 s, no P1 / P2.
  **PERF-UNPIVOT-1 (2026-09-12), in flight:** step 1 (#542) shipped native
  `stack(n, expr…)` / `UnpivotExec` in `repark-core`, Spark SQL rewrite, `F.stack`,
  linearity exponent 0.91 at 50/250/500; `interleave` once per stacked column and
  stream each input batch (C-006, C-007). Step 2 (`perf/unpivot-1-s2`) moves
  `describe`/`summary` back to a pure plan — chunked aggregates → one stack-order
  string-cast projection → `UnpivotExec`; the `mapInArrow` bridge and
  `_summary_unpivot` are deleted.
  `risk_tier: standard`. Branch `perf/unpivot-1` / `perf/unpivot-1-s2`.
  pins: perf-unpivot-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012, C-013
- [sepmo-e0-e1-ledger.md](sepmo-e0-e1-ledger.md) —
  **SEPMO-E0E1 (2026-09-06), in flight, round 3:** telemetry inventory (E-0) and usage
  collector (E-1). Minority truncated JSONL and exit-without-terminal are degraded
  records; majority-bad still fails. Muse tokens come from the session store
  (`runs.tsv` join, `.msp-view-v1` pinned); cost is still absent. Grok live keys
  include `cache_read_input_tokens` and `modelUsage`. OpenCode sqlite has token
  and cost columns. Claude transcripts are not accessible. Collector is
  `scripts/sepmo_usage.py`. `risk_tier: standard`. Branch
  `sepmo/e0-e1-usage-collector`.
  pins: sepmo-e0-e1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [sepmo-e2-ledger.md](sepmo-e2-ledger.md) —
  **SEPMO-E2 (2026-09-06), in flight, round 3:** compact role packets. Packet
  format v1 (eight field groups, stable prefix then dynamic, source identity,
  version), assembler `scripts/sepmo_packet.py` plus
  `scripts/sepmo_packet_extract.py` (`build` / `check` / `diff`), three
  converted campaign briefs as fixtures plus two prefix-only briefs,
  constraint-preservation tests (sidecar `STABLE_RULES` equality, trailer,
  re-render, `bash -n` through `build`/`check`, unbackticked boundary paths
  through `build`, prefix-negating phrases), and a baseline table against E-0
  cached/uncached ratios with no token-savings claim. Adoption proposal names
  `--brief` / `--followup`. `risk_tier: standard`. Branch
  `sepmo/e2-compact-packets`.
  pins: sepmo-e2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [set-ansi-runtime-1-ledger.md](set-ansi-runtime-1-ledger.md) —
  **SET-ANSI-RUNTIME-1 round 1 (2026-09-15), in flight:** runtime `SET` and
  `spark.conf.set` of `spark.sql.ansi.enabled` and `spark.sql.session.timeZone`
  apply to the live session through a per-query config snapshot (owner Q-15c-3).
  Round 1 covers C-001…C-005; BL-11 (C-006) is round 2. `risk_tier: standard`.
  Branch `feat/set-ansi-runtime-1`.
  pins: set-ansi-runtime-1/C-001, C-002, C-003, C-004, C-005, C-006
- [silver-s0-ledger.md](silver-s0-ledger.md) —
  **SILVER-S0 (2026-09-12), in flight:** READING unit — contract and storage
  feasibility for the deterministic silver-layer compiler (epic §17 S-0).
  Clause verdicts from the fork pin `3ebf7d36` and RePark write adapter
  source; probe tests live only in `fork/` on `probe/silver-s0`.
  `risk_tier: standard`. Branch `docs/silver-s0`.
  pins: silver-s0/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- [silver-s1-ledger.md](silver-s1-ledger.md) —
  **SILVER-S1 (2026-09-12), in flight:** typed `SilverPlan` in `crates/repark-core/src/silver/`
  — TOML parse with key-path refusals, closed operation enums, parse-time structural
  validation, canonical identity bytes, deterministic `explain()`. No data execution.
  `risk_tier: standard`. Branch `feat/silver-s1`.
  pins: silver-s1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [v3-0-charter-ledger.md](v3-0-charter-ledger.md) —
  **V3-0 (2026-08-21):** the format-v3 scope audit, and the defect it found. Intended as a
  charter with no product change and it does not close that way. **Read §3 first**:
  `rewrite_data_files` had no format-version check and reassigned every row's lineage on a v3
  table while returning the correct rows, where Spark carries lineage through unchanged. It is
  reachable on a v3 table that was already in the catalog, which is the drop-in case, so the
  guard shipped with the audit (`V3-LINEAGE-1`). §2 is the other half of the news, and it is
  good: v3 reads and v3 appends are already correct, round-tripped through Spark, including the
  row lineage the format mandates. §4 answers A12's stated first question — adoption, through
  `register_table`, whose Spark signature is measured there.
  Critic r3 PASS (2026-09-07): padded outer-join side disclosed in `FNP8-NULLABILITY`; counts trued up.
- [write-distribution-2-ledger.md](write-distribution-2-ledger.md) —
  **WRITE-DISTRIBUTION-2 (2026-09-06), in flight:** the hash distribution rule on the
  partitioned stream write paths — the funnel dispatcher routes each batch by hash of the
  writer's partition values, so one value lands in one writer: `INSERT OVERWRITE` and MERGE
  inserts go 32 → 8 data files (Spark's count) at 1e6. Plain `INSERT INTO` and
  `saveAsTable(append)` stay at 64 — fork-owned, the halted question. Closes the WD1 review
  gaps F-1 (mutation-proven cast pin), F-2 (partitioned abort pin) and F-4 (§8 counts). No
  dependency, no spawn. `risk_tier: standard`. Branch `perf/write-distribution-2`.
  pins: write-distribution-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [ice-nan-pushdown-1-ledger.md](ice-nan-pushdown-1-ledger.md) —
  **ICE-NAN-PUSHDOWN-1 (2026-09-17), in flight:** NaN filter pushdown end to end
  against Spark — the fork #284 `IsNan` / `NotNan` / `IsNan OR In(rest)` rewrite
  at pin `75da2b58` (RP-21), proven by the clause grid on double and float over
  NaN-only / mixed / two-file shapes at v2 and v3 on RePark- and Spark-written
  tables, both doors, plus DELETE/UPDATE row outcomes; Spark oracle recorded as
  truth JSON plus v2/v3 fixture warehouses with a live replay tier; registry row
  ICE-NAN-PUSHDOWN-1 FIXED. `risk_tier: standard`. Branch
  `fix/ice-nan-pushdown-1`.
  pins: ice-nan-pushdown-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013
- [rp-32-rdf-cow-bytes-ledger.md](rp-32-rdf-cow-bytes-ledger.md) —
  **RP-32-RDF-COW-BYTES (2026-09-19):** the flips fork #301 (F-RDF-COW-BYTES-1) causes —
  the COW-BYTES and DANGLE-2 strict xfails run plain, the changed-meaning pins (NULL
  precedence, residue sequence, C-011, two Rust dangling tests) move to new Spark 4.1.2
  cells, five registry rows close or come true. Test-only, no product change. Round 1
  CONCLUDED with all nine clauses PROVEN and the gates green.
  `risk_tier: standard`. Branch `chore/rp-32-fork-pin`.
  pins: rp-32-rdf-cow-bytes/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [rp-38-fork-pin-ledger.md](rp-38-fork-pin-ledger.md) —
  **RP-38-FORK-PIN (2026-09-19):** the fork pin moves to `f3bdd598` (#316 shared Parquet footer
  cache, with ICE-FOOTER-CACHE-1 in the same PR; #313 metrics config and #315 transform types for
  run 24c). `risk_tier: standard`. Branch `chore/rp-38`.
- [rp-37-fork-pin-ledger.md](rp-37-fork-pin-ledger.md) —
  **RP-37-FORK-PIN (2026-09-19):** the fork pin moves to `27e0d5fa` (#309 metadata-only DELETE
  decision API, #311 catalog cache handles for Glue and S3 Tables, default off); no RePark answer
  changes. `risk_tier: standard`. Branch `chore/rp-37`.
- [rp-36-fork-pin-ledger.md](rp-36-fork-pin-ledger.md) —
  **RP-36-FORK-PIN (2026-09-19):** the fork pin moves to `fa77fb2b` (#312 timezone-bearing
  timestamp filters reach the scan, #310 page-index row selection on); the ICE-READ-PERF-0 Q3
  sentinel flips; the bench pair (Q3 200 → 27 footers, 65.5 MB → 0.99 MB of pages) and a Q6
  regression on the 200-file bed are recorded. `risk_tier: standard`. Branch `chore/rp-36`.
- [rp-35-fork-pin-ledger.md](rp-35-fork-pin-ledger.md) —
  **RP-35-FORK-PIN (2026-09-19):** the fork pin moves to `7bd2fea3` (#308 Avro name
  sanitising); 20 recorded Spark cells pin IPI-52 (rows, `partitions`, the manifest's Avro
  partition record). `risk_tier: standard`. Branch `chore/rp-35-fork-pin`.
- [rp-34-fork-pin-ledger.md](rp-34-fork-pin-ledger.md) —
  **RP-34-FORK-PIN (2026-09-19):** the fork pin moves to `43fcd243` (#306 parquet footer,
  #305 dangling DVs); three `clippy::large_futures` calls are boxed; the three
  `ICE-RDF-GRANULARITY-1` cells run plainly at Spark's 8→4, and the four `rpd_target_small`
  cells are strict xfails under the new fork-ask row `ICE-RDF-RPD-TARGET-SMALL-1`. Round 1
  CONCLUDED with three clauses PROVEN. `risk_tier: standard`. Branch `chore/rp-34-fork-pin`.
  pins: rp-34-fork-pin/C-003
- [ice-rdf-options-1-ledger.md](ice-rdf-options-1-ledger.md) —
  **ICE-RDF-OPTIONS-1 round 1 (2026-09-17), in flight:** the RePark side of the options-map
  remediation (rating V2-08/C-4) — `rewrite_data_files` (16 keys) and
  `rewrite_position_delete_files` (measured 8-key subset) parse `options => map(…)` with
  Spark's class and text (new `IllegalArgumentMarker` → `IllegalArgumentException` path in
  `repark-core/src/error_map.rs`); pin-supported knobs apply to the fork builder, fork-owned
  keys (`rewrite-all`, `partial-progress.*`, `output-spec-id`, `rewrite-job-order`,
  `max-concurrent-file-group-rewrites`) validate now and wire in round 2 against
  `F-RDF-OPTIONS-1`. Recorded 45-cell Spark 4.1.2 oracle with generator, 29 Rust pins, 42
  Python pins (fork cells `xfail(strict)`), registry rows `ICE-RDF-OPTIONS-1`
  (OPEN-IN-PROGRESS) and `RDF-DANGLING-1` (BACKLOG, residue #37). Both CALL arms dispatch
  behind `Box::pin` (16 KiB `large_futures`). `risk_tier: standard`. Branch
  `feat/ice-rdf-options-1`.
  pins: ice-rdf-options-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [ice-sorted-insert-1-ledger.md](ice-sorted-insert-1-ledger.md) —
  **ICE-SORTED-INSERT-1 (2026-09-17), in flight:** sort-on-INSERT end to end
  against Spark — the fork #287 per-writer-stream sort plus `sort_order_id`
  stamp at fork main `4151b488` (RP-22), proven by per-file sortedness and
  stamp pins on the SQL and DataFrame doors plus the RePark-owned paths
  (INSERT OVERWRITE, CTAS, MERGE), with the Spark oracle recorded as truth
  JSON plus a live replay tier; registry sort-on-INSERT row FIXED. Round 3
  (2026-09-17, logic-critic remediation): the v3 lineage fanout sorts before it
  stamps, the owned sort canonicalises NaN, every stamp site has a revert-red
  pin, and the binpack and fork-UPDATE rewrites are filed as fork asks
  (`F-RDF-SORT-STAMP-1`, `F-COW-UPDATE-STAMP-1`) with strict-xfail pins.
  `risk_tier: standard`. Branch `feat/ice-sorted-insert-1`.
  pins: ice-sorted-insert-1/C-001, C-002, C-003, C-004, C-005
  pins: ice-sorted-insert-1/C-006, C-007, C-008, C-009, C-010
- [ice-session-write-conf-1-ledger.md](ice-session-write-conf-1-ledger.md) —
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19), in flight:** the session confs
  `spark.sql.iceberg.snapshot-property.*` and
  `spark.sql.iceberg.compression-codec` reach every Iceberg write with
  writer-option over session-conf over table-property precedence (one Rust
  resolver, router fold, facade forwards keys only); the 63-cell Spark oracle
  (`ice_session_write_conf_1_spark_oracle.json`) and the red-first pins
  (`test_ice_session_write_conf_1.py`: 26 red, 3 green controls on the base;
  `test_ice_session_write_conf_1_paths.py`: the 38 run-25c path cells, 23 red).
  Rounds 1 and 2 (2026-09-19) close a verification critic's four P1s and two P2s:
  branch writes, the native `repark.sql` door, position-delete codecs, the
  one-collision-rule summary merge, the mutation-proof Rust pins, and the
  measured no-stamp on TRUNCATE / metadata-only delete. Three named residues —
  `F-RDF-SESSION-CONF-1` (fork `rewrite_data_files`) on strict xfails, `IPI-08`
  (whole-partition DELETE routing) on a dated xfail, and the
  `engine-name` / `engine-version` reading; registry row FIXED. Round 6 (2026-09-20)
  CLOSES the `IPI-08` residue: #739 landed the metadata-delete route, the dated xfail
  re-measured green on the merged build, and `QS-DELETE-PART-META` is a pin.
  Round 3 (2026-09-19) closes a SECOND verification critic's four P1s, six P2s and one
  P3: no session conf decides a statement's route any more (a codec-only conf was
  changing a plain UPDATE's committed file count), the native door answers a plain
  UPDATE and `INSERT OVERWRITE … PARTITION`, a dynamic `overwritePartitions` builds its
  collision oracle from the partitions it actually replaces, the property suffix is
  carried verbatim as Spark carries it, and the three claims that lived only in the
  Python pins have Rust pins. Two of the critic's premises were refuted by measurement
  (Spark folds no conf-key prefix; Spark stamps no `rewrite_manifests` replace snapshot)
  and a third measurement narrowed `F-RDF-SESSION-CONF-1` to its codec half. Fifteen new
  Spark cells (`QP-*`, `QK-*`, `QM-*`); the fixture gains a per-cell `stamped` observation.
  Round 4 (2026-09-20) closes a THIRD verification critic's two P1s and two P2s: the STATIC
  `INSERT OVERWRITE … PARTITION` resolves the files its row filter removes (eight `QO-*`
  cells), the owned INSERT plans the insert rather than a rebuilt `SELECT` so a session conf
  changes what a commit stamps and never how its source types, the collision lookup uses the
  verbatim suffix Spark stamps (three `QC-*` cells), and the layout battery asserts Spark's
  OWN file count (six `QU-*` cells). The thirty-two round-3 and round-4 cells entered the
  fixture and the live recorder, which re-derives all 95.
  `risk_tier: standard`. Branch `fix/ice-session-write-conf-1`.
  pins: ice-session-write-conf-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  pins: ice-session-write-conf-1/C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
  pins: ice-session-write-conf-1/C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-024
  pins: ice-session-write-conf-1/C-025, C-026, C-027, C-028, C-029, C-030, C-031, C-032
  pins: ice-session-write-conf-1/C-033, C-034, C-035, C-036, C-037, C-038, C-039, C-040
  pins: ice-session-write-conf-1/C-041, C-042, C-043, C-044, C-045, C-046, C-047, C-048
  pins: ice-session-write-conf-1/C-049, C-050, C-051, C-052, C-053, C-054, C-055, C-056
  pins: ice-session-write-conf-1/C-057, C-058

## Pointers
- Up: [../map.md](../map.md)
- [perf-scan-1-plan-once-ledger.md](../completed/perf-scan-1-plan-once-ledger.md) —
  **PERF-SCAN-1 (2026-09-03 / r2 2026-09-04), in flight:** `TargetScanStream` caches
  `FileScanTask`s across concurrent `StreamingTable` re-executes (hardening). Registry
  `PERF-SCAN-3PASS-1` stays BACKLOG: production identity DELETE is 1 + 0 + 1 opens, not
  3 × N at scan. `risk_tier: standard`. Branch `perf/scan-1-plan-once`.
  pins: perf-scan-1-plan-once/C-001, C-002, C-003, C-004
- [sql-harden-1-cutover-shapes-ledger.md](../completed/sql-harden-1-cutover-shapes-ledger.md) —
  **SQL-HARDEN-1 (2026-09-04), in flight:** the cutover pipeline cutover Iceberg SQL shapes S1–S7
  measured against live Spark on the memory catalog; Glue + S3 Tables legs. Four registry
  rows filed, `V3-COV-7` cited, 0 FIXED. `risk_tier: standard`. Branch
  `feat/sql-harden-1-cutover-shapes`. pins: sql-harden-1-cutover-shapes/C-001
- [sql-harden-2-cow-shapes-ledger.md](../completed/sql-harden-2-cow-shapes-ledger.md) —
  **SQL-HARDEN-2 (2026-09-04), in flight:** S1/S2/S4 at v2 and v3 copy-on-write (S8/S9).
  `delete_files` empty both engines; data-file count 1 after the second MERGE; remaining
  DIVERGES are `CUTOVER-CTAS-REQ-1` / `V3-COV-7`. No `CUTOVER-COW-*` row. Glue + S3 Tables
  PASS. `risk_tier: standard`. Branch `feat/sql-harden-2-cow-shapes`.
  pins: sql-harden-2-cow-shapes/C-001, C-002, C-003, C-004
- [rp-10-repin-f25-ledger.md](../completed/rp-10-repin-f25-ledger.md) — **RP-10 (2026-09-04), in flight:**
  the fork repin `594bdbe5` → `85a4aaf0` (F-25). `validate_fresh_dvs_only` stops once every
  `added_dvs` key is found; `PERF-DVCLOSE-STMT-1` closes. `risk_tier: standard`. Branch
  `feat/rp-10-repin-f25`.
- [rp-19-ledger.md](rp-19-ledger.md) — **RP-19 (2026-09-12), in flight:** consume fork pin
  `3ebf7d36` (F-S3ROOT-1 `#281`, the whole bump — a bare-bucket object-store
  location, every S3 Tables table's, resolves to the bucket root like Java's
  `S3URI` for S3, GCS and OSS). Consumer: the ORPHAN-S3TABLES-1 registry row opens
  with the parser half FIXED at this pin and the owner's dev-bucket reproduction
  verbatim (the parser error before, the 405 `ListObjectsV2` refusal after); the
  loud refusal itself is the ORPHAN-S3TABLES-1 card. No product code, no live test
  (D-2/D-3). `risk_tier: standard`. Branch `chore/repin-rp-19`.
  pins: rp-19/C-001, C-002, C-003
- [rp-16-ledger.md](rp-16-ledger.md) — **RP-16 (2026-09-11), in flight:** consume fork pin
  `090bc821` (F-WRITE-COMPRESS-1 `#276` plus riders `#273`–`#275`, `#277`). Consumer
  fix: `PERF-CATALOG-CACHE-WEIGHT-1` FIXED — charged object-graph weight, measured
  retain 1,071,000 / evict 1,070,000, retain pin 1,250,000, old 280000 now evicts. Do not re-measure
  AP-1. `risk_tier: standard`. Branch `chore/repin-rp-16`.
  pins: rp-16/C-001, C-002, C-003, C-004
- [rp-17-ledger.md](rp-17-ledger.md) — **RP-17 (2026-09-12), in flight:** consume fork pin
  `41e25ba2` (F-WRITE-COMPRESS-2 `#278`, the whole bump — the maintenance, COW/MoR
  rewrite and position-delete writers honour `write.parquet.compression-codec`,
  closing residue sites F-WRITE-COMPRESS-1-R-001…R-004). Consumer: the third AP-1
  re-measure under one codec; the 20 % check answers AP-1-R-001 as measured and
  both projections are recorded for the orchestrator's Q-1 ruling.
  `risk_tier: standard`. Branch `chore/repin-rp-17`.
  pins: rp-17/C-001, C-002, C-003, C-004
- [rp-18-ledger.md](rp-18-ledger.md) — **RP-18 (2026-09-12), in flight:** consume fork pin
  `9e3522e3` (F-REWRITE-SIZE-1 step 2 `#280`, the whole bump — the maintenance
  rewrite disables the dictionary per column from the input footers, removing the
  dead dictionary pages that caused the measured 1.47× S2-24 inflation, and an
  unset `write.parquet.compression-level` means zstd 3 like Java). Consumer: the
  fourth AP-1 re-measure, now with AP-3's uncompressed-basis projection; the 20 %
  check answers AP-1-R-001 as measured. `risk_tier: standard`. Branch
  `chore/repin-rp-18`.
  pins: rp-18/C-001, C-002, C-003, C-004
- [date-fn-1-spark-date-spelling-ledger.md](../completed/date-fn-1-spark-date-spelling-ledger.md) —
  **DATE-FN-1 (2026-09-04), in flight:** Spark SQL `date()` spelling and `unix_timestamp`;
  `CUTOVER-DATE-1` FIXED; S6 gold rows Spark-equal, program still DIVERGES on `V3-COV-7`.
  `risk_tier: standard`. Branch `fix/date-fn-1-spark-date-spelling`.
  pins: date-fn-1-spark-date-spelling/C-004
- [ex-15-dataframe-a-ledger.md](../completed/ex-15-dataframe-a-ledger.md) —
  **EX-15 (2026-09-04), in flight:** the v1.1 example backfill's first `DataFrame.*` batch —
  36 roster names at base `c70a306`; 28 covered by eight `docs/examples/dataframe/` files
  (backlog 578 → 550), 8 measured divergences stay with §7 rows `EX-DF-1`…`EX-DF-6` and pins in
  `python/repark/tests/test_examples_dataframe_a.py`. `risk_tier: standard`. Branch
  `docs/ex-15-dataframe-a`. pins: ex-15-dataframe-a/C-001
- [ex-16-dataframe-b-ledger.md](../completed/ex-16-dataframe-b-ledger.md) —
  **EX-16 (2026-09-04), in flight:** the v1.1 example backfill's second `DataFrame.*` batch —
  36 roster names at base `f3968aa`; 32 covered by eight `docs/examples/dataframe/` files
  (backlog 550 → 518); `intersectAll`/`intersect_all` and `groupingSets`/`grouping_sets` stay
  with §7 rows `EX-DF-7`/`EX-DF-8`, and the narrow `mergeInto`/`printSchema` arms are recorded as
  §7 rows `EX-DF-9`/`EX-DF-10`, pins in `python/repark/tests/test_examples_dataframe_b.py`.
  `risk_tier: standard`. Branch `docs/ex-16-dataframe-b`. pins: ex-16-dataframe-b/C-001
- [ex-18-dataframe-c-ledger.md](../completed/ex-18-dataframe-c-ledger.md) —
  **EX-18 (2026-09-04), in flight:** the v1.1 example backfill's third `DataFrame.*` batch —
  36 roster names at base `e3600a1`; 35 covered by eleven `docs/examples/dataframe/` files (backlog 484 →
  449 through the EX-16/EX-17 merges), `toJSON` stays (R-DF-BATCH2), §7 `EX-DF-11`…`EX-DF-17`, pins in `python/repark/tests/test_examples_dataframe_c.py`. `risk_tier: standard`. Branch `docs/ex-18-dataframe-c`. pins: ex-18-dataframe-c/C-001
- [ex-17-column-a-ledger.md](../completed/ex-17-column-a-ledger.md) —
  **EX-17 (2026-09-04, r2), in flight:** the v1.1 example backfill's `Column.*` (a) batch —
  40 roster names at base `e3600a1`; 34 covered by ten `docs/examples/column/` files
  (backlog 550 → 516 at base; 484 after the EX-16 merge), 6 engine-plumbing rows stay (no PySpark analog), the two
  measured divergent bare-name arms are §7 rows `EX-COL-1`/`EX-COL-2` with pins in
  `python/repark/tests/test_examples_column_a.py`. `risk_tier: standard`. Branch
  `docs/ex-17-column-a`. pins: ex-17-column-a/C-001
- [df-printschema-1-trailing-newline-ledger.md](../completed/df-printschema-1-trailing-newline-ledger.md) —
  **DF-PRINTSCHEMA-1 (2026-09-04), in flight:** `printSchema` stdout byte-identical to
  Spark's (flat, nested, array, `level=1` exact captures); `EX-DF-10` flipped to FIXED in
  the merge commit `68e408d`. `risk_tier: standard`. Branch
  `fix/df-printschema-1-trailing-newline`.
  pins: df-printschema-1-trailing-newline/C-004
- [ex-20-window-catalog-ledger.md](../completed/ex-20-window-catalog-ledger.md) —
  **EX-20 (2026-09-04), in flight:** the v1.1 example backfill's `Window`/`WindowSpec` +
  first `Catalog.*` batch — 40 roster names at base `3484f8d7`; 37 covered by eight files
  under `docs/examples/window/` and `docs/examples/catalog/` (backlog 411 → 374 shipped;
  449 → 412 at the dispatch base), 3 stay
  (`getDatabase`/`get_database`, `listDatabases`) with §7 rows `EX-CAT-1`/`EX-CAT-2`, the
  `functionExists` dbName arm is `EX-CAT-3`, and the DataFrame-door tied-key default frame
  is `EX-WIN-1`, pins in `python/repark/tests/test_examples_window_catalog.py`.
  `risk_tier: standard`. Branch `docs/ex-20-window-catalog`. pins: ex-20-window-catalog/C-001
- [ex-19-dataframe-d-window-ledger.md](../completed/ex-19-dataframe-d-window-ledger.md) —
  **EX-19 (2026-09-04, r3), in flight:** the v1.1 example backfill's fourth `DataFrame.*` batch —
  the 39-name DataFrame remainder plus GroupedData, Row, na, and stat surfaces at base `7496049`;
  38 covered by ten `docs/examples/dataframe/` files (backlog 449 → 411 shipped after the EX-18
  merge; 518 → 480 at the dispatch base), `stat.freqItems` stays
  with §7 `EX-DF-19`, the `withColumnsRenamed` duplicate-name arm is §7 `EX-DF-18`, the struct
  `Row` field arm is §7 `EX-ROW-1`, pins in
  `python/repark/tests/test_examples_dataframe_d.py`. `risk_tier: standard`. Branch
  `docs/ex-19-dataframe-d-window`. pins: ex-19-dataframe-d-window/C-001
- [perf-dynflatten-2-null-mask-ledger.md](../completed/perf-dynflatten-2-null-mask-ledger.md) —
  **PERF-DYNFLATTEN-2 (2026-09-04), in flight:** the one candidate PERF-DYNFLATTEN-1 queued,
  built. A scalar UDF unions the parent struct's validity into the child array instead of a
  per-leaf `CASE WHEN parent IS NULL`; `struct_d6`'s isolated null cost 64.83 ms → 0.01 ms
  (0.1x its run's floor), every bed row set, schema and ordered-row digest identical against a
  rebuilt pre-extractor module, `DYNFLATTEN-QUALNAME-1` FIXED as a side effect and re-pinned as
  an answer pin. `risk_tier: standard`. Branch `perf/dynflatten-2-null-mask`.
  pins: perf-dynflatten-2-null-mask/C-001, C-002, C-003, C-004, C-005
- [ex-22-types-writerv2-ledger.md](../completed/ex-22-types-writerv2-ledger.md) —
  **EX-22 (2026-09-04), in flight:** the v1.1 example backfill's `types` + `DataFrameWriterV2`
  batch — all 43 roster names at base `b5827be6`; 42 covered by eleven files under
  `docs/examples/types/` (new) and `docs/examples/io/` (backlog 340 → 298 shipped; 374 → 332
  at the dispatch base), the flagged
  `VariantType`/`TimeType`/`CharType`/`VarcharType` measured Spark-equal, the Arrow helpers and
  four snake_case spellings covered as repark extensions; `DataFrameWriterV2.overwrite` stays
  with §7 `EX-W2-1`, the empty-source `overwritePartitions` arm is §7 `EX-W2-2`, the
  `option`/`options` branch-tag arm is §7 `EX-W2-3`, and the round-2 unpartitioned-table
  `overwritePartitions` parser leak is §7 `EX-W2-4` (OPEN, fix unit
  `WRITERV2-OVERWRITE-UNPART-1`), pins in
  `python/repark/tests/test_examples_window_catalog.py`. `risk_tier: standard`. Branch
  `docs/ex-22-types-writerv2`. pins: ex-22-types-writerv2/C-001, C-002, C-003, C-004, C-005
- [perf-dynflatten-1-measure-ledger.md](../completed/perf-dynflatten-1-measure-ledger.md) —
  **PERF-DYNFLATTEN-1 (2026-09-04), in flight:** measure `dynamicFlatten` on the
  nested bed; rank the three H-3 intake candidates. `risk_tier: standard`.
  Branch `perf/dynflatten-1-measure`.
  pins: perf-dynflatten-1-measure/C-001, C-002, C-003, C-004
- [perf-cast-1-ledger.md](perf-cast-1-ledger.md) —
  **PERF-CAST-1 step 1 (2026-09-12), in flight:** where a CAST costs a
  millisecond — 200k-row MemTable, 50 / 250 / 2500 CASTs, three plan
  shapes; superlinear phase is CAST-over-aggregate logical planning
  (exponent 1.535); `max_passes` 0/1/3 do not move the wall; current cost
  pinned (1.5× standalone 2500 median) plus a strict-xfail linear flip pin.
  No product change. `risk_tier: standard`. Branch `perf/cast-1`.
  pins: perf-cast-1/C-001, C-002, C-003, C-004
- [platform-1-ledger.md](platform-1-ledger.md) —
  **PLATFORM-1 step 1 (2026-09-12), in flight:** the abi3 wheel matrix —
  `wheels.yml` `platform-matrix` runs the four legs PRs never see (manylinux
  aarch64, macOS arm64, macOS x86_64, Windows x86_64) on a nightly cron plus
  `workflow_dispatch`, never `pull_request`; `release.yml` `build-wheel` is the
  five-leg tag matrix publishing all five through one trusted-publishing call;
  per-PR CI untouched. Workflow-only unit — pins in
  `python/repark-parity/tests/test_platform_1_wheel_matrix.py`, leg proof is the
  orchestrator's post-merge dispatch. `risk_tier: standard`. Branch
  `ci/platform-1-wheel-matrix`.
  pins: platform-1/C-001, C-002, C-003, C-004, C-005
- [profiles-1-ledger.md](../completed/profiles-1-ledger.md) —
  **PROFILES-1 steps 1–3 (2026-09-10/12), in flight:** the measurement bed step 2
  sweeps: three D-2 datasets, five reads + three writes, knob × value CSV harness,
  one-JVM guard, `--smoke` proof mode. Step 2 ran the sweep on a release build —
  baseline plus 19 knobs × 3 reps in
  `docs/perf/config-profiles-2026-09-12.md` with its CSV evidence; the two
  `write.*` table-property knobs reached the bed via `ALTER TABLE … SET
  TBLPROPERTIES` (harness fix); 5 knobs land "no effect measured" under the
  affected-cells rule. Step 3 derives the two named profiles from those tables —
  `read` carries three knobs (`batch_size` 16384, `target_partitions` 32,
  `repartition_joins` false), `write` carries none — documented in
  `docs/guide/repark-toml.md`, committed as `docs/examples/config/*.toml`, pinned
  against the CSVs and the CFG-1 loader, with a three-knob re-measure under
  `docs/perf/config-profiles-2026-09-12/step3-remeasure/`.
  `risk_tier: standard`. Branch `feat/profiles-1-step-3`.
  pins: profiles-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- [perf-facade-1-ledger.md](../completed/perf-facade-1-ledger.md) —
  **PERF-FACADE-1 (2026-09-04), in flight:** slate items 1 and 2 of PERF-ANALYSIS-1, the two
  biggest measured user-visible walls. `collect()` row materialization moves into
  `repark-python` (`collect_rows.rs` emits value tuples; the facade builds every `Row` from one
  shared names tuple per batch with the collector suspended): 1e6 x 7 **4,908 -> 956 ms**
  (5.14x), from 1.37x slower than Spark to 3.79x faster. `DataFrame.columns` answers from the
  plan's logical schema (`logical_names.rs`) and `with_columns` reads it once per call instead
  of once per existing column: depth-100 chain build **2,385 -> 367 ms** (6.50x, 5,750 analyzer
  passes -> 0), from 3.19x slower than Spark to 2.04x faster. The 150 ms chain target is NOT
  met and is reported as missed: the residue is DataFusion's own per-expression projection
  validation, and the collapse that would close it measures **65.04 ms** — under the bar, so
  `PERF-FACADE-CHAIN-2` is deferred on correctness (plan lineage, `_origin_plan_id`,
  `MISSING_ATTRIBUTES`) and not because the prize is small. Mutation 8 of 8 red; an independent
  critic reproduced the unit on its own clone and reds 7 of its own. Round 2 replaced the
  baseline with a tracked runner (`python/repark-parity/bench/facade/`, `make facade-bench`) and
  re-measured every number with it, because the first baseline's probes were untracked.
  `risk_tier: standard`. Branch `perf/facade-1`.
  pins: perf-facade-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [cutover-schema-1-ledger.md](../completed/cutover-schema-1-ledger.md) —
  **CUTOVER-SCHEMA-1 (2026-09-04), in flight:** nullability derived the way Spark
  derives it — reader relax, CTAS all-optional on both doors, decimal-cast analyzer
  rule, export-boundary Utf8 coercion. Closes `CUTOVER-CTAS-REQ-1` and
  `CUTOVER-DEDUP-SCHEMA-1`, the nullability half of `V3-COV-8`, converges
  `DYNFLATTEN-READNULL-1`. `risk_tier: standard`. Branch
  `fix/cutover-schema-1`.
  pins: cutover-schema-1/C-001, C-002, C-003, C-004, C-005, C-006
- [nullability-2-ledger.md](../archive/2026-09/2026-09-06-nullability-2-ledger.md) —
  **NULLABILITY-2 (2026-09-05), complete:** the analyzer's remaining nullability
  and cast residues, Spark-equal — generalized cast nullability, boolean→decimal,
  null-safe equal non-null, reader relax at every depth, tz-naive dtype mapping.
  Closes or narrows `CAST-NULL-1`, `CAST-BOOL-DEC-1`, `DEC-9` (remainder),
  `G6-4`, `G12-1`, `G12-2`, `CUTOVER-NULLDEPTH-1`, `READ-TSNTZ-DTYPE-1`.
  `risk_tier: elevated`. Branch `fix/nullability-2`.
  pins: nullability-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [perf-facade-cdf-1-ledger.md](../completed/perf-facade-cdf-1-ledger.md) —
  **PERF-FACADE-CDF-1 (2026-09-05), in flight:** PERF-ANALYSIS-1 candidate 2 —
  `createDataFrame(list of tuples)` stops normalizing every cell in Python five times and
  infers + converts column-wise, with nested columns delegated to the unchanged per-cell
  path. Target `create/100000/tuples_count` ≤ 100 ms. `risk_tier: standard`. Branch
  `perf/facade-cdf-1`.
  pins: perf-facade-cdf-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [dfcore-2-ledger.md](../completed/dfcore-2-ledger.md) —
  **DFCORE-2 (2026-09-07), in flight:** the four UDF select rewrites out of `core.py` —
  scalar and classic to `udf_projection.py`, the window variants to
  `udf_window_projection.py`. Move-only: `core.py` 5954 → 5263, class loses exactly
  the four methods, package and core gain exactly the two modules, 6101 collected IDs
  preserved plus one pin test, four mutations red existing pins. `risk_tier: standard`.
  Branch `refactor/dfcore-2`.
  pins: dfcore-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-3-ledger.md](../completed/dfcore-3-ledger.md) —
  **DFCORE-3 (2026-09-07), in flight:** the statistics family out of `core.py` —
  six bodies plus the `freqItems` refusal to `statistics.py`. Move-only: `core.py`
  5263 → 5060, `writer_readwriter.py` 1113 → 1111, class dir frozen, package and
  core gain exactly `statistics`, 6102 collected IDs preserved plus one pin test,
  seven mutations red existing pins, collect count 6 == 6. `risk_tier: standard`.
  Branch `refactor/dfcore-3`.
  pins: dfcore-3/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-4a-ledger.md](../completed/dfcore-4a-ledger.md) —
  **DFCORE-4a (2026-09-07), in flight:** sampling out of `core.py` — the three
  sampling bodies plus argument normalization and seed coercion to `sampling.py`.
  Move-only: `core.py` 5060 → 4819, class loses exactly `_prepare_sample_args`,
  package and core gain exactly `sampling`, 6103 collected IDs preserved plus five
  pin tests, five mutations red existing pins, same-seed determinism pinned on a
  fixed thousand-row frame. `risk_tier: standard`.
  Branch `refactor/dfcore-4a`.
  pins: dfcore-4a/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-4b-ledger.md](../completed/dfcore-4b-ledger.md) —
  **DFCORE-4b (2026-09-07), in flight:** display out of `core.py` — the ten
  show/repr/HTML/eager bodies to `display.py`. Move-only: `core.py` 4819 →
  4539, class loses exactly the six display leavers, package and core gain
  exactly `display`, 6108 collected IDs preserved plus fourteen pin tests, ten
  mutations red existing pins, show/repr/HTML goldens byte-identical with
  `count()` tallies as the DFCORE-6 baseline. `risk_tier: standard`.
  Branch `refactor/dfcore-4b`.
  pins: dfcore-4b/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  Critic r1 FAIL on the moved warning's `stacklevel`; served (stacklevel 3, filename pin).
- [dfcore-5-ledger.md](../completed/dfcore-5-ledger.md) —
  **DFCORE-5 (2026-09-07), in flight:** `approxQuantile` in one collect per
  frame — the per-probability loop becomes one aggregation over the list form
  of `percentile_approx` (2x3 collects 6 → 1, 4x5 20 → 1, values identical),
  validation, shapes, NaN rules and the ignored `relativeError` preserved on
  both doors, audit values equal to live Spark, the all-NULL/empty divergence
  pinned on both sides, and the three DFCORE-3/DFCORE-4a gap pins closed.
  `risk_tier: standard`.
  Branch `perf/dfcore-5`.
  pins: dfcore-5/C-001, C-002, C-003, C-004, C-005
- [dfcore-6-ledger.md](../completed/dfcore-6-ledger.md) —
  **DFCORE-6 (2026-09-07), in flight:** eager previews fetch N+1 and never
  `count()` — repr, HTML, and vertical show read the footer from the extra
  row (1 → 0 counts per door), bridged eager doors peek at most
  `maxNumRows + 1` UDF rows (1e6-row preview 2,000,000 → 65,536 computed
  rows, 0.821/0.845 → 0.031/0.031 s), footers and cap-edge shapes preserved,
  every DFCORE-4b golden byte-identical, DFCORE-4b F2 closed by a non-golden
  vertical pin. `risk_tier: standard`.
  Branch `perf/dfcore-6`.
  pins: dfcore-6/C-001, C-002, C-003, C-004, C-005
  Critic r1 PASS (2026-09-07); the row-count assertion added to the repr footer pin.
- [sql-describe-1-ledger.md](../completed/sql-describe-1-ledger.md) —
  **SQL-DESCRIBE-1 (2026-09-09), in flight:** `DESCRIBE [TABLE] [EXTENDED|FORMATTED]`
  on Iceberg tables — step 1 measured the live PySpark 4.1.2 oracle on one session:
  six captures plus schemas in the ledger's Oracle capture section, FORMATTED
  byte-identical to EXTENDED, plain identical to `DESCRIBE TABLE`, missing table raises
  `AnalysisException` with `[TABLE_OR_VIEW_NOT_FOUND]`. Step 3 (2026-09-09) landed the
  facade pins, the live leg (19 of 22 rows byte-identical; `Name`, `Location`, `Table
  Properties` engine defaults differ by measurement), the DESC-1 registry row, and the
  DBT-DESC-1 retirement; all clauses PROVEN with four residue rows. `risk_tier: standard`.
  Branch `feat/sql-describe-1`.
  pins: sql-describe-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [maint-policy-1-ledger.md](../completed/maint-policy-1-ledger.md) —
  **MAINT-POLICY-1 (2026-09-10), complete:** the declarative maintenance policy — the typed
  `[<profile>.maintenance]` table with per-table overrides and the D-2 duration parser
  (`config_file/maintenance.rs`), `CALL <catalog>.system.run_maintenance(...)` with its
  dry-run plan, the D-4 step order behind the delete-ratio gate, the apply path
  (`ran` / `failed` / `skipped`, the chain stopping on failure), the session-build stamp
  that makes `repark.toml` reach the procedure, and the `session.run_maintenance` facade
  wrapper with its guide. `adaptive_partitioning` reserved for ADAPT-PART.
  `risk_tier: standard`. Branch `feat/maint-policy-1`.
  pins: maint-policy-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-024, C-025, C-026, C-027, C-028, C-029, C-030
- [facade-audit-0-ledger.md](../completed/facade-audit-0-ledger.md) —
  **FACADE-AUDIT-0 steps 1–2 (2026-09-10), in flight:** Half A of the Rust-backed facade
  audit — one measured row per module under `python/repark/src/repark/` (106 files,
  51,930 lines; binding sites, pyarrow references, delegate/logic/pyarrow class), the
  IPC crossing sites, and every place a `Column` renders SQL text — plus Half B
  (weighing, freeze constraints, confirmed sequence with pins, open questions), in
  [task/roadmap/epic-term/facade-audit-2026-09-10.md](../../roadmap/epic-term/facade-audit-2026-09-10.md).
  Base `2fad8135`; no source touched; READING path under R-10.
  `risk_tier: standard`. Branch `docs/facade-audit-0`.
  pins: facade-audit-0/C-001, C-002, C-003, C-004, C-006, C-007, C-008
- [ap-2-ledger.md](../completed/ap-2-ledger.md) —
  **AP-2 (2026-09-11), in flight:** `CALL
  <catalog>.system.apply_partitioning(table => …, plan_id => … [, dry_run => …]
  [, target_file_size_bytes => …])` — dry-run default, plan-id re-derived at the current
  snapshot, one commit per step, P-5 refusals, D-8 lookup target, guide section.
  `risk_tier: standard`. Branch `feat/ap-2-apply`.
  pins: ap-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [ap-1-ledger.md](../completed/ap-1-ledger.md) —
  **AP-1 step 1 (2026-09-10), in flight:** `CALL
  <catalog>.system.plan_partitioning(table => …, target_file_size_bytes => …)` — the P-2
  candidates scored with exactly P-3 over the `files` metadata table, the D-1 frame, the
  AP-0-R-001 caveat on every row, P-5 branch refusal plus the multi-spec note. Step 2 (GLM:
  release run over the AP-0 tables, perf document, guide section) is out of scope.
  `risk_tier: standard`. Branch `feat/ap-1`.
  pins: ap-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [review-fix-4-ledger.md](../completed/review-fix-4-ledger.md) —
  **REVIEW-FIX-4 (2026-09-10), in flight:** the eager frame's checkpoint paths —
  `lazy()` with no `_cache_view` interpolation, pending-checkpoint discharge in
  `count()` and the styled row count with no count query, and the `none`-view
  unreachable pin. Closes Q-12, Q-13, Q-50. `risk_tier: standard`. Branch
  `fix/review-fix-4`.
  pins: review-fix-4/C-001, C-002, C-003
- [review-fix-5-ledger.md](../completed/review-fix-5-ledger.md) —
  **REVIEW-FIX-5 step 1 (2026-09-10), in flight:** DESCRIBE takes one- to three-part names
  with session-default completion (D-3), never filters a three-part table named like a
  metadata table (D-1), resolves `Owner` from the session-built `DescribeOwnerConfig`
  extension (D-2), and redacts Table Properties through `prop_key_is_secret` with the Spark
  `s3.access-key-id` delta recorded as deliberate (D-4). `risk_tier: standard`.
  Branch `fix/review-fix-5`.
  pins: review-fix-5/C-001, C-002, C-003, C-004, C-005, C-006
- [review-fix-1-ledger.md](../completed/review-fix-1-ledger.md) —
  **REVIEW-FIX-1 (2026-09-10), in flight:** the CFG-1 mirror agrees with the loader —
  ASCII-only knob values refusing as `ValidationError`, duplicate names refused across
  every catalog and every database source naming both key paths, and an empty overlay
  profile surviving the `to_toml()` round trip. Closes Q-3, Q-4, Q-5, Q-6.
  `risk_tier: standard`. Branch `fix/review-fix-1-2`.
  pins: review-fix-1/C-001, C-002, C-003
- [review-fix-2-ledger.md](../completed/review-fix-2-ledger.md) —
  **REVIEW-FIX-2 (2026-09-10), in flight:** CFG-1's own pins stop reading the
  developer's `HOME` — the seed no-file pin drives stubbed discovery with `home: None`
  and the C-023 control session builds from a forced empty file. Closes Q-1.
  `risk_tier: standard`. Branch `fix/review-fix-1-2`.
  pins: review-fix-2/C-001, C-002
- [review-fix-6-ledger.md](../completed/review-fix-6-ledger.md) —
  **REVIEW-FIX-6 step 1 (2026-09-10), in flight:** `explain()` refuses the both-set
  shape — `extended` and `mode` together raise `PySparkValueError`
  `CANNOT_SET_TOGETHER` (the `Row(1, a=2)` shape), while a string `extended` with no
  `mode` keeps working. Closes Q-17. `risk_tier: standard`. Branch
  `fix/review-fix-6-11`. All clauses PROVEN (both-set pin red-first on base, green
  with the guard).
  pins: review-fix-6/C-001, C-002, C-003, C-004
- [review-fix-11-ledger.md](../completed/review-fix-11-ledger.md) —
  **REVIEW-FIX-11 step 1 (2026-09-10), in flight:** every DF-EXPLAIN-1 D-2 row gets a
  pin — cost, codegen, `ANALYZE`, simple-mode physical-only, and the blank-line
  separation. Landed with REVIEW-FIX-6. Closes Q-32 (with Q-17). `risk_tier: standard`.
  Branch `fix/review-fix-6-11`. All clauses PROVEN (mode pins green-before-green on
  base, confirming the D-2 rows; green on the landed tree).
  pins: review-fix-11/C-001, C-002, C-003, C-004, C-005, C-006
- [display-lazy-1-ledger.md](../completed/display-lazy-1-ledger.md) —
  **DISPLAY-LAZY-1 step 1 (2026-09-10), in flight:** a lazy frame's `repr` shows
  the schema, not the data (R-22 supersedes R-2) — the D-1 header box over the
  `lazy: N columns` first line, D-2 materialised classification, D-3 eagerEval
  rows with one plan run, D-4 unchanged doors plus the lazy bridge header, D-5
  transformation-laziness chains, and RF-5's duckdb probe with its re-pin. Step 2
  adds the plain-checkpoint marker (C-007) and the docs round. `risk_tier: standard`.
  Branch `feat/display-lazy-1`.
  pins: display-lazy-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [review-fix-8-ledger.md](../completed/review-fix-8-ledger.md) —
  **REVIEW-FIX-8 (2026-09-11), in flight:** the PROFILES-1 probe is re-runnable and its
  table is true — a unique temporary directory per run, `REPARK_CONFIG=""` in every
  session, the `VALIDATED` state for the three `repark.*` session keys, the two
  `write.*` rows re-described as accepted-at-session / applied-at-table with per-key
  table measurements, and re-derived counts (13 / 3 / 4 / 0 / 0). Closes Q-21, Q-22,
  Q-23, Q-40, Q-41, Q-55, Q-56. `risk_tier: standard`. Branch
  `fix/review-fix-8-conf-unread-1`.
  pins: review-fix-8/C-001, C-002, C-003, C-004, C-005
- [review-fix-3-ledger.md](../completed/review-fix-3-ledger.md) —
  **REVIEW-FIX-3 step 1 (2026-09-10), in flight:** the polars keep-set honours every
  legal `max_rows` — `min(n, max_rows)` split `(keep + 1) // 2` head /
  `keep - head` tail (Q-9), `max_rows = 1` earns its `1, …` without reviving the
  C8-Q-001 bare-ellipsis keep-set, the `truncate=True` → `str_len` remap gains a
  mutation pin (Q-10), and `show`'s docstring states the probe-first count (Q-11).
  `risk_tier: standard`. Branch `fix/review-fix-3-9-14`.
  pins: review-fix-3/C-001, C-002, C-003, C-004
- [review-fix-9-ledger.md](../completed/review-fix-9-ledger.md) —
  **REVIEW-FIX-9 step 1 (2026-09-10), in flight:** the polars renderer matches live
  polars 1.43.2 at its boundaries — list cells elide at four items
  (`[0, 1, … 3]`, Q-27) and `_polars_float_text` is re-derived from `fmt_float`
  so `±999999.0` spells `±9.99999e5` (Q-28), with live-polars oracle pins on
  list lengths 3/4/5 and the float switch either side (D-3); the shared
  `max_rows` pin is worked once under REVIEW-FIX-3 (Q-26).
  `risk_tier: standard`. Branch `fix/review-fix-3-9-14`.
  pins: review-fix-9/C-001, C-002, C-003
- [review-fix-14-ledger.md](../completed/review-fix-14-ledger.md) —
  **REVIEW-FIX-14 step 1 (2026-09-10), in flight:** the display configuration keeps
  its promises — `conf.get` on an unset `repark.display.style` serves the
  alive-token snapshot instead of re-reading `REPARK_DISPLAY_STYLE` (Q-52,
  ADR-0004), and `repark.display.max_rows` refuses above the delegated ceiling
  `_DISPLAY_MAX_ROWS_CEILING = 10_000` so the styled probe cannot fetch unbounded
  (Q-51). `risk_tier: standard`. Branch `fix/review-fix-3-9-14`.
  pins: review-fix-14/C-001, C-002
- [sql-set-door-1-ledger.md](../completed/sql-set-door-1-ledger.md) —
  **SQL-SET-DOOR-1 (2026-09-14), in flight:** the `SET`/`RESET`/`SET TIME ZONE` SQL door for
  registry `B-TZ-5` — D-1 shapes through `RuntimeConfig` (the `datafusion.*` exclusion keeps the
  conf forwarder from looping), D-2 frames on `to_arrow` (non-null `key`/`value`, zero-column
  `RESET`, four-column `SET -v`), Spark-class errors produced in the new module
  (`CANNOT_MODIFY_STATIC_CONFIG`, `INVALID_CONF_VALUE.TIME_ZONE`, `INVALID_CONF_VALUE.TYPE_MISMATCH`),
  `SET TIME ZONE LOCAL` a dated refusal, and the D-4 measurement recorded — timezone and ANSI are
  accepted-but-not-applied residues (TZ-3, SET-ANSI-RUNTIME-1). `risk_tier: standard`.
  Branch `feat/sql-set-door-1`. pins: sql-set-door-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014
- [java-double-str-1-ledger.md](../completed/java-double-str-1-ledger.md) —
  **JAVA-DOUBLE-STR-1 step 1 (2026-09-15), in flight:** DOUBLE/FLOAT stringify as
  Java does (registry BL-7) — the base-tree census of every Spark-door and facade
  path reaching Arrow float→utf8 text plus red pins on both doors
  (`test_java_double_str_1.py`, gt1 equality flip). Shared formatter (D-1), the
  Spark CAST→UDF rewrite with native Arrow kept (D-2), length-kernel formatting
  (D-3), and the BL-7 FIXED registry row (D-4) land in steps 2–3.
  `risk_tier: standard`. Branch `feat/java-double-str-1`.
  pins: java-double-str-1/C-001, C-002
- [fnp-6d-followup-1-ledger.md](../completed/fnp-6d-followup-1-ledger.md) —
  **FNP-6D-FOLLOWUP-1 (2026-09-15), in flight:** the bitmap aggregate signature
  followup to merged #609 (run 15a Grok critic L-001/L-003) — OR/AND refuse every
  non-BINARY payload, construct refuses non-BIGINT payloads and raises
  `CAST_INVALID_INPUT` on malformed STRING under ANSI-on, against the copied
  live-PySpark-4.1.2 fixture (`FU-*` cells). Step 1 is red-first; step 2 is
  blocked on the concat-Utf8 ruling recorded in the ledger.
  `risk_tier: standard`. Branch `feat/fnp-6d-followup-1`.
  pins: fnp-6d-followup-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [fnp-win-1-ledger.md](../completed/fnp-win-1-ledger.md) —
  **FNP-WIN-1 (2026-09-15), in flight:** `window`, `window_time`,
  `session_window` answer PySpark 4.1.2 run-15a oracle cells on both doors —
  tumbling/sliding/`startTime` time windows, end-minus-one-microsecond
  `window_time`, static/dynamic-gap sessions, `CANNOT_PARSE_INTERVAL` and
  `MISSING_AGGREGATION` errors, plus step-5 residual pins (zone rules, DST
  grid, month-gap refusal, NTZ sessions). Steps 1–4 done; step 5 closes the
  verdicts. `risk_tier: standard`. Branch
  `feat/fnp-win-1`.
  pins: fnp-win-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012
- [spark-sql-grammar-1-ledger.md](spark-sql-grammar-1-ledger.md) —
  **SPARK-SQL-GRAMMAR-1 step 1 (2026-09-16), in flight:** Spark operators,
  keywords and type names on the SQL door — ledger with the card rulings plus
  R-17c-3/R-17c-6/R-17c-7 and the re-measure table (1 ALREADY-GREEN, the rest
  RED, findings F-001/F-002). C-009 OPEN per R-17c-7. Branch
  `feat/spark-sql-grammar-1`.
  pins: spark-sql-grammar-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [unresolved-routine-1-ledger.md](unresolved-routine-1-ledger.md) —
  **UNRESOLVED-ROUTINE-1 (2026-09-16), in flight:** every unknown routine
  refuses with Spark's `UNRESOLVED_ROUTINE` on both doors (Q-17c-3) — ledger with
  the card rulings plus R-18c-1/R-18c-2/R-18c-3, the red-first table (41
  failed, 4 passed on the base) and the C-006/C-007 close. All clauses PROVEN;
  residue rows BL-19-POS-SELX and BL-19-LATERAL-1 filed. Branch
  `feat/unresolved-routine-1`.
  pins: unresolved-routine-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
  **Remediation round 1 (2026-09-16):** token-based call-site matching (L-001 /
  L-002 / L-003), the `schema_of_csv` divergence tripwire (L-004), UR3 pins,
  residue row BL-19-POS-FILTER, hand-offs H-004/H-005. All clauses stay PROVEN.
  pins: unresolved-routine-1/C-001, C-002, C-003, C-006, C-007
- [fnp-11b-ledger.md](fnp-11b-ledger.md) —
  **FNP-11B step 1 (2026-09-15), in flight:** datetime format parsing, the TIME
  family, BL-13 and BL-14 — ledger with D-1…D-10 (card D-1…D-6, run-16a D-7…D-10
  with owner ruling Q-15a-3 under D-8), the filtered oracle
  (`python/repark/tests/fnp11b_spark_oracle.json`: 315 pa-11b cells plus 22
  pa-math `to_char`-family cells) and the red-first two-door pins
  (`test_fnp11b_temporal_formats.py`: 310 failed, 28 passed on the base, all
  clauses OPEN). No product code in step 1.
  `risk_tier: standard`. Branch `feat/fnp-11b-temporal-formats`.
  pins: fnp-11b/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [logical-width-1-ledger.md](logical-width-1-ledger.md) —
  **LOGICAL-WIDTH-1 step 1 (2026-09-16), in flight:** Spark's logical widths on the facade —
  ledger with the card rulings W-1..W-7 plus owner rulings Q-16b-3/Q-17a-2/Q-17a-4, the
  Spark-cell fixture (`python/repark/tests/facade_logical_width_oracle.json`: 22 width cells
  plus 13 round-3 cells) and the red-first pins (`test_logical_width_1.py`, red on the
  base); round 2 moved the fillna width decision into `repark_core::na_fill_expr`
  (R-18b-4); round 3 fixed the zero-arg grouped keep-set (R-18b-5), made
  `ARITH-FLOAT-INT-1` exact (R-18b-6) and pinned `to(string)`-over-binary (R-18b-8). Branch
  `feat/logical-width-1`.
  pins: logical-width-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-013, C-014, C-015, C-016
- [sql-literal-typing-1-ledger.md](sql-literal-typing-1-ledger.md) —
  **SQL-LITERAL-TYPING-1 (2026-09-16), in flight:** Spark integral literal
  typing and binary-arithmetic promotion on the SQL door (BL-20) — ledger with
  Q-17c-2/Q-17a-2/Q-15c-6 and R-18c-1…R-18c-3, the verbatim 64-cell oracle
  (`python/repark/tests/sql_literal_typing_1_spark_oracle.json`) and the
  red-first pins (`test_sql_literal_typing_1.py`: 19 failed, 44 passed on the
  base, all clauses OPEN). No product code in step 1.
  `risk_tier: standard`. Branch `feat/sql-literal-typing-1`.
  State 2026-09-16: C-001, C-002, C-003, C-004, C-006, C-007 PROVEN; C-005 OPEN
  with BACKLOG row BL-20-OVF.
  pins: sql-literal-typing-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [io-orc-1-ledger.md](io-orc-1-ledger.md) —
  **IO-ORC-1 (2026-09-16), in flight:** the read-only ORC scan — `DataFrameReader.orc`,
  `format("orc").load` over orc-rust 0.8.0 (owner ruling Q-15B-1; write stays declared),
  the oracle cells (`python/repark/tests/facade_orc_oracle.json`) and the Spark-written
  fixtures (`python/repark/tests/fixtures/orc/`).
  `risk_tier: standard`. Branch `feat/io-orc-1`.
  pins: io-orc-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- [ice-dyn-overwrite-1-ledger.md](ice-dyn-overwrite-1-ledger.md) —
  **ICE-DYN-OVERWRITE-1 (2026-09-17), in flight:** dynamic `partitionOverwriteMode`
  routing for PARTITION-less overwrites (V2-24b) plus the INSERT OVERWRITE half of the
  V2-20a K4 race — Rust-first conf carrier, dynamic path through `ReplacePartitions`,
  empty-dynamic no-op, `saveAsTable` static overwrite through a typed session flag, Spark
  race matrix with disk-verified interleaves (default/snapshot silently replace the
  same-partition append, serializable refuses loud). Fixture
  `python/repark-parity/fixtures/torture/data/ice_dyn_overwrite_1/spark_oracle.json`;
  pins `test_ice_dyn_overwrite_1.py` (C-001…C-018) + `tests::dyn_partition_overwrite`.
  `risk_tier: standard`. Branch `fix/ice-dyn-overwrite-1`.
  pins: ice-dyn-overwrite-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018
- [ice-rtas-byname-1-ledger.md](ice-rtas-byname-1-ledger.md) —
  **ICE-RTAS-BYNAME-1 (2026-09-17), in flight:** `INSERT … BY NAME` column
  resolution on the Spark door plus the RTAS snapshot-operation divergence
  (V2-24). The oracle cells
  (`python/repark/tests/ice_rtas_byname_1_spark_oracle.json`, generator
  `_record_ice_rtas_byname_1_oracle.py`) and the red-first pins
  (`test_ice_rtas_byname_1.py`); the RTAS operation fix is fork-owned
  (F-RTAS-OPS-1, xfail pins).
  `risk_tier: standard`. Branch `feat/ice-rtas-byname-1`.
  pins: ice-rtas-byname-1/C-001, C-002, C-003, C-004, C-005, C-006
- [rp-23-pin-bump-ledger.md](rp-23-pin-bump-ledger.md) —
  **RP-23-PIN-BUMP (2026-09-17), in flight:** the fork-pin `4151b488`
  consequences round — F-TARGET-FILE-SIZE-1 fallout over the 26 facade
  failures: the codec-stamp expectation updates (family A), the re-derived
  file-layout fixtures (family B), and the MERGE delete-file diagnosis
  (family C, counts healthy, no regression). No product code.
  `risk_tier: standard`. Branch `chore/fork-pin-ice-20c-2`.
  pins: rp-23-pin-bump/C-001, C-002, C-003, C-004
- [ice-rdf-fork-asks-1-ledger.md](ice-rdf-fork-asks-1-ledger.md) —
  **ICE-RDF-FORK-ASKS-1 (2026-09-17), in flight:** the 13 RDF strict xfails re-measured
  on RP-23 (`4151b488`, zero XPASS) and the three unnamed reasons named as registry
  fork asks (`ICE-RDF-GRANULARITY-1`, `ICE-RDF-COW-BYTES-1`, `ICE-RDF-RPD-COMMITS-1`)
  beside `ICE-RDF-DANGLE-2`. Docs-only unit, no product change.
  `risk_tier: standard`. Branch `docs/ice-rdf-fork-asks-1`.
  pins: ice-rdf-fork-asks-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [listing-cost-flake-1-ledger.md](listing-cost-flake-1-ledger.md) —
  **LISTING-COST-FLAKE-1 (2026-09-18), in flight:** the catalog
  listing-cost pin counts calls, not wall-clock — the `Instant` ratio
  assertion over 20 iterations becomes exact `list_tables` / `load_table`
  counts on a delegating counting catalog (test code only).
  `risk_tier: standard`. Branch `fix/listing-cost-flake-1`.
  pins: listing-cost-flake-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [ice-write-options-1-ledger.md](ice-write-options-1-ledger.md) —
  **ICE-WRITE-OPTIONS-1 (2026-09-17), in flight:** DataFrame write options on Iceberg
  writes (V2-29) — the facade renders stored options as an internal `OPTIONS(...)`
  clause, Rust validates and honours (`snapshot-property.*`, parquet `write-format`,
  `target-file-size-bytes`, codec/level, isolation) or refuses loud (orc/avro, bad
  values); the 43-cell Spark oracle (`ice_write_options_1_spark_oracle.json`) and the
  red-first pins (`test_ice_write_options_1.py`: 23 failed, 11 passed on the base).
  `risk_tier: standard`. Branch `feat/ice-write-options-1`.
  pins: ice-write-options-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
  Run 22b (2026-09-18): §19 records the rebase over #682 / #687 / #678 (Q-22b-WO-1..5,
  the constraint-2 table, red-first and gates).
  pins: ice-write-options-1/C-014, C-015, C-016, C-017, C-018
- [ice-orc-avro-1-ledger.md](ice-orc-avro-1-ledger.md) —
  **ICE-ORC-AVRO-1 (2026-09-22), in flight:** ORC and Avro Iceberg data files
  (IPI-41 RePark half) — the `write-format` / `delete-format` options and the
  `write.format.default` / `write.delete.format.default` properties resolve to a
  real format on every owned builder site (RP-47 bump `604edca0`); the 13
  inventory cells plus the delete-format, metrics, wide-type, refusal and
  compaction cells, red-first in `test_ice_orc_avro_1.py` (24P offline; 22P live
  at WO3a). Registry `ICE-WRITE-OPTIONS-ORC-AVRO` retires to SERVED; `IO-ORC-1`
  keeps its plain-file writer refusal. `risk_tier: standard`. Branch
  `fix/ice-orc-avro-1`.
  pins: ice-orc-avro-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  pins: ice-orc-avro-1/C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
  pins: ice-orc-avro-1/C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-025
- [ice-v3-write-default-1-ledger.md](ice-v3-write-default-1-ledger.md) —
  **ICE-V3-WRITE-DEFAULT-1 (2026-09-17), in flight:** omitted columns on every
  Iceberg write path fill from the schema field's `write_default` in Rust, in
  the shared write-projection step — INSERT / MERGE column lists and the
  DataFrame writers on both SQL doors, with type fidelity and the measured
  Spark oracle as a checked-in fixture.
  `risk_tier: standard`. Branch `fix/ice-v3-write-default-1`.
  pins: ice-v3-write-default-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014
- [test-hygiene-1-ledger.md](test-hygiene-1-ledger.md) —
  **TEST-HYGIENE-1 (2026-09-18), in flight:** four test-hygiene fixes, test code
  only — the Spark v3 fixture keeps its baked-in `/tmp` path behind a
  cross-process kernel file lock (Avro carries absolute paths in deflate blocks),
  the v3_dv torture fixture holds an `fcntl.flock` while materialized AND read,
  the `current_date` pin reads the session zone with a strict-xfail Kiritimati /
  `Etc/GMT+12` pin under new registry row TZ-9 (OPEN, product defect), the
  500-table RSS leg carries the registered `perf` marker with CI still running
  it, and the staging map loses its duplicated `array-null-1` / `fnp-11b` entries.
  `risk_tier: standard`. Branch `test/test-hygiene-1`.
  pins: test-hygiene-1/C-001, C-002, C-003, C-004, C-005
- [ice-rtas-ops-2-ledger.md](ice-rtas-ops-2-ledger.md) —
  **ICE-RTAS-OPS-2 (2026-09-18), in flight:** the RePark opt-in for the fork's
  RTAS replace commit (rating row V2-24) — `with_replace_write(ctas.or_replace)`
  on both staged CTAS branches, the four RTAS pins un-xfailed, plain-CTAS and
  column-def no-snapshot controls, the overwrite summary-key pin, the recorder
  Ivy-cache env fix, and the RTAS-OPS-1 registry row to FIXED. Round 2 (claude-opus-5)
  closes Critic-3 L-01 (the native ANSI door takes the same opt-in) and L-02 (service-managed
  new-table RTAS commits through the fork's public overwrite path, `commit_replace_write`).
  `risk_tier: standard`. Branch `ice-rtas-ops-2`.
  pins: ice-rtas-ops-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022
- [ice-tt-resolve-1-ledger.md](ice-tt-resolve-1-ledger.md) —
  **ICE-TT-RESOLVE-1 (2026-09-19), in flight:** 1:1 Spark Iceberg time-travel resolution on
  every door — one shared `repark-core` resolver for the reader built-ins and both SQL doors,
  Spark refusal texts, the 94-cell oracle green offline plus live. Round 1 steps 1–2 committed
  (`3413c537`, `4e19fee2`); this ledger carries C-001…C-012 PROVEN.
  `risk_tier: standard`. Branch `fix/ice-tt-resolve-1`.
  pins: ice-tt-resolve-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012
- [ice-registry-sweep-1b-ledger.md](ice-registry-sweep-1b-ledger.md) —
  **ICE-REGISTRY-SWEEP-1B (2026-09-18), in flight:** the registry agrees with
  merged main for the twelve remaining rating rows and claims — every row state
  measured on merged main (`6cd9ee06`) the hour it is written. Nine rows verify
  unchanged (nested-evo/DDL, dynamic overwrite, write options, variant/geo,
  stats carry-forward, write-default fill machinery, sorted INSERT INTO,
  promote-read ranges, mixed-case ID-1); three tighten with a measured clause
  (ORC/Avro table-property refusal, ENC-1 cleartext bytes, north-star C-2 FIXED
  citation); V3-05 recorded in the ledger only (PR #700 owns the row).
  `risk_tier: standard`. Branch `docs/ice-registry-sweep-1b`.
- [ice-drop-ns-1-ledger.md](ice-drop-ns-1-ledger.md) —
  **ICE-DROP-NS-1 (2026-09-19), in flight:** `DROP NAMESPACE` on a non-empty
  namespace refuses like Spark 4.1.2 — the 26-cell oracle plus red-first pins
  on the facade door, one shared pre-drop emptiness helper in `repark-iceberg`
  called by both doors, Rust pins at the helper and on both doors, registry
  row FIXED with the nested-namespace boundary and the exception-class
  residual. `risk_tier: standard`. Branch `fix/ice-drop-ns-1`.
  pins: ice-drop-ns-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [ice-replace-columns-1-ledger.md](ice-replace-columns-1-ledger.md) —
  **ICE-REPLACE-COLUMNS-1 (2026-09-19), in flight:** `ALTER TABLE … REPLACE
  COLUMNS` drops every current top-level column and adds the listed ones with
  fresh field ids, as Spark's Hive-style form does — the 34-cell oracle
  (`RC-*`, v2 and v3) plus red-first pins, the parser and planner moved to
  `crates/repark-spark/src/replace_columns.rs` on the shared column-type path
  (STRUCT / ARRAY / MAP), Spark's texts on the `NOT NULL`, duplicate-name and
  partition/sort source-loss refusals, the identity-trap gate and its five
  pins deleted, registry row FIXED with four declared residues, and a harness
  replay of 34 / 34 equal (was 12 / 34). `risk_tier: standard`. Branch
  `fix/ice-replace-columns-1`.
  pins: ice-replace-columns-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- [ice-writer-metrics-1-ledger.md](ice-writer-metrics-1-ledger.md) —
  **ICE-WRITER-METRICS-1 (2026-09-20), in flight:** RePark's writers honour
  `write.metadata.metrics.*` and write Java's position-delete properties (IPI-09,
  RePark half) — `MetricsConfig::for_table` on every Parquet data-file writer,
  `for_position_delete_table` on the delete writer, the fork's delete-properties
  helper behind RePark's codec validation, twelve recorded Spark 4.1.2 metrics
  cells replayed as Rust pins. `risk_tier: standard`. Branch
  `fix/ice-writer-metrics-1`.
  pins: ice-writer-metrics-1/C-001, C-002, C-003, C-004, C-005
- [ice-system-functions-1-ledger.md](ice-system-functions-1-ledger.md) —
  **ICE-SYSTEM-FUNCTIONS-1 (2026-09-20), rounds 1-3 of 3, done:** the Iceberg
  `bucket(n, col)` / `truncate(w, col)` system functions as DataFusion scalar
  UDFs under reserved internal names, thin wrappers over the fork's
  `create_transform_function` — eight recorded-value pins on the Arrow path,
  the NULL row, the fork-text width refusals and the old-order refusal, lane
  gate `CB=0 R=0 T=0 U=0 L=0`. Round 2 adds `years` / `months` / `days`
  (DATE, never int) / `hours` / `iceberg_version` with six more clauses
  (C-012..C-017), the Day-recast mutation self-check, and a second green
  gate. Round 3 resolves `<cat>.system.<fn>` on every live catalog through a
  pre-parse rewrite, intercepts `SHOW [USER] FUNCTIONS IN <cat>.system`,
  files and retires registry row `FN-SYSFN-1`, and replays 15/15 EQUAL
  (C-018..C-025); ready for the departure move to `completed/`.
  `risk_tier: standard`. Branch `fix/ipi-29-system-functions`.
  pins: ice-system-functions-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-024, C-025
- [ipi-19-56-37-schema-evolution-write-ledger.md](ipi-19-56-37-schema-evolution-write-ledger.md) —
  **IPI-19 + IPI-56 + IPI-37 (2026-09-20), in flight:** schema evolution on write —
  the `mergeSchema` / `merge-schema` write option and the `spark.sql.iceberg.merge-schema`
  session conf over the `write.spark.accept-any-schema` gate, `MERGE WITH SCHEMA EVOLUTION`
  through a pre-parse token strip, the short-name `mergeInto` qualifier, and the
  four-combination `INSERT … BY NAME` matrix; the schema evolves first through the fork's
  `union_by_name_with` and the data commit is the only new snapshot. Registry `EX-DF-9`
  narrowed to the remaining acceptance gap. `risk_tier: standard`. Branch
  `fix/ipi-19-56-37-schema-evo-write`.
  pins: ipi-19-56-37-schema-evolution-write/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
  C-008, C-009, C-010, C-011, C-012, C-013
- [ice-changelog-1-ledger.md](ice-changelog-1-ledger.md) —
  **ICE-CHANGELOG-1 (2026-09-20), in flight:** incremental append reads, the `t.changes`
  relation and `create_changelog_view` (IPI-22) — the four window reader options reach the
  fork's `IncrementalAppendScan`, `t.changes` resolves as its own relation kind onto a
  `ChangelogTableProvider` (never a metadata table), and the procedure registers a lazy view
  carrying Java's `ChangelogIterator` transforms read off the 1.11.0 bytecode. Needs the fork
  branch `fix/f-changelog-reader-1` pinned before CI can be green. `risk_tier: standard`.
  Branch `fix/ice-changelog-1`.
  pins: ice-changelog-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-013, C-014, C-015
- [ice-metadata-cols-1-ledger.md](ice-metadata-cols-1-ledger.md) —
  **ICE-METADATA-COLS-1 (2026-09-20), done:** IPI-20 PR-1 — the Spark door
  serves `_file` / `_pos` / `_spec_id` / `_partition` (nine recorded cells verbatim on two harnesses,
  star stays user columns) while `_deleted` refuses typed
  `[ICE-MC-1]`; registry row `ICE-MC-FILEPOS-1` filed BACKLOG; the A-6
  `pos`/`file_path` test defers on a pin-measured fork name hijack; lane gate
  `CB=0 R=0 T=0 U=0 L=0`. WO-R1 (2026-09-21) merged in as C-011..C-014: the
  Spark-door `snapshot_id_` / `at_timestamp_` ref selectors. WO-R2 (2026-09-22)
  merged in as C-015..C-018: `_spec_id` values, evolution, star, refusal strings.
  **WO-R3 (2026-09-22):** merged in as C-019..C-023 — `_partition` joins the
  served set as a NULLABLE union struct; only `_deleted` still refuses.
  **U10-MC-DELETED-1 (2026-09-23):** `_deleted` is served; C-023 is REJECTED
  (SUPERSEDED by u10-mc-deleted-1/C-001), with supersession notes on C-007, C-008 and C-018.
  Ready for the departure move to `completed/`.
  `risk_tier: standard`. Branch `fix/ice-metadata-cols-1`.
  pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018,
  C-019, C-020, C-021, C-022, C-023
  C-009, C-010, C-011, C-012, C-013, C-014
  pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [fnp-math-1-ledger.md](../completed/fnp-math-1-ledger.md) —
  **FNP-MATH-1 step 1 (2026-09-15, run 16a), in flight:** ledger, fixture and
  red-first pins for the math/formatting/masking/crypto card — `bround`, `conv`,
  `mask`, `collate`, `collation`, `sentences`, `hash`, `format_number`,
  `aes_encrypt` / `aes_decrypt` / `try_aes_decrypt`, the facade half of `split`
  (D-8, Q12-41…Q12-55), the BL-6 facade half (`bin` / `rint` BOOLEAN refusal,
  3-argument `like` escape, D-9), plus `locate` / `array_join` cells held
  conditionally (D-6). No product code, no cargo, no dependency edits; AES crates
  approved under D-7 land in a later build step. 227 failed / 16 passed on base.
  `risk_tier: standard`. Branch `feat/fnp-math-1`.
  pins: fnp-math-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [ipi-23-mt-as-of-1-ledger.md](ipi-23-mt-as-of-1-ledger.md) —
  **IPI-23-MT-AS-OF-1 (2026-09-22), in flight:** `VERSION`/`TIMESTAMP AS OF` on a
  dotted Iceberg metadata table served on the Spark door — log-like tables answer
  the current table, file-like tables answer the resolved snapshot (unknown
  numeric ids answer empty), the five `all_*` tables refuse with Spark's
  `Cannot select snapshot in table: <TYPE>` text. Five facade pins replay the
  recorded run-25/26 inventory cells (`R-MT-SNAPSHOTS-TT`, `R-MT-FILES-TT`,
  `R-MT-ENTRIES-TT`, `R-MT-PARTITIONS-TT`, `R-REF-BRANCH-FILES`); registry row
  `MT-1` re-ruled DECLARED → FIXED. **R4 (2026-09-22, WO mt-r4-pins):** the
  refusal pins assert the mapped `engine_err` class plus the full message
  (unknown-ref / too-old-timestamp → `IllegalArgument` verbatim; the rule-1
  unknown id → `IllegalArgument("Cannot find snapshot with ID 999")`; `all_*`
  → mapped `Analysis`, the UnsupportedOperationException gap declared; the
  paren form → mapped `Parse`), and the empty-scan schema pin compares Arrow
  fields. Facade pin C-006 asserts `IllegalArgumentException` and the full
  message on the three probe-55b refusals. `risk_tier: standard`. Branch
  `fix/ipi-23-mt-as-of`.
  pins: ipi-23-mt-as-of-1/C-001, C-002, C-003, C-004, C-005, C-006
- [ipi-30-orphan-1-ledger.md](ipi-30-orphan-1-ledger.md) —
  **IPI-30-ORPHAN-1 (2026-09-22), in flight:** the orphan-parity R2 — the Python pins, the
  MW-7 runbook step, the guide and the registry follow R1's engine onto Spark's answers (bare
  `older_than` is now minus 3 days, a bare call deletes); the runbook step passes
  `dry_run => true` explicitly and ORPHAN-1/ORPHAN-2 retire per owner ruling Q-55-2.
  `risk_tier: standard`. Branch `fix/proc-orphan-spark-parity`.
  pins: ipi-30-orphan-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-011, C-012
- [ipi-07-branch-read-schema-1-ledger.md](ipi-07-branch-read-schema-1-ledger.md) —
  **IPI-07-BRANCH-READ-SCHEMA-1 (2026-09-22), in flight:** a branch read projects the
  table's current schema — the two `VersionRef` call sites build through the fork's
  `try_new_from_table_ref` while tag, snapshot-id, and timestamp pins keep the snapshot
  schema and unknown refs keep the pinned refusal. Nine proven clauses; FINDING
  BS-TAGOPT-1 flags the brief's `.option("tag")` clause against the pinned IPI-23
  refusal. `risk_tier: standard`. Branch `fix/ipi-07-branch-read-schema`.
  pins: ipi-07-branch-read-schema-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
  C-010 (`file_list_view` refuses) is REJECTED since 2026-09-22, superseded by the ledger below.
- [ipi-30-orphan-guard-narrow-1-ledger.md](ipi-30-orphan-guard-narrow-1-ledger.md) —
  **IPI-30-ORPHAN-GUARD-NARROW-1 (2026-09-22), in flight:** owner ruling Q-55-6. The
  `remove_orphan_files` shared-root guard now refuses only a scan path that is the CTAS
  fallback root, a parent of it, or a directory holding another table of the catalog, so a
  fallback table created without an explicit table `LOCATION` sweeps its own directory.
  `file_list_view` is accepted: `dry_run` lists the
  view's orphans verbatim, and an armed call deletes exactly those. Registry row ORPHAN-3.
  `risk_tier: high`. Branch `fix/ipi-30-orphan-guard-narrow`.
  pins: ipi-30-orphan-guard-narrow-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009
- [ipi-23-mt-describe-1-ledger.md](ipi-23-mt-describe-1-ledger.md) —
  **IPI-23-MT-DESCRIBE-1 (2026-09-22), in flight:** `DESCRIBE [TABLE]`/`DESC` on a
  dotted Iceberg metadata table answers one row per column of the metadata table
  from the same provider schema `SELECT *` resolves; a missing base raises
  `TABLE_OR_VIEW_NOT_FOUND` naming the full metadata-table name as written, and
  `EXTENDED`/`FORMATTED` print the column rows only. Facade pins replay the recorded
  `R-MT-DESCRIBE` cell plus spelling variants, the two-part `USE` form, the
  critic-r1 refusal pins, and four near misses; registry row `DESC-1` re-ruled to
  the served behavior. Critic r4 (md-r6fix) re-pins C-004/C-011 to the 42P01
  answer, adds the C-017 real-table-wins collision pin, and records the
  nested-namespace DESCRIBE gap as D-4. Critic r5 (md-r7fix) makes the
  missing-base answer name the identifier as written — case kept, and the `$`
  form kept for a quoted `t$snapshots` name — pinned by C-018/C-019. Critic r7
  (md-r9fix) makes the `$` metadata name split at the last `$` like the pinned
  fork's SELECT path — a base containing `$` describes its metadata table
  (C-020), the base describes itself (C-021), and an unknown `$` suffix keeps
  the refusal (C-022); the IPI-30 `C-009` citation the branch deleted is
  restored.
  `risk_tier: standard`. Branch `fix/ipi-23-describe-metadata-table`.
  pins: ipi-23-mt-describe-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019,
  C-020, C-021, C-022
- [ipi-23-mt-reader-1-ledger.md](ipi-23-mt-reader-1-ledger.md) —
  **IPI-23-MT-READER-1 (2026-09-22), in flight:** `format("iceberg").load("c.n.t.<meta>")`
  answers what SQL answers, and with `versionAsOf` / `timestampAsOf` what SQL `AS OF`
  answers — the #802 decision moved to `repark_core::time_travel::metadata_at` with two
  callers (SQL door, `read_table_at`); the un-pinned arm quotes into the router path.
  Twenty-two facade pins assert reader/SQL equality plus near misses and — after
  critic r2 — the absolute recorded field type, nullability and rows where the
  cells measured them; the two recorded cells replay Spark-equal.
  **critic r3 (2026-09-23):** the `all_*` columns pin absolute against Spark
  4.1.2, the live leg drops its file-split-dependent pins, and C-015/C-019 are
  marked router-equivalence-only — Spark refuses the quoted-dollar spellings
  (pre-existing divergence filed). The Rust pin
  `metadata_asof_nested_namespace_real_table_wins` covers the four-part
  real-table-wins branch. **critic r4 (2026-09-23):** the offline seed
  deletes the whole-file row id 3 and every layout-dependent pin is now an
  invariant multiset or sum; the Rust file-count pins are reported, not
  edited. `risk_tier: standard`. Branch
  `fix/ipi-23-reader-metadata-tables`.
  pins: ipi-23-mt-reader-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020, C-021,
  C-022
- [u10-mc-deleted-1-ledger.md](u10-mc-deleted-1-ledger.md) —
  **U10-MC-DELETED-1 (2026-09-23), in flight:** `_deleted` joins the served
  metadata-column set as a non-null Boolean after `_partition` — the projected
  name reaches the pinned fork's include-deleted scan mode, so
  `SELECT id, _deleted` on a merge-on-read table answers the recorded
  `R-MC-DELETED` cell `[[1,true],[2,false],[3,false],[4,false]]` on both doors
  while not projecting it keeps the delete filter and `SELECT *` stays user
  columns. The unserved-token machinery is deleted and the composed refusal
  names all five columns. mcdel-r2 pinned the live-Spark S1–S14 row lists
  verbatim (predicate-only, subqueries, expressions/order/group, the empty
  self-join) and recorded two pre-existing KNOWN DIVERGENCEs with full-message
  pins: quoted `` `_DELETED` ``/`` `_FILE` `` refuse
  `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` where Spark resolves quoted `` `_DELETED` ``
  (quoted `` `_FILE` `` unmeasured on Spark), and `_file` / `_deleted` over
  `VERSION AS OF` refuse unresolved where Spark was measured serving them.
  mcdel-r3 pinned the live-Spark P1–P8 rows in order: the composed
  `_spec_id` + `_deleted` rows on both tables, the discriminating
  `a.id = b.id + 1` joins that prove the right-side `_deleted` reaches its scan,
  and the `SELECT *` rows under `ORDER BY id` and `ORDER BY id DESC`.
  mcdel-r4 asserts the merge-on-read types beside the copy-on-write ones and
  refuses a named metadata column that the table schema also carries with Spark's
  reserved-name text (C-014, all five names). `SELECT *`, `DELETE` and other
  metadata columns on such a table stay served: KNOWN DIVERGENCE
  `R-MC-RESERVED-NAME-SCAN` (C-015). It also trues up the `ICE-MC-FILEPOS-1` registry
  row and the predecessor ledger.
  mcdel-r5 measured the collision rule on both engines (M-12). The bracket lists the
  referenced colliding names in the table's declaration order. A query naming no
  colliding column answers on Spark too, so C-015 now keeps only the divergent
  `SELECT *`, copy-on-write `DELETE` and `WHERE` shapes. Near misses are pinned for
  all five names, and the join case is recorded as residue candidate
  `R-MC-RESERVED-NAME-JOIN` (C-016).
  mcdel-r6 moved the collision check to the scan's read columns (Spark's rule,
  M-14). Aliases, CTE names, table aliases and the join now answer like Spark
  (C-016 PROVEN, C-017), and the class-N positions were measured on both engines
  (M-15). The pins live in `metadata_columns_reserved.rs`.
  mcdel-r7 made every answer leg assert its field names and full rows (C-012), and
  measured and pinned the join and query positions (M-17). It also recorded the
  qualified-wildcard silent wrong answer as residue candidate
  `R-MC-QUALIFIED-WILDCARD` (C-018, M-18), pinned as RePark's answer at that time.
  mcdel-r8 fixed it and withdrew the candidate (C-018 FIXED, M-20). mcdel-r9 scoped each
  wildcard to its own SELECT (C-019, M-21). mcdel-r10 and mcdel-r11 removed three
  unreachable arms and saved the mutation sweep (M-22). mcdel-r12 recorded that
  reserved-name refusals share Spark's text but not its class (residue
  `R-MC-RESERVED-NAME-CLASS`) and pinned the comma-join `input_file_name()` refusal
  (C-020, residue `R-MC-IFN-COMMA-JOIN`). mcdel-r13 corrected the ledger header: the
  registry row, the current base `970ac11a` and the per-round model.
  `risk_tier: standard`. Branch `fix/u10-mc-deleted`.
  pins: u10-mc-deleted-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020
- [u7-write-df-ledger.md](u7-write-df-ledger.md) —
  **WO U7 PR1 (2026-09-24):** the DataFrame writer surface. `format("iceberg").save(name)`
  is a table write in every mode (Spark's TABLE_OR_VIEW_NOT_FOUND / ALREADY_EXISTS texts,
  partitionBy checked against the table), a path target refuses as Spark does or keeps the
  declared create-mode refusal; `bucketBy` on `saveAsTable` is the Iceberg bucket transform
  (CTAS/RTAS through the SQL door's `CLUSTERED BY`, layout-checked appends, Spark's
  sorted/multi-column refusals; Ruling R-1 retired); `output-spec-id` stages files under the
  requested spec. Four scoreboard cells EQUAL, nine mutations red. `risk_tier: standard`.
  Branch `feat/u7-write-df`. Round 2 (critic r1 V-001..V-012): Ruling Q1's missing-column
  answer, the staged spec deciding the writer and the dynamic scope, `NumberFormatException`,
  the path relation, the `CLUSTERED` scan and one Rust statement kernel for both writers.
  pins: u7-write-df/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018
- [u7-write-df-2-ledger.md](u7-write-df-2-ledger.md) —
  **WO U7 PR2 (2026-09-24):** DataFrame writer semantics. Slice 1: every `saveAsTable`
  overwrite is Spark's RTAS (uuid, properties and refs kept; schema, spec and sort order
  replaced; the replace snapshot has no parent), and a `branch`/`tag` writer option is
  ignored as on Spark (the write lands on main). Cells `W-DF-SAVEASTABLE-OVERWRITE` and
  `W-DF-V2-OPTION-BRANCH` EQUAL; residue R-1 (format-less provider property) is registry
  `EX-W2-5`. Round 2 (critic r1): a replace keeps field ids by name (Java's
  `assignFreshIds`), so an older branch reads its rows; residues R-9 and R-10.
  Slice 2: `writeTo(t).overwrite(condition)` is Spark's overwrite by filter on U8's
  REPLACE WHERE door (built on repark#833, rebased onto main once #833 and #835 merged), with
  `validate-from-snapshot-id` beside an `isolation-level`; cells `W-DF-V2-OVERWRITE-COND-PART`
  and `-ROWS` EQUAL, residues R-2..R-8 in registry EX-W2-1. Slice-2 round 2 (critic r4,
  2026-09-25): the frame binds to the table by name in the Rust door (`EXTRA_COLUMNS` where a
  positional write committed shifted columns), `isolation-level=none` refuses as on Spark, and
  the critic's 21 shapes are pinned (residues R-11..R-14). Slice-2 round 3 (critic r3,
  2026-09-25): `isolation-level` is parsed only on the doors Spark parses it on (append and
  create ignore `none` and `bogus`), the by-name binding reaches struct sub-fields, and
  residues R-15..R-18 are pinned beside Spark.
  `risk_tier: standard`. Branch `feat/u7-write-df-2b` (slice 1: `feat/u7-write-df-2a`, repark#835).
  pins: u7-write-df-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
