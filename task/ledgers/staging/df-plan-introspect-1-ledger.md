# Charter ledger — DF-PLAN-INTROSPECT-1 · `DataFrame.inputFiles` + `DataFrame.semanticHash` in Rust

**Date:** 2026-09-14 · **Branch:** `feat/df-plan-introspect-1` · **Base:** `origin/main`
`efcb14efa30ad360503ad1c9b9da2527a923041a` · **Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** EX-DF-11 FIXED 2026-09-15 by round 3 (R-9: `sameSemantics` answers plan equality).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** DF-SURFACE-A-1's first cut parsed EXPLAIN text for both names and the critic
showed wrong answers (L-006: a `, ` path split into three URIs; L-007: an overlapping-name
join raised instead of listing files; L-009: the hash covered physical batch/shuffle
tokens). Owner standing instruction (2026-09-14): plan introspection lands in Rust.
Home: `crates/repark-core/src/plan_introspect.rs`.

**Not in this unit:** glob-escape semantics for bracket-bearing directory segments (both
engines glob read paths; §6); the bracket-dir writer anomaly (§6, write path — another
lane's territory); Iceberg manifest reads for data files (§5, measured `[]`); memtable
data-content hashing (§5 boundary); `STATUS.md`, `briefs/next-sequence.md`.

## PROPOSITION LEDGER — DF-PLAN-INTROSPECT-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `inputFiles()` answers PySpark 4.1.2 from Rust: the four oracle cells plus a `", "`/bracket filename as one URI, an overlapping-name join listing both sides, csv/json reads, a glob, a cache-kept lineage, and the SQL door. | `test_df_plan_introspect_1.py` inputFiles pins, red on the base. | **PROVEN** | 10 pins green on the release module. Red on base `efcb14ef`: all 19 pins `PySparkAttributeError [ATTRIBUTE_NOT_SUPPORTED]` (both names absent). Oracle filename `part-` is Spark's writer naming; repark writes `<rand>_0.parquet`, so the parquet pin asserts shape (one `file://` URI, exists) not the name. pins: df-plan-introspect-1/C-001 |
| C-002 | `semanticHash()` answers PySpark 4.1.2 from Rust: the five oracle cells plus literals, column order, limits, CAST widths, two file paths, and batch-size independence, folded to a Java-int range. | `test_df_plan_introspect_1.py` semanticHash pins, red on the base. | **PROVEN** | 9 pins green on the release module, same red base as C-001. Fold: low 32 bits of the process-seeded SipHash digest read as signed `i32`, widened to `i64`. pins: df-plan-introspect-1/C-002 |
| C-003 | Example coverage, maps, and ledger grammar close the loop with no baseline raised. | New dataframe example, refreshed inventory, count pin, map rows, grammar checker. | **PROVEN** | `docs/examples/dataframe/plan_introspect.py` covers both names and runs green; `inventory.txt` gains exactly the two rows; `len(rows)` 961 → 963; `check_example_coverage.py` and its parity suite green; `core.py` holds its exact 4035 baseline (two bindings absorbed into one collapsed raise); `check_ledger_grammar.py` green except the orchestrator-owned attestation block. pins: df-plan-introspect-1/C-003 |
| C-004 | No regression: `make verify`, the DataFrame test files, and the API inventory stay green. | The gates. | **PROVEN** | `make verify` green except the sanctioned attestation finding (see C-003); the touched suites (`test_df_surface_a_1`, `test_examples_dataframe_*`, `test_ex_0_example_coverage`, `test_dfcore_1_exports`) green; full facade and parity suites green (counts in the handback). The facade run first failed exactly the three `test_dfcore_1_exports` freeze pins (new `plan_introspect` submodule, two new dir names); the freeze now absorbs them per that file's own precedent. pins: df-plan-introspect-1/C-004 |

## Per-name decisions

| Name | Verdict | Reason |
|---|---|---|
| `DataFrame.inputFiles` | implemented | File-group walk over the built physical plan in `repark-core`; Python binds the name and picks the cache lineage, nothing else. |
| `DataFrame.semanticHash` | implemented | Analyzed-logical-plan canonical hash in `repark-core`; Python binds the name only. |

## 5. Measurements and rulings applied

**sameSemantics implication.** `f.sameSemantics(f)` is `True` and `f.semanticHash()` is
stable, so self-implication holds; independently built twins answer `sameSemantics ==
False` with equal hashes, so the implication holds vacuously on every pinned shape.
EX-DF-11 is unchanged.

**Iceberg exposure.** The fork's `IcebergTableScan` exposes table, snapshot, projection,
predicates, and limit — never a materialized data-file list — so Iceberg frames answer
`[]`. Measured live on this branch (memory catalog, `CREATE TABLE probe_ice_t (a INT)
USING iceberg`, two rows): `collect()` answers both rows, `inputFiles()` answers `[]`,
`semanticHash()` answers `-1080756021`. A data-file list needs manifest I/O the exec
does not do; candidate registry row, reported not filed.

**Memtable boundary.** Same-schema different-data memtables share a hash (schema shape
only — row content needs a scan the hash must never trigger). No pin covers it.

**Snapshot boundary.** Two scans of one table at different snapshots share a hash (the
fallback provider fingerprint is the table type). No pin covers it.

## 6. Out-of-scope observations (not fixed — write path and glob semantics belong elsewhere)

**Bracket-bearing directory segments are un-globbable.** `spark.read.parquet` over a
`br[0]` dir refuses at plan time (`No files found`, or `Pattern syntax error` for an
unbalanced `[`); the COPY write refuses the same way. Both engines glob read paths, so
the Spark side of a bracket-dir read is UNMEASURED (no JVM in this lane). The pin
therefore carries the brackets in the FILE name (`data [0], x.parquet` under an
`if, a` dir): the URI integrity the L-006 class is about, through public doors only.

**Bracket-dir writer anomaly.** Writing into a `brackets [0]` dir drops a stray
`<rand>_0.parquet` in the PARENT directory and names the in-target file
`part-00000.parquet` (the plain path names `<rand>_0.parquet` in-target only).
Write-path territory; reported, not touched.

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Follow-up round — 2026-09-15 (R-1..R-4, P2-1/P2-2)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-006 | The follow-up hash answers hold: local-frame construction identity, the cache-keeps-hash lineage round-trip, cached `inputFiles` lengths, cube/rollup discrimination, hex column names, temp-view equality, and the URI form. | The eight `C-006` tests over the twenty-three `planintro_*` oracle cells, red first on the pre-follow-up implementation. | **PROVEN** | 27/27 green on the rebuilt release module. Red first measured 2026-09-15 (new tests over the old implementation): `local_frames_differ_by_data`, `cache_keeps_hash`, `cube_rollup_differ`, `hex_names_differ`, `temp_view_matches_frame` red; `stable_for_one_frame`, `inputfiles_cached_frame_matches_spark`, `uri_form` green. pins: df-plan-introspect-1/C-006 |
| C-007 | The follow-up hash stays fast: a depth-200 chained-filter `semanticHash` keeps its answer while the analyze-skip plus streaming rewrite removes the old cost. | Depth-200 timing before/after on the release module. | **PROVEN** | Before 0.4143s median; after 0.0001s median (11 samples, 0.00006–0.00008s, same shape, release module, 2026-09-15). The old cost was the unconditional analyzer pass plus the intermediate canonical string. pins: df-plan-introspect-1/C-007 |

**Supersessions (dated, §5 stays as the 2026-09-14 record).** R-1 replaces the
"Memtable boundary" ruling: independently built memtables hash by construction
identity now, so same-data twins no longer share a hash and §5's
`sameSemantics`-with-equal-hashes vacuous case no longer occurs — twins answer
unequal hashes. R-2 adds the cache-lineage map (Python gathers live cache-view
name to pre-cache plan pairs; the hash expands scans through it). The view-strip
rule (`?table?` and lineage-known qualifiers strip, scan-level pushed-down
filters are not hashed) makes a filter over a cache view equal the same filter
over the base table. R-3 keeps the hex-norm on identifiers and discriminates
cube / rollup / grouping sets by tag (the old always-analyze pass had erased the
cube/rollup distinction). R-4 is the GIL-detached binding.

VERDICT (whole ledger, 2026-09-15): 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Follow-up round 2 — 2026-09-15 (R-6..R-8, always-analyze plus door canonicalization)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-008 | A DF-API `filter`/`where` and the same SQL `WHERE` hash equal, and a temp view over a filtered frame reads back equal through both `spark.sql` and `spark.table`. | The R-6 tests over `planintro_df_filter_vs_sql_where`, `planintro_df_filter_col_vs_sql_where`, `planintro_where_vs_filter`, `planintro_filter_then_select_vs_sql`, `planintro_view_over_filtered_sql`, `planintro_view_over_filtered_table`, red first on the round-1 head. | **PROVEN** | 35/35 green on the rebuilt release module. Red first on head `c48557dc` (new tests over the round-1 implementation): `test_semantichash_df_filter_matches_sql_where` (`assert (-172874460 == -936395348) == True`), `test_semantichash_filter_then_select_matches_sql` (`assert (-552705930 == 633446044) == True`), `test_semantichash_view_over_filtered_matches_frame` (`assert (359928196 == -250295646) == True`), `test_semantichash_identity_selects_match_frame` (`assert (1475915780 == 1120403206) == True`); the SQL-door selects, the reorder, and the Iceberg pins were already green. pins: df-plan-introspect-1/C-008 |
| C-009 | An identity projection on either door hashes equal to its input; reorder, rename, subset, and expression projections stay `Projection` nodes. | The R-7 tests over `planintro_select_all_named_vs_df`, `planintro_select_cols_vs_df`, `planintro_select_star_vs_df`, `planintro_sql_select_named_vs_df`, `planintro_sql_select_star_vs_df`, `planintro_select_reordered_vs_df`, plus the rename/subset/expression/string-cast-fallthrough asserts, red first as above. | **PROVEN** | Same green run; the DF-door identity red above is the R-7 fail-before. `is_passthrough` now accepts a same-named `Alias` of a column next to a bare `Column`. pins: df-plan-introspect-1/C-009 |
| C-010 | `inputFiles` keeps Spark's `[]` for an Iceberg table scan, pinned, with the registry row filed. | `test_inputfiles_iceberg_scan_is_empty` plus row DF-PLAN-INTRO-1 in `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Green on the round-1 head already (no code change on this arm); the row sits after DF-OBSERVE-1 and carries the `[]` note plus the R-1 construction-identity line. pins: df-plan-introspect-1/C-010 |

**R-6 (L-101, P1, recorded as ordered).** The `needs_analyze` skip is removed:
`hash_plan` always runs the analyzer, then streams into the hasher with the GIL
still detached. Always-analyze alone fixed the view arms but NOT the predicate
arms, so the round adds three measured normalizations, each probed before it was
kept (temporary analyzer-shape probes, since removed): every column qualifier
strips to the bare name (the analyzer keeps the view qualifier `v.a` after
inlining, and the DF doors disagree with each other on it); an integer-literal
cast folds to the target width (the SQL parser mints `Int64` while the Column
API mints `Int32`, and the analyzer casts one side only); an integer comparison
between a column (through int casts landing on the literal's own width) and an
integer literal hashes as operator plus bare name plus `i128` value (the
analyzer widens the *column* to the literal type on the string/SQL doors and
leaves the Column door bare). The optimizer was measured and rejected: it
constant-folds filters down to literal-vs-literal over `EmptyRelation`, which a
semantic hash must never equate. Depth-200 chained-filter timing on the release
native: median 0.0047s over 11 samples (0.0047–0.0051s, digest `159053862`,
2026-09-15) against 0.0001s with the skip and 0.4143s before the follow-up; the
cost is the analyzer pass, linear in plan depth, and per the ruling no timing
repair may move a hash.

**R-7 (L-102, P3 upgraded by the oracle, recorded as ordered).** The oracle
(`planintro_l102_probe_2026-09-15.json`, twelve keys each holding `equal_hash`
and `sameSemantics`) is transcribed one key per cell as
`planintro_<key>` with `{"result": {"equal_hash": …, "sameSemantics": …}}`;
`select_all_named`, `select_cols`, `select_star`, `sql_select_named`,
`sql_select_star` are true/true and `select_reordered` is false/false, and every
new pin reads its cell's `equal_hash`. The cells' `sameSemantics` values are
transcribed but never asserted: repark answers `False` for distinct handles by
EX-DF-11 while Spark answers `True`, so asserting them would pin a disposed
divergence as a failure.

**R-8 (Q-15B-3, owner ruling 2026-09-15, recorded as ordered).** `inputFiles`
for Iceberg stays `[]`; row DF-PLAN-INTRO-1 filed after DF-OBSERVE-1, appended
not reordered, carrying the one-line `[]` note and the one-line R-1
construction-identity note. Spark itself answers unequal hashes with
`sameSemantics` false for independently built same-data twins (cells
`planintro_local_data_same_data_equal_hash` /
`planintro_local_data_same_data_same_semantics` are both false), so
construction identity is Spark-equal, not a divergence.

VERDICT (whole ledger, 2026-09-15 round 2): 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Follow-up round 3 — 2026-09-15 (R-9..R-11, plan-equality sameSemantics plus user-cast rule)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-011 | The cast probe answers hold on both fields: the alias twin, the same-frame filter twin, the Column-vs-SQL int filter, and the literal-width pair answer true/true; the string cast, the SQL-door narrowing cast, the Column-door narrowing cast, and the widening cast answer false/false. | `test_semantichash_cast_cells_match_oracle` over the eight `planintro_cast_*` cells, red first on the round-2 head, plus `sameSemantics` asserts on the twelve round-2 cells. | **PROVEN** | 36/36 green on the rebuilt release module. Red first is the orchestrator-recorded round-2 answers (`planintro_cast_repark_2026-09-15.txt`): `narrow_cast_vs_plain_col` and `widen_cast_vs_plain_col` answer `equal_hash` true against the oracle false, and every `sameSemantics` answers `False` against oracle `True` on the twin arms. pins: df-plan-introspect-1/C-011 |
| C-012 | `dataframe/core.py` holds its ceiling with ruff clean after the `sameSemantics` move. | `uvx ruff@0.15.22 check python/repark` plus `format --check`, and both ceiling tables ratcheted to the real count. | **PROVEN** | `ruff check` and `format --check` clean; `core.py` 4027 → 4014 in `scripts/check_lib_py.py` and the CAP-1 mirror, with the `scripts/map.md` note. pins: df-plan-introspect-1/C-012 |

**R-9 (shape rule; retires EX-DF-11, recorded as ordered).** `sameSemantics`
answers Spark's plan equality: true exactly when the two analyzed plans produce
the same canonical byte stream in Rust. The binding hashes nothing twice: one
`ByteSink` pass per plan collects the same bytes the `DefaultHasher` pass
collects, and equality compares the streams, never the 32-bit fold alone. The
NOT_DATAFRAME type gate stays. The method body moves to
`dataframe/plan_introspect.py` behind the one-line class binding, with native
`same_semantics(left, right, lineages)` in `repark-core` and `repark-python`.
Alias twins, same-frame filter twins, Column-vs-SQL int filters, and
literal-width pairs answer true; narrowing, widening, and string casts answer
false; independently built local frames stay false by construction identity
(cell `planintro_local_data_same_data_same_semantics`). EX-DF-11 flips to
FIXED 2026-09-15 with its pin renamed to
`test_same_semantics_alias_plan_equality`; the one `docs/examples` sameSemantics
example still passes unchanged (its False arms are local-frame twins, still
false by construction identity). The `range(5)` twin arm in `test_c5_census_r7`
flips to True as the same rule requires.

**R-10 (recorded as ordered).** A cast the user wrote is part of the plan and
is never stripped or folded; only analyzer-inserted coercion (and literal-width
differences between the SQL parser and the Column API) normalizes. The round-2
width rule is retired: the strip now consults the pre-analysis plan
structurally, per comparison, as (column, operator, literal value). A column
cast beside a bare literal in pre-analysis is user-shaped and blocks the strip;
a cast beside a cast literal (the SQL door coerces both sides up front, measured
in the debug trace) or no pre-analysis cast at all is coercion and still
strips. The old "Known boundary" paragraph is deleted: the narrowing cells pin
the decided behavior. Remaining boundary (dated 2026-09-15): a user cast on
both sides of one comparison (`CAST(a AS BIGINT) > CAST(5 AS BIGINT)`) reads as
coercion-shaped and still strips; no oracle cell covers that spelling.

**R-11 (recorded as ordered).** The R-9 move frees the lines the wrapped import
needs: `core.py` 4027 → 4014, ratcheted DOWN in both tables with the
`scripts/map.md` note.

**Timings (release native, 2026-09-15, depth-200 chained Column filters).**
`semanticHash` median 0.0055s over 11 samples (0.0054–0.0056s, digest
`920142959`); `sameSemantics` on the same depth-200 pair median 0.0110s over 11
samples (0.0109–0.0111s, two analyzer passes plus the byte compare).

VERDICT (whole ledger, 2026-09-15 round 3): 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-plan-introspect-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-011/C-012 walked against the eight recorded cast-probe cells plus the twelve round-2 cells; red first is the orchestrator-recorded round-2 answers (two equal_hash trues against oracle falses, every sameSemantics false against oracle trues), all green after; every earlier clause re-green in the same 36/36 run.
      artifacts: [python/repark/tests/test_df_plan_introspect_1.py, python/repark/tests/facade_dataframe_surface_oracle.json, crates/repark-core/src/plan_introspect.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Narrowing and widening casts on the Column door and narrowing on the SQL door stay distinct from the plain column; alias twins, same-frame filter twins, Column-vs-SQL int filters, and literal-width pairs answer equal; string casts stay distinct; reorder/rename/subset/expression selects stay distinct; local-frame twins stay unequal by construction identity; range twins answer equal.
      artifacts: [python/repark/tests/test_df_plan_introspect_1.py, python/repark/tests/test_c5_census_r7.py, python/repark/tests/test_examples_dataframe_c.py]
    - id: AT-3
      status: ATTACKED
      evidence: Analyzer failure still maps to the engine error with no panic path (no unwrap in production code); the NOT_DATAFRAME gate moved with the body and both spellings still raise it; stopped-session behavior covered by _ensure_alive on both frames.
      artifacts: [crates/repark-core/src/plan_introspect.rs, python/repark/src/repark/spark/dataframe/plan_introspect.py]
    - id: AT-4
      status: ATTACKED
      evidence: No shared mutable state added; the GIL stays detached across analyze plus streaming hash on both same_semantics arms; the MemTable construction-identity boundary unchanged and re-pinned green.
      artifacts: [crates/repark-python/src/plan_introspect.rs, python/repark/tests/test_df_plan_introspect_1.py]
    - id: AT-5
      status: ATTACKED
      evidence: No new input surface (same two doors, same signatures plus the documented plan-equality semantic); the user-shaped block fires only on int column-vs-literal comparisons with a bare pre-analysis literal, with a structural fallthrough everywhere else, pinned by the string-cast and both-sides note.
      artifacts: [crates/repark-core/src/plan_introspect.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Every hash this round moves is re-pinned (36/36 green, including all C-001/C-002/C-006/C-008/C-009 pins unchanged); only the two Column-door cast cells change hash, exactly the oracle falses; the both-sides-user-cast corner is declared in R-10, not absorbed.
      artifacts: [python/repark/tests/test_df_plan_introspect_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Depth-200 chained-filter semanticHash median 0.0055s and sameSemantics median 0.0110s on the release native (linear analyzer cost times two passes plus the byte compare, streaming hasher kept, no intermediate string).
      artifacts: [task/ledgers/staging/df-plan-introspect-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency change (Cargo.toml, Cargo.lock, pyproject.toml, uv.lock, .github untouched); DataFusion TreeNode/analyzer APIs used as the session guards already do; ceilings ratcheted down, never up.
      artifacts: [crates/repark-core/src/plan_introspect.rs, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every new pin prints both hashes and both sameSemantics on failure; the ledger keeps the orchestrator-recorded fail-before file next to the ruling.
      artifacts: [python/repark/tests/test_df_plan_introspect_1.py, task/ledgers/staging/df-plan-introspect-1-ledger.md]
    - id: AT-10
      status: ATTACKED
      evidence: Two equal_hash pins and every twin-arm sameSemantics pin red before the fix, green after; every added branch names its triggering input (user-shaped block by the narrowing/widening cells, coercion passthrough by the SQL-door cells, literal fold by the literal-width cell, ByteSink compare by the alias/filter twins).
      artifacts: [python/repark/tests/test_df_plan_introspect_1.py, crates/repark-core/src/plan_introspect.rs]
  complete: true
```
