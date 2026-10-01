# Unit ledger — STRING-LITERAL-ESCAPE-1 · SQL string literals unescape as Spark does

**Date:** 2026-09-29 · **Branch:** `fix/string-literal-escape-1` · **Base:** `5fb38051` (`origin/main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PE-10 (S1, silent, pre-existing): `SELECT "x\\""y"` answers `x\""y`
on RePark where Spark 4.1.2 answers `x\"y` — a doubled `""` inside a
double-quoted literal is not collapsed, and the wrong value is stored or
returned without any error. The Step-0 matrix (C-000) measures 229 literals on
five doors under both `escapedStringLiterals` settings against live PySpark
4.1.2; this unit makes the shared Rust value engine quote-aware, answers
`r"…"` literals, splits raw doublings into head plus quoted tail, and keeps
verbatim values exact — on `spark.sql`, `F.expr`, `selectExpr`, `filter`, and
the write path.

**Not in this step:** `r`/`R` followed by three or more quotes (sqlparser's
triple path, which Spark's pair-lexing does not share — 7 literals stay
error-vs-value, unchanged from base); `F.expr` under `escapedStringLiterals=true`
(the Column plans on a sessionless context, so the flag cannot reach it —
pre-existing, previously scoped to session doors by FNP-4B);
`spark.sql.ansi.doubleQuotedIdentifiers` (no carrier; measured only); `STATUS.md`,
`briefs/`, `.github/`, `Cargo.toml`, `Cargo.lock`.

## PROPOSITION LEDGER — STRING-LITERAL-ESCAPE-1 — 2026-09-29

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-000 | The Step-0 matrix is measured before any edit: 229 literals (every Spark escape in both quote types, doublings before/after/between escapes, adjacency, raw forms, 54 controls) on five doors under both `escapedStringLiterals` settings, on RePark and live PySpark 4.1.2; the premise holds (`"x\\""y"` → `x\""y` vs Spark `x\"y`); 22 default-mode and 69 verbatim sql-door diffs isolate to the unescape layer. | The four probe JSONs agree cell by cell; the comparison table is in the hand-back. | PROVEN | Measured 2026-09-29, PySpark 4.1.2 `local[1]` via `jvm-lock.sh`. Spark values baked into the pins verbatim. |
| C-001 | Default-mode values are quote-aware: `""` collapses only in double-quoted literals and `''` only in single-quoted (cross-type doublings pass through); `r"…"`/`R"…"` answer; every default diff cell flips to Spark on all five doors and the write read-back. | The `double_quoted_*` Rust pins plus `DEFAULT_CASES` facade pins green; post-fix probe shows only the `r''''` triple-raw edge. | PROVEN | Post-fix default: 5/1145 cells differ, all `r''''` on five doors; write read-back differs on that row only. |
| C-002 | A raw `''`/`""` ends the raw token: the head stays verbatim and the tail unescapes with the same quote's rules (`r'a''\n'` → `a`+LF, `r'a''b''c'` → `ab'c`); in verbatim mode the raw value is the opening quote plus the raw text with the first doubling removed (`r'a'` → `'a`). | The `raw_doubled_quote_splits_like_spark` Rust pins plus the raw facade rows green. | PROVEN | Split rule confirmed by discriminator cells on Spark (`r'a''\n'`, `r'\n''\t'`, `r''''` → empty); uppercase `R` splits identically. |
| C-003 | Verbatim mode keeps the raw text exactly (backslashes AND doublings; `d-dq`-family downstream errors become values); every verbatim session-door cell matches Spark; the `F.expr`-ignores-verbatim gap is measured and disclosed, not absorbed. | The `verbatim_keeps_*` Rust pins plus `VERBATIM_CASES` facade pins green; verbatim non-expr non-`r''''` diffs are zero. | PROVEN | Post-fix verbatim: 0 non-expr diffs outside `r''''`; ~150 expr-door diffs are the pre-existing sessionless-planning gap. |
| C-004 | Nothing else moves: every Step-0 match stays a match (zero match-to-diff transitions); the native door answers byte-identically; NTZ/LTZ and escape neighbours green; the full `repark-spark` suite green; byte-frozen files keep their hashes. | The before/after transition check, the native rerun, neighbour suites, and the freeze record green. | PROVEN | 2472 passed, 0 failed; `test_pr_245_revalidation_record.py` 11 passed; native before==after. |
| C-005 | DIFF-PROBE fold: verbatim keep-exact covers query-expression literals and `OPTIONS` values only; DDL property lists (`TBLPROPERTIES` / `PROPERTIES` / `DBPROPERTIES`) and `COMMENT` runs in `CREATE` / `ALTER` take default treatment, so all 6 regression cells equal Spark 4.1.2. | The `verbatim_ddl_positions_take_default_treatment` Rust pins plus the 4 verbatim-DDL facade read-back pins green; the 6 cells equal their Spark oracles cell by cell. | PROVEN | `o10r`, `v_cmt`+`v_cmt_r`, `v_tb_dq_r`, `w_alter_r` (key and backslash value), `w_cmt1_r` all Spark-equal; `SELECT comment` alias guard and `OPTIONS`-value survivor pinned. |
| C-006 | The whole-probe replay moves nothing else: 489 of 497 cells are byte-identical old-head to new-head; the 8 that move are the 5 fix read-backs plus 3 red-at-both-sides backslash cells, all 8 Spark-equal; 0 control diffs. | Old-head rerun into a separate out dir, byte-compared cell by cell; every moved cell compared against its Spark oracle. | PROVEN | New flips `v_tb_esc_r`, `v_tb_bsdoub_r`, `w_cmt2_r` Spark-equal; `v_cmt` CREATE succeeds on both heads; 43 INTENDED + 16 SPARK-CONFIRMED head values byte-identical. |
| C-007 | Namespace `PROPERTIES` / `DBPROPERTIES` take the same DDL treatment (Spark fully unescapes there, measured both shapes); `COMMENT ON` is out of scope (Spark collapses doublings but preserves backslashes there — a different rule, verbatim doubling red recorded); default-mode `COMMENT` doubles refusal stays (pre-existing, both modes). | Facade namespace pins green; Spark oracle cells for namespace escapes, `COMMENT ON` × modes, and default `COMMENT` doubles recorded in the fold evidence. | PROVEN | Namespace doubling/backslash/`\u0041` Spark-oracled; `COMMENT ON` backslash green both modes (do-not-touch), doubling red verbatim-only; default `COMMENT "x""y"` ParseException on both heads. CORRECTED 2026-09-30 by C-008: clean Spark reruns show `COMMENT ON` fully unescapes both modes. |
| C-008 | VE-1: `UNSET TBLPROPERTIES [IF EXISTS]`, `SHOW TBLPROPERTIES t (key)` and `COMMENT ON … IS …` always unescape in verbatim mode (UNSET removes the collapsed key, SHOW-key returns the collapsed value, COMMENT ON stores the fully unescaped text both modes); default mode unchanged. | Rust DDL-table rows + facade VE-1 pins + control pins green; verify-esc/DIFF-PROBE replay at 0 regressions. | PROVEN | 6/6 Rust DDL rows green; 4/4 facade VE-1 pins green; mutation red-first (3 verbatim pins red, default control green); replay moves only to Spark-equal. |
| C-009 | VE-2: `filter`/`where` strings read the frame's session verbatim flag and `F.expr` reads the active session's build-time flag, so all three parse like the session door; default mode takes the identical path as before. | Rust frame-flag pin + facade VE-2 pins + control pins green; verify-esc/DIFF-PROBE replay at 0 regressions. | PROVEN | Rust frame-flag pin green; 3/3 facade VE-2 pins green; each half mutated red-first (frame, `F.expr`); the ten `dv*e` replay cells flip Spark-equal; replay at 0 regressions. |
| C-010 | VE-3: every DML re-render (`UPDATE`/`DELETE` selections, `SET` values, MERGE `ON`/predicates/`VALUES`/assignments/sources, both doors, both fragment rewrites) preserves string values exactly; verbatim `''`/backslash and default quad conditions match and store Spark-equal values. | `sql_text` round-trip + planning pins + facade VE-3 pins + control pins green; verify-esc/DIFF-PROBE replay at 0 regressions. | PROVEN | 8/8 `sql_text` pins green; 5/5 facade VE-3 pins green; mutation red-first at both levels (6 Rust + 4 facade red); replay moves only to Spark-equal; heals the UPDATE/DELETE/MERGE-SET halves of carried VE-4 items 3 and 4. |
| C-011 | VE2-1 re-verify fold: facade-built SQL parses with verbatim forced off for that one parse (scoped override, session conf untouched); user-written text (`spark.sql`, `selectExpr`, `expr`, `filter`/`where`) still follows the flag; user fragments spliced into built SQL (`F.expr` columns) are pre-rendered under the flag into default-stable text; DDL-defs fragments splice raw (C-005 default treatment). | Rust fragment + override-wiring pins + facade VE2-1 pins + default controls green; reverify-esc/verify-esc replay at 0 regressions. | PROVEN | 4/4 fragment + 8/8 wiring Rust pins green; 13/13 facade pins green; mutation red-first (override, fragment); 19 replay cells flip Spark-equal, 0 true regressions over 1475. |
| C-012 | VE2-2 re-verify fold: nested struct-field `UPDATE`/`MERGE SET` values render through `render_for_reparse`, so verbatim doublings/backslashes store kept and default ones collapse, and `\'` parses instead of refusing; refusal texts quote values as valid SQL. | Rust nested leaf/fold/refusal pins + facade VE2-2 pins + default controls green. | PROVEN | 24/24 nested_assign Rust pins green (4 new re-render/refusal); 2/2 facade VE2-2 pins green; mutation red-first (`.to_string()`); nest.py 14/14 Spark-equal. |
| C-013 | CI round: the five failure-injection/SQL-spy seams (`test_catalog_surface_1`, `test_create_dataframe_materialize` x3, `test_eager_own_1`, `test_mapinarrow`, `test_ml_boost_oracle`) observe the `sql_built` door the product now uses for facade-built SQL, with identical assertions — injection still lands on the SQL call after register, the catalog spy still sees cache-view reads, CV still requires the mat-view read; cleanup behavior unchanged. | The 7 seam tests green with assertions untouched; no product change on those paths. | PROVEN | All 8 CI failures reproduced locally then green; the two capped test files rename the seam method line-neutrally, the other three observe both doors. |

## Evidence

### C-000 diff taxonomy (all measured, Spark 4.1.2)

Default mode, 22 literals: nine `""`-doubling cells (`d-defect`, `d-bs-doubled`,
`d-doubled-esc`, `d-esc-doubled`, `d-escq-doubled`, `d-doubled-escq`, `d-between`,
`j-its-d`, `c-dsay`, `x-esc-cross2`), five double-raw refusals (`r-d-bs`,
`R-d-bs`, `r-d-nl`, `r-d-doubled`, `r-d-empty`), six single-raw splits
(`r-doubled`, `x-raw-a`, `x-raw-b`, `x-raw-d`, `x-raw-e`, `x-draw-a`,
`x-draw-c`). Verbatim sql door, 69 literals: all doublings kept by Spark
(`'it''s'` → `it''s`), verbatim raw marked (`r'a'` → `'a`), and three
borrowed-double downstream errors fixed by rewriting. Error-vs-error cells
(`e-*` family, `r'\''`, unterminated) match in kind and are untouched.

### C-001/C-002/C-003 files

Product: `crates/repark-spark/src/spark_literals/unescape.rs` (child module;
the 999-line parent forced the split — no ceiling raised, `lib.rs`
untouched), `spark_literals.rs` (four-arm value dispatch, verbatim-aware
rewrite rule), `spark_rewrites/create_options.rs` (option keys unescape with
their own quote type). Pins: `crates/repark-spark/src/tests/string_literal_escape_1.rs`
(own leaf — the SQP-1 leaf is byte-frozen),
`python/repark/tests/test_string_literal_escape_1.py` (40 default + 11
verbatim cases on five doors plus an iceberg write round-trip).

### C-004 guards and mutation

Neighbours: SQP-1/FNP-4B/LIKE-escape suites 93 passed + 1 pre-existing xfail;
NTZ/LTZ suites 48 passed (`test_ltz_stacked_sign_1.py` absent — PR #882
unmerged; equivalents from the re-verify corpus covered as controls). Native
door: 12/12 byte-identical. Mutation (old unescape restored, signatures
kept): 5/6 new Rust pins red (the borrow pin correctly stays green) and 6/6
facade tests red; fix restored byte-exact (`diff` clean) and green again.

### C-004 known edges (measured, unchanged from base)

| Edge | Spark value | RePark | Reason |
|---|---|---|---|
| `r''''`, `r'''x'''`, `r'''''x'''''`, `r''''''`, `R"""x"""`, `r""""`, `r""""""` (+ verbatim twins) | ``, `x'`, `'x''`, `'`, `x"`, ``, `"` | TokenizerError / ParserError | `r`+3-quote runs take sqlparser's triple path; no fork edit allowed |
| `F.expr` under `escapedStringLiterals=true` | verbatim values | default values | Column plans on a sessionless context; needs session-aware planning |
| `"…"` under ANSI + `doubleQuotedIdentifiers=true` | identifier / `UNRESOLVED_COLUMN` | string literal | no carrier (measure-only per brief) |

### C-005/C-006/C-007 DIFF-PROBE fold evidence (2026-09-29)

Mechanism: `plan_literal_regions` computes `ddl_verbatim_spans` when
`keep_verbatim` is set — `TBLPROPERTIES` / `PROPERTIES` / `DBPROPERTIES`
paren ranges plus `COMMENT` maximal adjacent-literal runs, gated on
`CREATE` / `ALTER` first words (`EXPLAIN` skipped). Literals inside take
default treatment, so verbatim DDL canonicals are byte-identical to default
ones; `COMMENT` doubles holding `""` force a rewrite (borrow refuses there)
into single-quoted form. `skip_whitespace` / `matching_paren` went
`pub(crate)` for reuse (2 words, no behavior change). `OPTIONS` takes no
span: keys already unescape from original text, values splice from verbatim
inners. The merger inherits the first literal's treatment; span edges always
coincide with non-literal tokens, so a merged region never straddles a span
boundary (verified by construction plus the alias-guard pin).

Replay: 497 cells, base outputs reused, Spark oracles reused (every moved
cell has one). Old-head rerun into `out_old/`: 489 cells byte-identical,
the 8 that move are `o10r`, `v_cmt_r`, `v_tb_dq_r`, `w_alter_r`,
`w_cmt1_r`, `v_tb_esc_r`, `v_tb_bsdoub_r`, `w_cmt2_r` — all 8 Spark-equal
at value level. `v_cmt` CREATE succeeds on both heads (only its read-back
moves). The `e` backslash value in `w_alter_r` heals as a side effect
(canonical carries a real LF). `w_nsprop_r` / `w_nscreate_r` do not move
(non-extended reads show no properties). bench_ratio 1.05.

Fold oracle cells (Spark 4.1.2, `jvm-lock.sh`): namespace `PROPERTIES`
doubling/backslash/`\u0041` and `DBPROPERTIES` doubling/backslash/unicode
all fully unescape verbatim; `COMMENT ON` collapses doublings and doubles
but preserves backslashes in both modes (RePark: backslash green both
modes, doubling/doubles red verbatim-only — collapse-only rule, needs its
own unit); column `COMMENT` doubles collapse both modes on Spark
(`x"y`); default `COMMENT "x""y"` ParseException on both RePark heads.

Mutation: the Rust DDL pin red without the fix, green with; 4/5 new facade
pins red without (the `OPTIONS`-value survivor correctly stays green),
11/11 green with. Product files: `spark_literals.rs`,
`spark_rewrites/mod.rs`; pins: `tests/string_literal_escape_1.rs`,
`python/repark/tests/test_string_literal_escape_1.py`;
`python/repark/tests/map.md`, `crates/repark-spark/src/map.md`,
`src/tests/map.md`, `src/spark_rewrites/map.md` in the same commit.

### C-008/C-009/C-010 verifier fold evidence (2026-09-30, PROVEN)

Mechanism VE-1: the DDL statement gate covers `SHOW` and `COMMENT`;
`UNSET … [IF EXISTS]` skips the guard words before the key paren,
`SHOW TBLPROPERTIES t (…)` spans the trailing key paren, `COMMENT ON … IS …`
spans the literal after the last `IS` followed by one (`is_bare_word` /
`is_lparen` / `comment_on_span` helpers; gate renamed
`is_create_or_alter_statement` → `is_property_statement`). Default mode
computes no spans, as before.

Mechanism VE-2: `parse_canonical_predicate[_exact]` read the flag from the
frame (`frame_verbatim` over `task_ctx` options) and run the verbatim
canonicalize + error translation with it; `PyColumn::sql` takes
`keep_verbatim` from `F.expr`, which reads the active session's build-time
key (`functions_session._active_verbatim_flag`). The four Spark entry points
turn `pub` for the binding. `functions.py` 1984 → 1938 via the pure
`functions_lit` move (shed `#` notes recorded in the spark map).

Mechanism VE-3: `write/sql_text.rs::render_for_reparse` swaps each
quote-bearing string value for an indexed placeholder, renders, then splices
the doubled literal back (collision falls back to the plain render). Applied
at every DML render and re-render: predicate selections, `SET` values, the
folded `UPDATE`, all MERGE fragments incl. the derived source on both doors,
plus `rewrite_fragment_case`, `check_fragment_exact`/`check_identity_exact`
and the `fold_query_text` folded exits. `N'…'` was tried and abandoned:
DataFusion does not plan it (`national_literals_plan_as_strings` red).

Spark oracles (all `jvm-lock.sh`, PySpark 4.1.2): reused `ns-spark.json`
v/d_unset_r + v/d_filter + v/d_where and `rt-spark.json` v/d_cm_key +
v/d_cm_on_r; new `cm-spark.json` (COMMENT ON doubling/backslash/both ×
modes: full unescape everywhere — supersedes the C-007 preserve-backslash
note), `pin-spark.json` verbatim UPDATE/DELETE/MERGE/others,
`cf-spark.json` default `F.expr`/dq-UPDATE/MERGE, `qd-spark.json` default
quad UPDATE/DELETE/MERGE.

Method note: a Spark default session built after a verbatim session in one
JVM inherits `escapedStringLiterals=true` (`ord-spark.json`: `SET` shows
`true`). The first pin probe ran verbatim-first, so its default leg was
discarded and re-measured default-first; every oracle above ran
default-first or single-session. By the same token `attack.py`'s third (ansi)
Spark leg may read verbatim — its cells are escape-free, so no finding, but
future Spark legs must run default-first or in separate JVMs.

VE-4 carried items observed (orchestrator files cards): the ten pre-existing
divergences stand, except items 3 and 4 heal on the UPDATE/DELETE/MERGE-SET
paths as a side effect of C-010 (CTAS/INSERT OVERWRITE still collapse);
the 2,000-deep `||` segfault belongs to DEEP-FILTER-CHAIN-CRASH-1 (#892).

Close-out: verify-esc (`attack` 379 + `rt` 150 + `ns` 24 + `dmlv` 36) and
DIFF-PROBE (`dp` 437 + `dp2` 26 + `dp3` 22 + `dp4` 12) replayed post-fix —
1,086 cells, 40 move, 0 regressions, 34 flip to Spark-equal (6 `dmlv` cells
have no Spark leg and read Spark-consistent); every other move is
snapshot-id/path/run-tag noise. Mutations red-first per fix at both levels
(VE-1: 1 Rust + 3 facade; VE-2: 1 Rust + 2 facade halves; VE-3: 6 Rust + 4
facade), controls green throughout, tree clean after each revert. Lane gate
`gate.sh` green.

### C-011/C-012 re-verify fold (2026-09-30)

Override path: `EngineContext::verbatim_override` (repark-core `dialect.rs`,
`None` default) → `StatementWriteOptions::verbatim_override` (set by
`SparkDialect` from `cx`, never from an option key) →
`StatementWriteOptions::effective_verbatim` (override or session flag), read by
`router.rs` for both the canonicalize call and the error translation, so the
translation matches the rewrite actually applied. `ReparkSession::sql_built`
and `sql_built_with_write_options` pass `Some(false)`; `sql_with` passes
`None`. The error-translation sharing keeps `router.rs` at 998 lines (was
1000). Native entry points in `session_write_options.rs` (one `#[pymethods]`
block per type per module, so free `#[pyfunction]`s):
`session_sql_built(session, query)` and
`built_sql_user_fragment(sql, keep_verbatim)`.

User fragments: `F.expr` stores the fragment pre-rendered for the default door
as the column's `sql_expr` (`spark_literals/built_fragment.rs`:
`canonicalize_fragment_for_default_parse` resolves literals under the session
flag, then requotes default-stable; default input returns borrowed, so default
sessions splice byte-identical text). Operator composition renders in Rust
(`PyColumnParts`), so splice positions are full-expression positions where the
old display fallback's parens never mattered. The DDL-defs fragment in
`createTable` splices raw: its only string positions are `COMMENT` text, which
takes default treatment both modes (C-005, re-confirmed on live Spark in the
`ct` oracle cells), and `DEFAULT` is refused.

Internal-caller inventory (every `session.sql` / `inner.sql` caller; user
doors keep the flag, built doors route `_sql_built` (facade) /
`inner.sql_built` (native)): user text — `spark.sql`, `selectExpr`
(`dataframe/core.py`), `expr` (`functions.py`), `filter`/`where` strings, the
UDF-rewrite re-entries (`session_core.py`), the ANSI door. Built —
`merge.py`, `writer_schema.py`, `writer_layout.py` (via
`session_sql_with_write_options`, now `sql_built_with_write_options`),
`joins_columns.py`, `sampling.py`, `udf_window_projection.py`,
`cache_handle.py`, `core.py` (except `selectExpr`), `_csv_smart.py`,
`polars.py`, `catalog.py`, `catalog_surface.py` (DDL + three native scans),
`session_core.py` (`range`, SET constant), `session_surface.py` (fixed TVF
names), `session_maintenance.py`, `session_configuration.py` (SET, intercepted
pre-engine), `create_dataframe_rows.py`, `ml/*` (incl. the `_transformers`
`_sql_on` funnel; the one exception is `SQLTransformer`, whose user-written
statement runs through the user `sql` door per the VE3-1 fix, 2026-09-30).
Built SQL cannot carry registered-UDF calls (UDF markers
refuse composition), so the UDF rewrite keeps its user-door re-entry.

R2: `nested_assign.rs` `leaf_sql`, `Keyed.sql`, `repeated_insert_keys` render
values through `render_for_reparse`.

Oracles: `pins_probe.py` (evidence `reverify-esc-fold/`) measured the new pin
values on live Spark 4.1.2 in separate JVMs per mode (the JVM flag-inheritance
trap from the verify fold): built agg/unpivot/merge/overwrite answers are
mode-identical; the expr-condition merge discriminates (default hits
`it's`, verbatim hits `it''s`); DDL `COMMENT` unescapes both modes.

Replay: reverify-esc (`att2`, `att3`, `nest`) plus verify-esc (`attack`,
`rt`, `nb`, `dmlv`, `ns`, diff-probe `dp`-`dp4`) replayed into
`reverify-esc-fold/` against a true e99fa1eb base built in a throwaway
worktree (removed after; Spark oracles reused) — 1475 cells, 1349 fold-equal
to Spark, 19 moved to Spark, 0 true moved-away. The one flagged away cell
(`v_sidx`) is a canon artifact: Spark errored only on missing numpy in the
oracle env while the fold matches base-default exactly. The two
changed-still-wrong cells are the pre-existing `groupingSets` grand-total row
(fold values Spark-equal). Mutations red-first per fix (R1 override
follow-flag-again, R1 fragment identity, R2 `.to_string()`), tree clean after
each revert. Lane gate `gate.sh` green.

Follow-up (2026-09-30): the first cut routed native receivers
(`DataFrame._session` is the native `PyReparkSession`) through a free
`session_sql_built(session, query)`; the facade pins caught the missing method
(11 failed, 26 passed). The follow-up adds the `sql_built` pymethod on
`PyReparkSession` in `session.rs`, funded by moving `apply_session_knobs`
verbatim into `session_runtime.rs` (1122 → 1097, shrink-only ratchet; the
brief's new-file suggestion would have needed a `mod` line in `lib.rs`, which
sits at its 190 ceiling). Native receivers call `inner.sql_built(...)`;
facade receivers keep `_sql_built`. Same pins, same oracles, re-verified
post-fix.

Fix round (2026-09-30): the new pins caught two pre-existing gaps the brief's
cells did not cover. (1) `groupingSets` returns a grand-total row beyond Spark
(att3 base and head both show it); the pin asserts it with a Spark-equal value.
(2) `refuse_non_deterministic` re-rendered the REPLACE WHERE predicate with
`Display`, refusing `\'` values as unterminated in both modes; it now renders
through `render_for_reparse` (same VE-3-class fix, 3 lines).

Gate round (2026-09-30): the lane gate caught the two intended body changes
under the session-split AST-hash pin; the `_forward_datafusion_conf` and
`_materialize_values_as_memtable_frame` hashes move to the `_sql_built`
routing (same sanctioned pin update as the catalog-1 `resolve_table_name`
move).

### C-013/C-014 CI round (2026-09-30)

CI's full facade suite on 9d101b8c failed 8 tests the lane gate does not
run. All 8 reproduced in the lane venv as one pytest command over the 6
files, then fixed.

Seams (C-013): `test_catalog_surface_1::_SpyInner`,
`test_create_dataframe_materialize::_NativeRegisterProxy`,
`test_eager_own_1::_FailScanOnCacheView`, `test_mapinarrow::_SessionProxy`,
and `test_ml_boost_oracle::_SessionProxy` intercepted only the native
`sql` door, which the `_sql_built` routing no longer uses for
facade-built scans — so injections never fired and spies never recorded.
Each seam now observes the `sql_built` door with the same assertions.
The two capped files rename the seam method (line-neutral; the old door
still delegates correctly through `__getattr__`, only unobserved); the
other three observe both doors. No product change: every guarded cleanup
(`_materialize_exporter_as_memtable_frame`,
`_materialize_values_as_memtable_frame`, `bind_registered_view`, the
mapInArrow register/track/scan paths) still runs around the `sql_built`
call, and each test's own drop/track assertions confirm it.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: string-literal-escape-1
  complete: true
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-000..C-004 walked against behavior — the 229-literal Step-0 matrix on five doors under both modes against live PySpark 4.1.2, the post-fix rerun to zero unexplained diffs, the residuals ledgered with Spark values, the guards re-run to zero match-to-diff transitions, the mutation run red-first at both levels, and the gate run as written; every clause is PROVEN and cited from the maps.
      artifacts: [task/ledgers/staging/string-literal-escape-1-ledger.md, python/repark/tests/test_string_literal_escape_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries actually exercised — empty literals both quotes, quote runs of 2/3/4/6, lone and trailing escapes, NUL/control bytes, astral and lone surrogates, out-of-range \U, malformed \u, unterminated and unpaired-backslash refusals, 10k-char control; Arrow value AND type pinned per facade cell.
      artifacts: [python/repark/tests/test_string_literal_escape_1.py, crates/repark-spark/src/tests/string_literal_escape_1.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Failure modes preserved in kind — unterminated literals, unpaired trailing backslashes, and raw backslash-quote forms refuse on both engines before and after; error-to-value flips land only where Spark answers (double-raw, verbatim borrowed doubles); no new failure mode and no retry/cleanup surface (pure value function).
      artifacts: [crates/repark-spark/src/tests/string_literal_escape_1.rs, task/ledgers/staging/string-literal-escape-1-ledger.md]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state — the value engine is a pure function of (raw text, quote, verbatim flag); the flag arrives as a bool from session config and no ordering or reentrancy assumption exists.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no injection or deserialization surface — literal values already inside the engine, requoted through the existing exact round-trip.
    - id: AT-6
      status: ATTACKED
      evidence: Stored values move only toward Spark — the write read-back differs solely on the documented r'''' row; every value that matched Spark is byte-identical after (zero match-to-diff transitions, native door included); no migration or schema surface.
      artifacts: [python/repark/tests/test_string_literal_escape_1.py]
    - id: AT-7
      status: N/A
      justification: No resource or performance envelope change — the same single token pass with one value allocation per literal as before; nothing unbounded, nothing hot.
    - id: AT-8
      status: ATTACKED
      evidence: Dependency and size contracts honored — no sqlparser/fork or Cargo change; the downstream Generic behaviors relied upon (backslash-kept, own-quote doubling collapse) are measured, not presumed; the 1,000-line ceiling holds via a child-module split with lib.rs untouched and no ceiling raised; the byte-frozen SQP-1 leaf keeps its hash.
      artifacts: [crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/tests/string_literal_escape_1.rs]
    - id: AT-9
      status: N/A
      justification: No new failure path and no diagnostic change — remaining refusals keep their texts and locations, and error-to-value flips only remove errors Spark does not raise.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — old behavior fails 5/6 new Rust pins and 6/6 facade tests, and passes again on the byte-exact restore; every new branch names a flipping input (quote arms, raw head/tail/verbatim splits, verbatim rewrite rule); no dead branch ships.
      artifacts: [crates/repark-spark/src/tests/string_literal_escape_1.rs, python/repark/tests/test_string_literal_escape_1.py]
```
