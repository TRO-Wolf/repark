# Unit ledger — WO CASESENS-2 · the DataFrame door resolves names in Rust

**Date:** 2026-09-28 · **Branch:** `feat/casesens-2-s1` · **Base:** `167d07a9`
(the CASESENS-1 verifier fold) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** GUIDED. **risk_tier: high.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** CASESENS-1 S3 proved the rule-aware native side but could not reach
the Python pre-binding that folds the written spelling before Rust sees it, so
under `caseSensitive=true` the DataFrame door answers where Spark refuses
(R-CS1-10, six legs plus `df["ID"]`, `withColumn`, renames, `fillna`,
`dropDuplicates`), and under `false` qualified strings refuse where Spark binds
(`r7_selfjoin`, R-11). This unit moves every touched name match into
`repark_common::names::NameRule` in Rust in three slices.

**What Spark does (measured 2026-09-28; Spark 4.1.2 + Iceberg 1.11.0, probe
`cs_probe6.py`, `p6-spark.json`, 29 cells, 7 EQUAL, 22 DIFF; oracle
`casesens_2_spark_oracle.json`, 34 steps; RePark baselines `p1-repark.json`,
`p4-repark.json`, `p6-repark.json`, same tree).** Under `true` every bare name
API refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) naming the written
spelling while exact spellings answer; `withColumn` appends, renames no-op,
`fillna` subsets refuse and `dropDuplicates` refuses the legacy text. Under
`false` qualified strings bind and name the output with the written last
segment, renames fan out to twins, folded `withColumns` keys refuse
`COLUMN_ALREADY_EXISTS` (42711), and `na` subsets match ignoring case over
nulls. The qualified-under-`true` pair reproduces only when Q is built once
under `false` and reused under `true` (S1-0 method note, pinned by the S1 test).

## Slice 1 (2026-09-28, this round; Q1–Q3 ruled the same day)

S1-0 was recorded before this round (hand-back
`casesens-2-s1-0-handback.json`): 29/29 p6 cells, every OD premise CONFIRMED,
zero contradictions. This round copies `cs_probe6.py` and `p6-spark.json` into
the committed oracle `python/repark/tests/casesens_2_spark_oracle.json` (new
file, spark payloads byte-equal to `p6-spark.json`).

S1-1 new `crates/repark-core/src/session/df_guards/written_names.rs`
(`resolve_df_names`, `match_display_names`, `unresolved_subset_name`,
`Disposition::{Bound, Ambiguous, Missing}`, three inline unit tests) wired
through `frame_names` (2 `pub use` lines in `case_bind.rs`, now exactly 1000;
`unresolved_column` widened to `pub(super)` line-neutrally).
`dataframe_names.rs` exposes `resolve_df_names`, `match_display_names` and
`frame_is_exact` (194 → 252); `frame_case_sensitive` delegates to
`frame_is_exact` (adaptation A1: the WO's new gate already existed as VC-4's
probe). `expr_build.rs` routes a total-miss filter field under `Exact` through
`resolve_df_names` so `filter_str_nope_true` refuses Spark's text instead of
the raw engine text (adaptation A2; without it C-002's filter leg is
unreachable in any slice).

S1-2 `core.py` per R2/R6: `_bind_schema_column` resolves the written path
through R5 (exact-or-raise under `Exact`, match plus legacy rendering under
`IgnoreCase`, qualified strings ride the proven `F.col` path),
`_rebind_stable_name_column` returns its input under `Exact`, and `filter(str)`
skips the quoter under `Exact`. `__getitem__` and `_column_of` delegate
unchanged. S1-3 holds: `select(F.col("ID"))` under `true` refuses through
`bind_projection_expr` (verified live, plus the aliased-`F.col` probe on the
base tree).

Adaptations recorded: A3 R3's exact-membership fast path lives in
`_bind_schema_column`, not in `__getitem__`'s body — C-023 pins `select("id")`
lazy on twins, so every string site must share getitem's laziness; the shape is
the specified 2-line `in` check plus the duplicate guard the `joined["b"]` pin
requires. A4 the qualifier comparison lives in `written_names.rs`
(`qualifier_matches`, same semantics as `same_relation`) because widening
`same_relation` costs 4 reformatted lines the 1000-line ceiling cannot spare.
A5 per the Q2 ruling (REDIRECT, no ceiling change) the written-path body leaves
`core.py` for the new sibling `written_names.py` as the private frame-first
`_bind_written_column` (DFCORE-3 shape): `_bind_schema_column` keeps the engine
branch plus a one-line delegation, `core` binds the module (both export tables
gain exactly `written_names`), and `core.py` stays 3973 — no ceiling or table
edit. The engine branch reuses the top-level `_quote_ident_sql` alias instead
of its old method-local import (same function, 2 lines saved).

Baseline vs inferred (S1-0 wins everywhere): the `r7_selfjoin` refusal comes
from the join condition (native `UNRESOLVED` naming `` `l`.`ID` ``), not from
the select strings as the gap table inferred (re-homed per the Q1 ruling). `filter_str_nope_true`
refuses with the raw engine text on the base tree, not the inferred facade
text. `r7_withColumn_ID`, `r20_wc_twin` and every `r7_*` guard leg match the
table's prediction (halt rule 2 satisfied).

Rulings (2026-09-28, same day). Q1 (C-003 `r7_selfjoin`): RE-HOME to the
join-origin follow-up — the condition `F.col("l.ID") == F.col("r.ID")` refuses
`UNRESOLVED` naming `` `l`.`ID` `` before any select runs (verified live;
`_join_on_condition_h1` rewrites only QCOL tokens), and the condition rewrite
is out of scope by OD-1. Dated residue R-CS2-1 below carries both answers; C-003
is PROVEN-partial on the legs it proves. Q2 (`core.py` size): REDIRECT with no
ceiling change — the written-path body moved to `written_names.py` (A5) and
`core.py` stays 3973. Q3 (`qs_sel_t_id_true`): RE-HOME as dated residue R-CS2-2
— the frame captures its rule at construction, so a false-built Q reused under
`true` still reads `IgnoreCase`; the forbidden files and the facade conf read
stay untouched, and the fresh-frame `true` legs stay pinned.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Under `caseSensitive=true` the bare-name legs refuse `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) naming the written spelling: `p1/cs_df_select_ID`, `cs_df_select_col_ID`, `cs_df_select_data`, `cs_df_orderBy_ID`, `cs_df_groupBy_ID`, `cs_df_filter_str_ID`, `cs_df_getitem_ID`. | Facade replay asserts class, condition, SQLSTATE, head and candidate set per key. | PROVEN | `test_casesens_2.py::test_s1_true_door_refuses_bare_names` (7/7 green); M2 reds it. |
| C-002 | Under `caseSensitive=true` the name APIs follow the rule: `withColumn` appends (`p1/cs_df_withColumn_ID`), `withColumnRenamed` no-ops (`p1/cs_df_renamed_ID`), `fillna` subset refuses (`p4/df_fillna_true`), `dropDuplicates` refuses the legacy text (`p4/df_dropDuplicates_true`); the p6 miss, twin and qualified cells (`sel_nope_true`, `getitem_nope_true`, `order_nope_true`, `group_nope_true`, `filter_str_nope_true`, `wcs_true`, `wcs_true_exact`, `wcrn_plural_true`, `wcr_twin_true`, `wc_twin_true`, `fill_null_true`, `fill_subset_nope_true`, `dropna_subset_true`, `dd_exact_true`, `dd_nope_true`, `qs_sel_t_id_true`, `qs_sel_t_ID_exact_true`, `selfjoin_true`) answer Spark's recorded cells. | Slice 3. | OPEN | S1 partial: the five miss legs pin green (`test_s1_true_misses_refuse`); `qs_sel_t_id_true` is re-homed per the Q3 ruling (R-CS2-2); the twin/`na`/`dropDuplicates` legs are S2/S3. |
| C-003 | Under `caseSensitive=false` qualified df-door strings bind: `p1/r7_selfjoin` answers `ID`, `data` `[[1,a],[2,b]]`, and p6 `qs_sel_t_id`, `qs_sel_t_ID`, `qs_alias_sel` answer Spark's recorded names and rows; `r18_alias_join` is pinned if it falls out, else a dated residue. | Facade replay asserts names and rows per key. | PROVEN (partial: `qs_sel_t_id`, `qs_sel_t_ID`, `qs_alias_sel`, `qs_sel_t_ID_exact_true`, `selfjoin_true` pin green in `test_s1_qualified_strings_bind`; M3 reds it) | `r7_selfjoin` re-homed to the join-origin follow-up per the Q1 ruling (R-CS2-1, both answers recorded, not pinned). `r18_alias_join` is dated residue R-CS2-1 (condition refuses naming `` `l`.`id` ``, both answers recorded). |
| C-004 | Under `caseSensitive=false` `withColumn` replaces and renames fan out: `p1/r7_withColumn_ID`, `r7_withColumnRenamed_ID`, p6 `r20_wcr_twin`, `r20_wc_twin` answer Spark's recorded frames, and p6 `r26_wcs_dup` refuses `[COLUMN_ALREADY_EXISTS]` 42711. | Slice 2. | OPEN | |
| C-005 | Under `caseSensitive=false` `na` subsets match ignoring case over nulls (p6 `fill_null_false`, `dropna_subset_false`) and the `dropDuplicates` miss raises Spark's legacy text (p6 `dd_nope_false`). | Slice 3. | OPEN | |
| C-006 | The `false` path is otherwise byte-identical: `p1/r7_orderBy_ID`, `r7_groupBy_DATA`, `r7_dropDuplicates_ID`, `r7_fillna_subset`, `r7_sort_col_ID`, the S3 `df_*_false` legs, today's facade miss/ambiguous texts, the quoter battery and R-19's lazy timing all guard-pinned. | Guard pins plus the facade sweep. | OPEN | S1 partial: `test_s1_false_door_byte_identical` green (r7 guards, S3 false legs, miss text, R-19 timing, quoter spot); M7 reds it. Flips in S3. |
| C-007 | One rule: every site this unit touches matches through `repark_common::names::NameRule`; no `casefold` / `lower` / `eq_ignore_ascii_case` name comparison is added (grep of the unit's diff); `core.py` shrinks in every slice and `_resolve_getitem_column_name` is deleted. | Grep of the unit diff; S3 deletes the matcher. | OPEN | S1 partial: the S1 diff adds zero `casefold`/`lower`/`eq_ignore_ascii_case` (grep verified); `_resolve_getitem_column_name` keeps its S2/S3 callers (`declare_sorted`, `drop_duplicates`, `with_column_renamed` — note `declare_sorted` is outside S2/S3's named sites, S3 halt-rule-6 input). `core.py` stays 3973 in S1 (written-path body moved out per the Q2 ruling); flips in S3. |
| C-008 | Nothing regresses: the U11-EDGE-1 V-001 … V-004 pins, the `case_bind` and `column_resolution` batteries, the S3 `true` legs, the U8 C-033 keys and the ANSI door stay green; cells `E-CASE-SELECT`, `E-CASE-ALTER`, `E-CASE-INSERT-BY-NAME`, `E-CASE-MERGE`, `E-CASE-PARTITION-FIELD`, `E-CASE-TABLE-NAME`, `R-MT-CASE`, `P-CALL-UPPERCASE` replay unchanged. | Full lib sweeps, the facade sweep, the probe re-run and the scoreboard replay. | OPEN | S1 partial: the WO gate batteries green (evidence in the S1 hand-back); zero existing pins changed. Flips in S3. |

## Mutation record (2026-09-28, S1)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M2 | `frame_is_exact` returns `false` always. | `test_s1_true_door_refuses_bare_names` red (legs answer); `test_s1_false_door_byte_identical` stays green; restored green. |
| M3 | Qualifier split removed (whole-string match only). | `test_s1_qualified_strings_bind` red (R-11 legs miss); restored green. |
| M7 | Exact-membership fast path removed. | `test_s1_false_door_byte_identical` red (twin select goes eager-legacy); restored green. |

Not run in S1: M1 (`resolve_df_names` returns `Missing` for a folded hit —
needs a Rust rebuild cycle; M2/M3 cover the same pins), M4–M6 (S2/S3 scope).

## Tests rewritten

None in slice 1: every existing pin keeps its answer (sweep evidence in the S1 hand-back).

## Residues

| # | Residue |
|---|---|
| R-CS2-1 | **OPEN 2026-09-28** (S1): `p6/r18_alias_join` — the aliased-join condition `F.col("l.id") == F.col("r.id")` refuses `UNRESOLVED_COLUMN` naming `` `l`.`id` `` before `select("l.id")` runs (RePark) where Spark answers `id` `[[1],[2]]`. The string half binds through S1's qualified routing; the condition half belongs to the join-origin follow-up with R-CS1-10's condition shapes. |
| R-CS2-2 | **OPEN 2026-09-28** (S1): toggle-reuse staleness — a frame built under `false` and reused under `true` (`p6/qs_sel_t_id_true`: Q answers `id` where Spark refuses naming `` `t`.`id` ``) because `frame_rule` reads the frame's captured `task_ctx`. Re-homed per the Q3 ruling (2026-09-28); the fresh-frame `true` legs stay pinned. |
| R-CS2-3 | **OPEN 2026-09-28** (S1): `F.col("nope")` under `true` keeps the engine's raw miss text (R2 returns the input unchanged, and the native binder passes total misses through) — no p6 cell covers it; UNRESOLVED-TEXT owns suggestion texts. |
| R-CS2-4 | **CLOSED 2026-09-28** (S1): superseded by the Q2 ruling — the written-path body moved to `written_names.py`, `core.py` stays 3973, no ceiling edit. |
