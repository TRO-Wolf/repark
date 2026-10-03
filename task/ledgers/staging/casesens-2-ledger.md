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

## Slice 2 (2026-09-28, this round; branch `feat/casesens-2-s2`, base `5b730b0c`)

OD-3 adopted 2026-09-28 (renames fan out, folded `withColumns` keys refuse).
`written_names.rs` gains `refuse_folded_duplicate_keys` (Exact answers ok,
IgnoreCase raises Spark's 42711 text through the S3-tested error mapping),
re-exported through `frame_names` inside the existing `pub use` line
(`case_bind.rs` stays exactly 1000); `dataframe_names.rs` exposes the
`refuse_folded_duplicate_keys(frame, keys)` pyfunction (252 → 262).
`written_names.py` gains the S2 body (71 → 145): `_refuse_folded_with_columns_keys`,
`_match_with_columns_keys`, `_locate_rename_targets`, `_rewrite_running_names`;
`core.py` keeps the Column construction and the duplicate-name check and calls
them (26 insertions, 26 deletions — holds 3973, no ceiling or table edit, the
S1 A5 precedent). The red-first run on `5b730b0c` fails exactly the differing
legs (`cs_df_withColumn_ID`, `cs_df_renamed_ID`, `r26_wcs_dup`); the overlay
pin is green on arrival.

Adaptations recorded: A6 the Exact arm matches with Python `==` behind the
native `frame_is_exact` gate while the IgnoreCase arm calls native
`match_display_names` — R5's matcher raises (not Missing) under Exact, and
these sites append/no-op on a miss, so the raising entry cannot serve them;
the grep holds (zero new `casefold`/`lower`/`eq_ignore_ascii_case`). A7 the
plural duplicate-name check stays: a fanned-out twin collision
(`{"id":"z"}` on twins → `[z,z]`) raises under the disclosed EX-DF-18 rule
while the singular fans out per OD-3 — unmeasured (no p6 cell), no pin moves.
A8 the folded-key refusal runs before the window-layer merge attempt, so the
call's own keys are refused per Spark's rule; a folded cross-collision between
the prior layer and the new keys refuses in the combined recursive call (loud,
unmeasured, no pin). Exact-duplicate frames keep today's silent no-op on the
singular rename path (R-22, out of scope).

## Slice 3 (2026-09-28, this round; branch `feat/casesens-2-s3`, base `730530e1`)

OD-4 adopted 2026-09-28 (`na` subsets fold under `false`, the
`dropDuplicates` miss raises Spark's legacy text under both rules).
`written_names.rs` gains `match_subset_names` (fan-out under `IgnoreCase`,
exact hits under `Exact`, `unresolved_subset_name` on a miss under both
rules), re-exported through `frame_names` inside the existing `pub use` line
(`case_bind.rs` stays exactly 1000); `dataframe_names.rs` exposes the
`match_subset_names(frame, written, held)` pyfunction (262 → 276).
`written_names.py` gains the S3 body (145 → 171): `_match_lenient_subset`
(`na`: misses match nothing under `IgnoreCase`, refuse natively under
`Exact`) and `_match_subset_names` (`dropDuplicates`, above).
`actions_export.py` resolves `_columns_for_fill_value` and the `drop`
overlay branch through `_match_lenient_subset` (317 → 318: the import plus
the one resolve line, minus the exact-membership line); the `drop` plain
path keeps its S1 `_bind_schema_column` routing (already EQUAL).
`core.py` `drop_duplicates` resolves through `_match_subset_names`
(3973 → 3968; the EXCEPTIONS and CAP-1 rows ratchet to 3968 in this
change). The red-first run on `730530e1` fails exactly the differing legs
(`fill_null_false`, `fill_subset_nope_true`, `df_fillna_true`,
`dd_nope_true`, `dd_nope_false`, `df_dropDuplicates_true`, the two `na`
overlay legs); `fill_null_true`, `dropna_subset_false` (plain),
`dropna_subset_true`, `r7_dropDuplicates_ID` and `dd_exact_true` are green
on arrival. The probe re-run flips exactly the six oracle legs (p1 zero
flips); the scoreboard replay is identical to S2 modulo timing.

HALT-RULE-6 FIRED: the deletion premise is false. Repo-wide grep at
`730530e1` shows five code callers of `_resolve_getitem_column_name` in
four files — `drop_duplicates` (removed by this round), `declare_sorted`,
`colregex.col_regex_column`, `surface_a.withMetadata`, and
`replace_expr._resolve_subset_targets` — all four survivors present at the
design base `f2d3d220` and all outside S3's named sites, plus the
`tests/_dfcore_1_expected.py` export pin and the `dataframe/map.md` rows.
The S1/S2 ledger tracked `core.py` callers only. Q1 ruling (a),
2026-09-28: the helper stays, C-007 is amended as ruled, and residue
R-CS2-6 (ruled as R-CS2-3, recorded under the next free id since R-CS2-3
is the S1 `F.col` residue) names the four callers. No clause is OPEN, so
the attestation block below is filed.

Adaptations recorded: A9 the `na` subsets match through
`match_display_names` on both overlay and plain frames, not
`resolve_df_names` on plain as S3-1's parenthetical prescribes — subset
matching is display-spelled (targets match `display in …`), the single
call fans out twins where `resolve_df_names` returns no hits on
`Ambiguous`, and on plain frames displays equal schema names so outcomes
equal the prescribed split. A10 `dropDuplicates` on exact-duplicate
frames under `Exact` raises the legacy subset text (was
`AMBIGUOUS_REFERENCE`): the R-22 corner, unmeasured, no pin moves. A11 a
mixed hit-and-miss `dropDuplicates` subset on overlay frames now refuses
(was a silent skip when some display hit exactly): aligned with the plain
path, which always refused; unmeasured, no pin.

## Slice 4 (2026-09-28, this round; branch `feat/casesens-2-s4`, base `63acd9bb`)

Owner ruling 2026-09-28 ("Lets get the self join fixed"): residue R-CS2-1 is
fixed in v1.5.1, not re-homed. Q1 ruling (orchestrator, same day): EXTEND —
S4 lands both halves as one R4 mechanism (condition rewrite plus
qualified-display select routing, ≈110 product lines, size wire raised to
~130 for this slice only). Q2 ruling (same day): ACCEPT the non-join
refuse→answer flips on measurement — `cs_probe10.py` (9 cells, Spark 4.1.2
UTC banner, then RePark) pins every non-join shape; a Spark-refuses shape
the routing would answer narrows the routing to join children only.

S4-0 measured (2026-09-28): Spark answers `alias_dupe_sel` / `_fold` /
`_true` (multi-hit qualified binds), refuses `alias_wc_sel*` /
`alias_dupe_sel_first*` naming the qualifier (projected expressions lose
it) and `wrongqual` / `fold_true` with `UNRESOLVED_COLUMN.WITH_SUGGESTION`.
RePark's select/withColumn children carry unqualified native schemas (the
alias qualifier is lost, verified live), so schema-qualifier routing binds
only on join children (H1 `requalify_join_sides` keeps `l`/`r`) — the Q2
narrowing falls out of the mechanism, no Spark-refuses shape flips. The
S1 "string half binds" note in R-CS2-1 is corrected: qualified strings
refused on every overlay frame until S4; join children bind now, other
overlay children stay refusing (residue R-CS2-7).

Plan: p10 oracle keys plus red-first pins; `written_names.rs` gains
`rewrite_join_condition_aliases` (Databricks parse, two-part compounds
rebind through the side schemas, byte-identical passthrough otherwise) and
`resolve_qualified_display_names` (positional schema-qualifier plus
display pairing, first hit binds, Exact raises the qualified
`unresolved_column`); `dataframe_names.rs` exposes both;
`written_names.py` gains `_rewrite_join_condition` and
`_bind_qualified_display_column`; `core.py` swaps the six-line QCOL call
for the wrapper (shrinks; ceilings ratchet).

Landed 2026-09-28: `r7_selfjoin` and `r18_alias_join` answer Spark's
names and rows byte-equal; `selfjoin_true` keeps refusing Spark's text;
the wrong-case alias qualifier refuses 42703; the p10 true misses match
head plus candidates; the p10 false shapes keep the R4 facade text and
the three answer-shapes stay refuse-pinned (R-CS2-7). The probe re-run
flips exactly `r7_selfjoin` (p1) and `r18_alias_join` (p6); p4 zero
flips; the scoreboard replay is identical to S3 modulo timing. Red-first:
`test_s4_selfjoin_answers_as_spark`,
`test_s4_probe10_true_qualified_misses_match` and
`test_s4_probe10_true_gap_stays_pinned` fail on `63acd9bb`, the two
behavior-preserving pins pass on arrival.

Adaptations recorded: A12 the written qualifier builds
`TableReference::Bare` directly — `TableReference::from` lowercases
(`parse_str_normalized`), which bound wrong-case qualifiers under
`Exact` (caught by the new unit test, not the facade pin). A13 the
rewritten view qualifier is bare (`Ident::new`) with a backticked engine
field, the QCOL shape — a backticked view does not resolve the scratch
temp view (verified live). A14 the third `frame_names` re-export line is
funded by inlining one single-use test `let` (`case_bind.rs` stays
exactly 1000). A15 the multi-hit select binds the first positional hit —
unobservable in every S4 pin (multi-hit values are identical wherever
RePark binds); same-alias joins stay unmeasured. The condition half
binds single-hit only (twin conditions keep today's refusal,
unmeasured).

## Port onto the ATTR-ID-1 stack (2026-10-03, phase 1; branch `feat/attr-id-1-casesens-2`, base `e6810060`)

The four charter commits were cherry-picked onto the ATTR-ID-1 stack in order
(`5b730b0c` → `e4be1feb`, `730530e1` → `581f91b1`, `63acd9bb` → `28f6d28a`,
`4f927d2c` → `111e95b3`), conflicts resolved with the stack as the authority on
identity: every conflicting `core.py`, `actions_export.py`, `case_bind.rs` and
frozen-surface hunk kept the stack's side, the additive binding registrations
kept both, and auto-merged lines that reached the charter's mechanism from a
stack body were dropped with their hunk. The four fold commits (`0487afe7`,
`15a7bb9d`, `b89a7d7f`, `3ac80920`) were not replayed (OD-1).

Re-pointing (Claude commits on top of the picks):

- **Retired.** The charter's matchers (`written_names.py` whole;
  `written_names.rs` but `refuse_folded_duplicate_keys`; six bindings) are
  deleted. The stack's `resolve` and its S3a–S3e binds already answer every site
  they served; `test_casesens_2.py` proves it cell by cell.
- **Kept, re-pointed.** `refuse_folded_duplicate_keys` lowers keys with the Java
  `String.toLowerCase` table and reads the live session rule; the exact filter
  miss and the sort fall-through miss render `unresolved_column` (Spark's
  `UNRESOLVED_COLUMN`, every frame field suggested); the facade's unresolved
  refusals suggest every display (the stack listed fold hits only, the "bare
  suggestion lists" divergence the S3e record names).
- **Ported from the folds.** Of `predicate_names.rs`, the join-condition
  qualifier binder with its lambda scope stack (RC4-1, RC4-6, RC5-2), its
  matching re-pointed at `resolve` per side with the facade's frame
  qualifiers. The stack had no condition-join binding for a free
  `F.col("l.id")`, so `p1/r7_selfjoin`, `p6/r18_alias_join`, every alias join
  in the #881 diff-probe file and the h2 compound alias join (R-18) refused.
  The filter-string half was already on the stack (`filter_quote.py`, S3b
  H-1, whose map entry names the same RC4-1/RC4-6/RC5-2 port), so it is not
  ported again.
- **Dropped from the folds.** The origin dedupe, `is_bare_bind`, the
  alias-source tracking (`__repark_sel_a_…` engine names), `sort_names.rs`'s
  twin logic and the twin-identity helpers: attribute ids and `resolve` cover
  them (the stack's `sort_names.rs` is S3b's own, not #881's).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Under `caseSensitive=true` the bare-name legs refuse `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) naming the written spelling: `p1/cs_df_select_ID`, `cs_df_select_col_ID`, `cs_df_select_data`, `cs_df_orderBy_ID`, `cs_df_groupBy_ID`, `cs_df_filter_str_ID`, `cs_df_getitem_ID`. | Facade replay asserts class, condition, SQLSTATE, head and candidate set per key. | PROVEN | `test_casesens_2.py::test_s1_true_door_refuses_bare_names` (7/7 green); M2 reds it. |
| C-002 | Under `caseSensitive=true` the name APIs follow the rule: `withColumn` appends (`p1/cs_df_withColumn_ID`), `withColumnRenamed` no-ops (`p1/cs_df_renamed_ID`), `fillna` subset refuses (`p4/df_fillna_true`), `dropDuplicates` refuses the legacy text (`p4/df_dropDuplicates_true`); the p6 miss, twin and qualified cells (`sel_nope_true`, `getitem_nope_true`, `order_nope_true`, `group_nope_true`, `filter_str_nope_true`, `wcs_true`, `wcs_true_exact`, `wcrn_plural_true`, `wcr_twin_true`, `wc_twin_true`, `fill_null_true`, `fill_subset_nope_true`, `dropna_subset_true`, `dd_exact_true`, `dd_nope_true`, `qs_sel_t_id_true`, `qs_sel_t_ID_exact_true`, `selfjoin_true`) answer Spark's recorded cells. | Slice 3. | PROVEN | `test_casesens_2.py::test_s3_fillna_follows_the_rule` (`p4/df_fillna_true`, `fill_null_false`, `fill_null_true`, `fill_subset_nope_true`), `test_s3_dropna_follows_the_rule` (`dropna_subset_false`, `dropna_subset_true`, the overlay legs), `test_s3_drop_duplicates_follows_the_rule` (`p4/df_dropDuplicates_true`, `r7_dropDuplicates_ID`, `dd_exact_true`, `dd_nope_true`, `dd_nope_false`, legacy legs type plus message per R8); M6 reds the legacy legs. |
| C-003 | Under `caseSensitive=false` qualified df-door strings bind: `p1/r7_selfjoin` answers `ID`, `data` `[[1,a],[2,b]]`, and p6 `qs_sel_t_id`, `qs_sel_t_ID`, `qs_alias_sel` answer Spark's recorded names and rows; `r18_alias_join` is pinned if it falls out, else a dated residue. | Facade replay asserts names and rows per key. | PROVEN | S1 legs in `test_s1_qualified_strings_bind` (M3 reds); S4 adds `r7_selfjoin` and `r18_alias_join` in `test_s4_selfjoin_answers_as_spark` (M8/M9 red; both byte-equal Spark) per the 2026-09-28 owner ruling fixing R-CS2-1 in v1.5.1. |
| C-004 | Under `caseSensitive=false` `withColumn` replaces and renames fan out: `p1/r7_withColumn_ID`, `r7_withColumnRenamed_ID`, p6 `r20_wcr_twin`, `r20_wc_twin` answer Spark's recorded frames, and p6 `r26_wcs_dup` refuses `[COLUMN_ALREADY_EXISTS]` 42711. | Facade replay asserts names, rows and the 42711 refusal per key. | PROVEN | `test_casesens_2.py::test_s2_withcolumn_follows_the_rule`, `test_s2_renamed_follows_the_rule`, `test_s2_folded_keys_refuse` (12/12 legs green); M4 reds the r20 legs, M5 reds the r26 leg. |
| C-005 | Under `caseSensitive=false` `na` subsets match ignoring case over nulls (p6 `fill_null_false`, `dropna_subset_false`) and the `dropDuplicates` miss raises Spark's legacy text (p6 `dd_nope_false`). | Slice 3. | PROVEN | `test_s3_fillna_follows_the_rule` (`fill_null_false`), `test_s3_dropna_follows_the_rule` (`dropna_subset_false` plain plus the overlay leg), `test_s3_drop_duplicates_follows_the_rule` (`dd_nope_false`, type plus message per R8). |
| C-006 | The `false` path is otherwise byte-identical: `p1/r7_orderBy_ID`, `r7_groupBy_DATA`, `r7_dropDuplicates_ID`, `r7_fillna_subset`, `r7_sort_col_ID`, the S3 `df_*_false` legs, today's facade miss/ambiguous texts, the quoter battery and R-19's lazy timing all guard-pinned. | Guard pins plus the facade sweep. | PROVEN | S1 partial: `test_s1_false_door_byte_identical` green (r7 guards, S3 false legs, miss text, R-19 timing, quoter spot); M7 reds it. S2 partial: the S1 guard pins stay green and `test_s2_overlay_replace_unchanged` pins the R7 overlay replace set. S3: PROVEN — the S1/S2 pins stay green, the 281-test facade sweep passes, and the probe re-run flips exactly the six S3 legs (p1 zero flips). |
| C-007 | One rule: every site this unit touches matches through `repark_common::names::NameRule`; no `casefold` / `lower` / `eq_ignore_ascii_case` name comparison is added (grep of the unit's diff); `core.py` shrinks in every slice (3973 → 3968 at S3); `_resolve_getitem_column_name` stays until its four remaining callers are migrated. | Grep of the unit diff; the S3 ruling keeps the matcher. | PROVEN | S1 partial: the S1 diff adds zero `casefold`/`lower`/`eq_ignore_ascii_case` (grep verified); `_resolve_getitem_column_name` keeps its S2/S3 callers (`declare_sorted`, `drop_duplicates`, `with_column_renamed` — note `declare_sorted` is outside S2/S3's named sites, S3 halt-rule-6 input). `core.py` stays 3973 in S1 (written-path body moved out per the Q2 ruling). S2 partial: the S2 diff adds zero `casefold`/`lower`/`eq_ignore_ascii_case` (grep verified); the `with_column_renamed` caller is gone, remaining callers are `declare_sorted` and `drop_duplicates`; `core.py` holds 3973 (26/26, call sites plus docstrings fund the folded loops). S3 partial: the S3 diff adds zero `casefold`/`lower`/`eq_ignore_ascii_case` and removes two `casefold` uses (grep verified); the `drop_duplicates` caller is gone and `core.py` ends 3968 (ceilings ratcheted). Halt-rule-6 FIRED: four non-S3 callers survive (`declare_sorted`, `col_regex_column`, `withMetadata`, `na.replace`'s `_resolve_subset_targets`, all present at `f2d3d220`) plus the export pin. Q1 ruling (a), 2026-09-28: the helper stays, this clause is amended as ruled, residue R-CS2-6 names the four callers — amended clause PROVEN. |
| C-008 | Nothing regresses: the U11-EDGE-1 V-001 … V-004 pins, the `case_bind` and `column_resolution` batteries, the S3 `true` legs, the U8 C-033 keys and the ANSI door stay green; cells `E-CASE-SELECT`, `E-CASE-ALTER`, `E-CASE-INSERT-BY-NAME`, `E-CASE-MERGE`, `E-CASE-PARTITION-FIELD`, `E-CASE-TABLE-NAME`, `R-MT-CASE`, `P-CALL-UPPERCASE` replay unchanged. | Full lib sweeps, the facade sweep, the probe re-run and the scoreboard replay. | PROVEN | S1 partial: the WO gate batteries green (evidence in the S1 hand-back); zero existing pins changed. S2 partial: the WO gate batteries green (evidence in the S2 hand-back); zero existing pins changed. S3: PROVEN — the WO batteries green (evidence in the S3 hand-back); zero existing pins changed; the 8-cell scoreboard replay is identical to S2 modulo timing. |
| C-009 | A free alias-qualified name in a join condition (`F.col("l.id") == F.col("r.id")` over `alias("l")`/`alias("r")`) binds through `resolve` on the one side whose facade qualifier matches under the live rule; a lambda parameter shadows a qualifier only inside its own body; two ids under one qualifier refuse `AMBIGUOUS_REFERENCE`; any other dotted text reaches the engine unchanged. | Rust pins on the preparer plus the facade replay. | PROVEN | `../../../crates/repark-core/src/session/tests/join_qualifiers.rs` (6 pins); `test_casesens_2.py::test_s4_selfjoin_answers_as_spark` (`p1/r7_selfjoin`, `p6/r18_alias_join` byte-equal Spark, re-measured live 2026-10-03), `test_s4_wrongcase_alias_qualifier_refuses_under_true`; `test_h2_group_h2.py::test_h2_compound_alias_free_names_answer` (strict xfail removed). |
| C-010 | The port keeps the charter's behaviour through the stack: the charter's matchers retire without a cell moving; `withColumns` folded keys refuse `COLUMN_ALREADY_EXISTS` (42711, Java-lowered key) under the live `false` rule; unresolved refusals on select, getitem, groupBy, `fillna`/`dropna` subsets, string filters and sort keys suggest every frame display. | `test_casesens_2.py` in full plus the `test_attr_id_1_*` files; Rust `case_bind` pin. | PROVEN | `test_casesens_2.py` 16/16, `test_attr_id_1_*` 641 + 3 xfailed; `../../../crates/repark-core/src/session/tests/case_bind.rs::folded_with_columns_keys_refuse_only_under_ignore_case`. |
| C-011 | Where a charter pin and an ATTR-ID-1 pin disagree, the cell is measured on live Spark 4.1.2 and the measured answer is kept. | Live probes `cs2-port/spark/probe1.py`..`probe3.py` (2026-10-03). | PROVEN | `withColumns({"v", "V"})` under `false`: Spark refuses `COLUMN_ALREADY_EXISTS` `` `v` ``, so the S3c "last key wins" pin flips (`test_attr_id_1_s3c.py::test_with_columns_folded_keys_refuse_insensitive`). The `false` `select("nope")` refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` (`test_s1_false_door_byte_identical`); the overlay `dropna(subset=["id"])` and twin `fillna(subset=["x"])` refuse `AMBIGUOUS_REFERENCE` (`test_s3_dropna_follows_the_rule`); the nine p10 cells re-measure byte-equal to the oracle and replay in full (`test_s4_probe10_false_shapes_answer_as_spark`, `test_s4_probe10_true_multi_hit_answers_as_spark`). |

## Mutation record (2026-09-28, S1)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M2 | `frame_is_exact` returns `false` always. | `test_s1_true_door_refuses_bare_names` red (legs answer); `test_s1_false_door_byte_identical` stays green; restored green. |
| M3 | Qualifier split removed (whole-string match only). | `test_s1_qualified_strings_bind` red (R-11 legs miss); restored green. |
| M7 | Exact-membership fast path removed. | `test_s1_false_door_byte_identical` red (twin select goes eager-legacy); restored green. |

Not run in S1: M1 (`resolve_df_names` returns `Missing` for a folded hit —
needs a Rust rebuild cycle; M2/M3 cover the same pins), M4–M6 (S2/S3 scope).

## Mutation record (2026-09-28, S2)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M4 | `match_display_names` hits truncated to one under `IgnoreCase`. | `test_s2_withcolumn_follows_the_rule` red (`p6/r20_wc_twin`) and `test_s2_renamed_follows_the_rule` red (`p6/r20_wcr_twin`); restored green. |
| M5 | `folded_duplicate` keys not refused (the `with_columns` refuse call removed). | `test_s2_folded_keys_refuse` red (`p6/r26_wcs_dup` answers); restored green. |

Not run in S2: M1 (S1 rationale stands), M6 (S3 scope).

## Mutation record (2026-09-28, S3)

The S3 break ran red and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M6 | `unresolved_subset_name` not raised (the `_match_subset_names` native call swallowed). | `test_s3_drop_duplicates_follows_the_rule` red (misses answer); restored green. |

Not run in S3: M1 (S1 rationale stands).

## Mutation record (2026-09-28, S4)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M8 | The alias rewrite call removed from `_rewrite_join_condition` (QCOL text passes through). | `test_s4_selfjoin_answers_as_spark` red (`r7_selfjoin`, `r18_alias_join` refuse); restored green. |
| M9 | `_bind_qualified_display_column` returns `None` always. | `test_s4_selfjoin_answers_as_spark` red (select half misses), `test_s4_probe10_true_qualified_misses_match` red (whole-string head), `test_s4_probe10_true_gap_stays_pinned` red; restored green. |
| M10 | The rewrite rule forced `IgnoreCase` (native rebuild). | `test_s4_wrongcase_alias_qualifier_refuses_under_true` red (answers); `selfjoin_true` stays green via the select-half refusal; restored green. |

## Tests rewritten

None in slice 1: every existing pin keeps its answer (sweep evidence in the S1 hand-back).

None in slice 2: every existing pin keeps its answer (sweep evidence in the S2 hand-back).

None in slice 3: every existing pin keeps its answer (281-test sweep, probe re-run flips exactly the six S3 legs).

None in slice 4: every existing pin keeps its answer (378-test sweep, probe re-run flips exactly `r7_selfjoin` and `r18_alias_join`, scoreboard replay identical to S3 modulo timing).

Port (2026-10-03), each to a live Spark 4.1.2 measurement (C-011): `test_s1_false_door_byte_identical` (the `false` miss), `test_s3_dropna_follows_the_rule` (the two overlay legs), `test_s4_probe10_false_shapes_refuse` → `test_s4_probe10_false_shapes_answer_as_spark`, `test_s4_probe10_true_gap_stays_pinned` → `test_s4_probe10_true_multi_hit_answers_as_spark`; outside this file, `test_attr_id_1_s3c.py::test_with_columns_folded_keys_last_key_wins_insensitive` → `test_with_columns_folded_keys_refuse_insensitive` and the h2 compound alias join's strict xfail (removed).

## Residues

| # | Residue |
|---|---|
| R-CS2-1 | **CLOSED 2026-09-28** (S4, owner ruling "Lets get the self join fixed"): `p6/r18_alias_join` and `p1/r7_selfjoin` answer Spark's names and rows byte-equal (`test_s4_selfjoin_answers_as_spark`). The S1 note erred: the string half refused on overlay frames too — both halves bind now through one R4 mechanism. |
| R-CS2-2 | **OPEN 2026-09-28** (S1): toggle-reuse staleness — a frame built under `false` and reused under `true` (`p6/qs_sel_t_id_true`: Q answers `id` where Spark refuses naming `` `t`.`id` ``) because `frame_rule` reads the frame's captured `task_ctx`. Re-homed per the Q3 ruling (2026-09-28); the fresh-frame `true` legs stay pinned. |
| R-CS2-3 | **OPEN 2026-09-28** (S1): `F.col("nope")` under `true` keeps the engine's raw miss text (R2 returns the input unchanged, and the native binder passes total misses through) — no p6 cell covers it; UNRESOLVED-TEXT owns suggestion texts. |
| R-CS2-4 | **CLOSED 2026-09-28** (S1): superseded by the Q2 ruling — the written-path body moved to `written_names.py`, `core.py` stays 3973, no ceiling edit. |
| R-CS2-5 | **OPEN 2026-09-28** (S3): `fillna` over mixed-type duplicate-display frames refuses with the engine cast error (observed: `[ID, ID]` overlay over the null table, subset matching both, `Cannot cast string 'z' to Int32`). Pre-existing mechanism — `_fill_scalar` matches by display spelling, untouched by this unit — reached by more spellings once subsets fold. R-22 corner; the origin/attribute follow-up owns exact-duplicate frames. |
| R-CS2-6 | **OPEN 2026-09-28** (S3, Q1 ruling (a)): `_resolve_getitem_column_name` keeps four callers outside this unit's named sites — `core.py` `declare_sorted`, `colregex.col_regex_column`, `surface_a.withMetadata`, `replace_expr._resolve_subset_targets` (the `na.replace` subset) — whose Exact-mode behaviour is unmeasured, so the helper stays until they migrate. Homed to "CASESENS follow-up: remaining Python-matched name sites (probe first)". |
| R-CS2-7 | **CLOSED 2026-10-03** (port onto ATTR-ID-1): the stack keeps the alias qualifier on select/withColumn children, so `p10/alias_dupe_sel`, `_fold` and `_true` answer Spark's names and rows and the p10 refusals carry Spark's head and candidates (`test_s4_probe10_false_shapes_answer_as_spark`, `test_s4_probe10_true_multi_hit_answers_as_spark`). |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: casesens-2
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All 8 clauses walked one by one against behavior — the p6 oracle measured on live PySpark 4.1.2 + Iceberg 1.11.0 plus the recorded p1/p4 legs, hermetic Rust pins plus the facade replay per clause, mutations M2-M10 red-then-green (M1 declined with rationale, covered by M2/M3; S4 adds C-003 full via the p10-measured self-join halves, M8-M10).
      artifacts: [task/ledgers/staging/casesens-2-ledger.md, python/repark/tests/test_casesens_2.py, crates/repark-core/src/session/df_guards/written_names.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — twin spellings id/ID, exact duplicates, qualified strings (last-dot split, dotted whole-name first), both caseSensitive settings per site, empty and mixed subsets, unknown columns, overlay and plain frames, null-bearing tables.
      artifacts: [python/repark/tests/test_casesens_2.py, crates/repark-core/src/session/df_guards/written_names.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal raises Spark's measured text (byte-exact, or head plus candidate set per R12, or type plus message per R8); name resolution is plan-time only, so no refusal or answer persists state.
      artifacts: [python/repark/tests/test_casesens_2.py]
    - id: AT-4
      status: ATTACKED
      evidence: No shared or global state touched — the rule reads from the frame's own task_ctx per call (Python never reads the flag), each pin runs in its own session and warehouse.
      artifacts: [crates/repark-python/src/dataframe_names.rs, python/repark/tests/test_casesens_2.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no injection or deserialization surface — name matching over closed schema and display-name lists.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per leg (dtypes plus rows); divergences recorded as dated residues R-CS2-2, R-CS2-3, R-CS2-5…R-CS2-7 with homes, never absorbed (S4 closes R-CS2-1, opens R-CS2-7).
      artifacts: [python/repark/tests/test_casesens_2.py, task/ledgers/staging/casesens-2-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Planning-only changes — no row path, no hot loop; the facade sweep, the three probes and the scoreboard replay ran in normal time; file-size baselines ratcheted DOWN only (core.py 3973 -> 3968 -> 3963; case_bind.rs held 1000).
      artifacts: [scripts/check_lib_py.py, scripts/check_rust_file_size.py]
    - id: AT-8
      status: ATTACKED
      evidence: The session rule reaches every touched site through frame_rule (M2); the crate DAG is unchanged (re-exports inside existing pub use lines, S4's third line funded in-file, no new edge); ceilings ratcheted DOWN only with map.md lockstep in every touched directory.
      artifacts: [crates/repark-core/src/session/df_guards/case_bind.rs, scripts/check_lib_py.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every changed refusal text is pinned on the door that raises it (byte-exact, R12 head plus candidate set, or R8 type plus message); the engine planning prefix is stripped by the shared _plain_message helper and the remainder asserted, not stripped silently.
      artifacts: [python/repark/tests/test_casesens_2.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — every new pin fails pre-change (S1/S2/S3 red runs recorded in the slice hand-backs; S4 red run recorded in the Slice 4 section) and the pre-change answers are recorded in the after-sN probe diffs; no dead branch ships (each new arm has a named pin).
      artifacts: [task/ledgers/staging/casesens-2-ledger.md, target/casesens-2/after-s4/p6-repark.json]
```
