# Charter ledger — SQL-LITERAL-TYPING-1 (BL-20) · Spark integral literal typing on the SQL door

**Date:** 2026-09-16 · **Branch:** `feat/sql-literal-typing-1` · **Base:** `origin/main`
`33c87cbf` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The SQL door types an unsuffixed integral literal as BIGINT (Arrow
Int64) while Spark types it INT when the value fits, BIGINT when it needs 64
bits, and DECIMAL(p,0) past that. Every promotion that meets the literal widens
(`1 + CAST(1 AS TINYINT)` is bigint on the SQL door, int on the Python door and
in Spark), so the two repark doors disagree. Owner ruling Q-17c-2 charters this
as a high 1.5 card.

**Not in this unit:** the LOGICAL-WIDTH-1 `df.schema` tinyint/smallint display
(run 18b owns `type_table.rs::arrow_name_at_depth` and `dataframe.rs`); `div`
and unary `~` (SPARK-SQL-GRAMMAR-1 C-001/C-002 residues); `functions*.py`,
`dataframe/**`, `column.py`, `catalog.py`, `types.py` (runs 18a/18b).

## Rulings recorded at open

- Q-17c-2 (owner, 2026-09-16): this card exists and is high priority.
- Q-17a-2 (owner, 2026-09-16): a decision that raises, casts, coerces or
  branches on a value is a kernel too. Python holds names, argument shapes and
  API plumbing only. The literal typing lands in Rust.
- Q-15c-6 (owner): no registry-wide wrapper that re-binds every UDF's return
  field. The fix is at the literal, never a per-operator result retag.
- R-18c-1 (this lane, 2026-09-16): the oracle fixtures beside the brief are the
  spec. `2.5D` / `1L` spellings are not used in pins (FNP-4B is not on main);
  the same shapes are reached with `CAST` spellings already in the cells.
- R-18c-2 (this lane, 2026-09-16): out-of-scope DIFF lines that are only the
  LOGICAL-WIDTH-1 schema display are pinned through `typeof(...)` /
  `to_arrow().schema`, which already agree. The pins guard the agreement.
- R-18c-3 (this lane, 2026-09-16): the defect is analyzer ORDER, not a missing
  kernel. DataFusion's default `TypeCoercion` runs before every appended rule,
  so it widens `Int64(1) + Int8` to Int64 before `SparkIntegerLiteral` ever sees
  the literal. The narrowing must run before the first `TypeCoercion`.

## PROPOSITION LEDGER — SQL-LITERAL-TYPING-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Unsuffixed integral literals answer Spark's type: Int32 when the value fits i32, Int64 when it needs 64 bits, DECIMAL(p,0) past i64, refusal past 38 digits. | `test_sql_literal_typing_1.py` LIT-SQL-00…09, 13…15 cells (value, Arrow type, error class/message). | PROVEN | Green after the fix: 06 answers decimal(19,0), 09 refuses with the recorded class and message, the rest unchanged. Probe LIT-SQL-06/09 cells SAME. pins: sql-literal-typing-1/C-001. §2. |
| C-002 | Binary arithmetic meeting a literal answers Spark's type: `+ - * %`, `pmod`, `1 + 1L` bigint, `2147483647 + 1` raises ARITHMETIC_OVERFLOW, `1 + 1.5` decimal(3,1), `1 / 2` double, `1 + decimal(5,2)` decimal(6,2), tinyint plus 2^63 decimal(20,0). | Same pin file, LIT-SQL-16, 19…25, 27, 35, 36, 40…42, 53, LIT-PY-08 cells. | PROVEN | Green after the fix: 16, 19, 21, 27, 41, 42, PY-08 answer int; 35 answers decimal(20,0); 22/53 still raise. No per-operator retag: DataFusion's own coercion produces the promotion once the literal is Int32. pins: sql-literal-typing-1/C-002. §2. |
| C-003 | Coercion sites answer Spark's type: `coalesce`, `CASE`, `array(...)`, `greatest` over int literal plus tinyint answer int / array<int>; `1 = CAST(1 AS TINYINT)` stays boolean true. | Same pin file, LIT-SQL-30…34 cells. | PROVEN | Green after the fix: 30, 31, 33 answer int; 32 answers array<int>; 34 still boolean true. Same mechanism as C-002. pins: sql-literal-typing-1/C-003. §2. |
| C-004 | `hex(CAST(<expr> AS BINARY))` under ANSI off answers Spark's byte count on both doors; LIT-SQL-44 and LIT-PY-05 agree. | Same pin file, LIT-SQL-44…51 and LIT-PY-05 cells. | PROVEN | Green after the fix: 44 and 47 answer `00000002`; the agreement pin asserts LIT-SQL-44 equals LIT-PY-05. Follows from C-002, no extra code. pins: sql-literal-typing-1/C-004. §2. |
| C-005 | `CAST(127 AS TINYINT) + CAST(1 AS TINYINT)` under ANSI raises BINARY_ARITHMETIC_OVERFLOW. | Same pin file, LIT-SQL-52 cell; or BACKLOG residue row BL-20-OVF naming the seam. | OPEN | Still wraps to -128 after the fix (no unsuffixed literal involved, so the rule never fires). Needs checked Int8/Int16 add/sub/mul kernels plus the `S`-suffixed error shape: new kernels in `repark-functions`, outside this round's fence. Stays OPEN with BACKLOG row BL-20-OVF; the wrap is held by a declared-divergence pin that flips red when the kernel lands. §5. |
| C-006 | The Python door does not regress: LIT-PY-00…09 stay as recorded. | Same pin file, LIT-PY-00…09 cells; PY-07 pins today's refusal. | PROVEN | Green after the fix: all nine cells plus the PY-07 refusal pin unchanged (`F.lit` builds Int32 directly, so the SQL-text rule never fires on the Python door). pins: sql-literal-typing-1/C-006. §2. |
| C-007 | BL-20 moves to FIXED with pin names and the oracle path; residues (`div`, `~`, C-005 if open) are their own rows. | Registry diff plus this ledger. | PROVEN | BL-20 FIXED with the pin file, the fixture and the oracle path; new BACKLOG row BL-20-OVF for C-005; `div` / `~` named as SPARK-SQL-GRAMMAR-1 C-001/C-002 residues. pins: sql-literal-typing-1/C-007. §2. |

## 1. Red-first record (base `33c87cbf`, release native in `.venv`, 2026-09-16)

`python/repark/tests/test_sql_literal_typing_1.py` over
`sql_literal_typing_1_spark_oracle.json` (verbatim copy of the orchestrator's
live PySpark 4.1.2 recording `/tmp/oc-worker/sc/oracle/bl20-oracle.json`,
batch `sc18-bl20-literal-typing`, measured 2026-09-16). One case per in-scope
cell, driven by cell id; value plus `to_arrow().schema` Arrow type, error class
plus message where the oracle records an error. `div` (LIT-SQL-26) and unary
`~` (LIT-SQL-38) are not pinned (SPARK-SQL-GRAMMAR-1 residues, named in C-007).

| Cell | Oracle (Spark 4.1.2) | RePark on base | State |
|---|---|---|---|
| LIT-SQL-06 | `decimal(19,0)` value 9223372036854775808 | `typeof(UInt64) is not implemented` | RED (C-001) |
| LIT-SQL-09 | refuses DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION | answers decimal(39,0) (decimal256) | RED (C-001) |
| LIT-SQL-16, 19, 21, 27, 41, 42 | int | bigint | RED (C-002) |
| LIT-SQL-35 | decimal(20,0) | decimal(21,0) | RED (C-002) |
| LIT-SQL-30, 31 | int | bigint | RED (C-003) |
| LIT-SQL-32 | array<int> | array<bigint> | RED (C-003) |
| LIT-SQL-33 | int | bigint | RED (C-003) |
| LIT-SQL-44, 47 | `00000002` (4 bytes) | `0000000000000002` (8 bytes) | RED (C-004) |
| LIT-SQL-52 | raises BINARY_ARITHMETIC_OVERFLOW | answers -128 | RED (C-005) |
| LIT-PY-08 | int | bigint | RED (C-002) |
| All other in-scope cells | as recorded | as recorded | GREEN, pinned |

Failing summary pasted at red run: `19 failed, 44 passed` —
every RED row in the table above fails for the recorded reason, every GREEN row
passes (full list in §2).

## 2. Work log

### 2026-09-16 — open, red pins

- Cloned state verified: branch `feat/sql-literal-typing-1` on `33c87cbf`,
  oracle present, probe reproduces the brief's 30 DIFF lines exactly.
- Root-cause analysis (verified against DataFusion 54.1.0 sources in the cargo
  cache): the default analyzer is `[ResolveGroupingFunction, TypeCoercion]`;
  `SparkExtension::configure_analyzer_rules` inserts preparation rules before
  `type_coercion`, but every rule from `repark_functions::analyzer_rules()`
  (including `SparkIntegerLiteral`) is appended AFTER it via
  `ctx.add_analyzer_rule`. The first `TypeCoercion` therefore coerces
  `Int64(1) + Int8` to Int64 before the narrowing runs, and
  `SparkIntegerOverflow` then locks in the Int64 width. Bare literals still
  narrow (nothing coerced them), which is why LIT-SQL-00…05 are green while
  every promotion cell is red. pins: sql-literal-typing-1/C-001, C-002, C-003.
- Fixture copied verbatim (64 cells). Red pins written, run, evidence above.
- Red run 2026-09-16 (`.venv/bin/python -m pytest
  python/repark/tests/test_sql_literal_typing_1.py -q -p no:cacheprovider`):
  `19 failed, 44 passed`. Failing: LIT-SQL-06 (typeof(UInt64) unimplemented),
  16, 19, 21, 27, 30, 31, 32, 33, 35 (decimal(21,0)), 41, 42, 44, 47 (8 bytes),
  LIT-PY-08 (bigint), the 09 precision refusal, the 52 tinyint overflow, the
  hex-agreement pin, the nullability pin (blocked on 06). Passing: every other
  in-scope cell, including the LOGICAL-WIDTH-1 display cells through
  `typeof`/`to_arrow` (10, 11, 17, 18, 28, 29, 37, 39, 43, PY-03, PY-04).
- Green run 2026-09-16 after the `spark_literal_typing.rs` fix (release native
  rebuilt): `63 passed`. Probe `rp.py` over the 64 oracle cells: 30 DIFFs
  down to 14, and every remaining DIFF is declared out of scope (11
  LOGICAL-WIDTH-1 display cells whose `typeof` agrees, `div` LIT-SQL-26,
  unary `~` LIT-SQL-38) except LIT-SQL-52 (C-005, declared divergence pin).
- Registry commit: BL-20 → FIXED with pin names and the oracle path, new
  BACKLOG row BL-20-OVF carrying C-005 with the seam named, `div` / `~`
  named as SPARK-SQL-GRAMMAR-1 residues. C-007 PROVEN.
- Gate fallout (announced): `extension/tests.rs`
  `analyzer_configuration_seats_hof_preparation_and_float_stringify_before_type_coercion`
  pinned the old three-seat pre-coercion order, so the new fourth seat failed
  it. Updated to the new contract (name kept) with an `extension/map.md` row;
  no behavior assertion changed, only the seating this card owns.
- Facade-suite fallout, round 1 (27 failed): the early narrowing left two
  cements for the first `TypeCoercion`, both diagnosed with a rule-by-rule plan
  probe and both fixed inside the new rule. (i) Higher-order lambdas over
  column-fed arrays: `HigherOrderPreparation` binds the lambda variable from
  the still-wide array, so the coercion widened the array back to Int64. The
  rule now recurses with subqueries (the inner constructor lives a level down;
  `map_expressions` alone never reached it) and re-resolves lambda variables
  after narrowing, so the coercion sees one consistent narrow world.
  (ii) Unions: `recompute_schema` preserves the stale Int64 union schema and
  `coerce_union_schema_with_schema` seeds from it, so the coercion wrapped
  every narrowed branch in `CAST(... AS BIGINT)` — visibly cementing
  `5 / 2 UNION ALL 7 / 2` to integer division. The rule now re-derives the
  union schema from its inputs (`try_new_with_loose_types`); the coercion
  then does the real merge. Both fixes carry Rust unit tests beside the rule.
- Facade-suite fallout, round 2 (targeted repairs, Spark-recorded answers):
  `overlay(..., -1)` stopped truncating because the coercion wraps the
  narrowed `-1` in a cast before the rewrite sees it; `is_negative_one_literal`
  now sees through one compiler cast (Spark treats every `-1` len as omit).
  `COALESCE(CAST(NULL AS INT), 1)` now answers int32 — TY-7's recorded Spark
  answer — so the TY-7 twins flip to int32 and the TY-7 row moves to FIXED.
  `coalesce(int_col, 0)` CTAS width flips long to int the same way (Spark:
  int); the test's nullability purpose is untouched.
- Indexed-transform note: the live oracle types Spark's `(x, i)` index INT, so
  the Int32 index stays; the `null_element` door row now reads int32 on both
  doors (the column-vs-SQL disagreement is gone with the widening). Two
  `fnp8` sql_columns dispositions re-measured from the running engine to the
  oracle-converged int32 schema (script `/tmp/bl20-rerecord.py`, measured not
  hand-computed). A mid-course index-to-Int64 experiment was reverted the same
  day once the oracle decode showed INT.
- Files owned by other units touched (announced): `test_types_1.py` (TY-7
  twins), `test_fnp_8_sql_door.py` (null_element row), `test_cutover_schema_1.py`
  (CTAS width cell), `fnp8_repark_dispositions.json` (2 re-measured schemas),
  `crates/repark-sql/tests/cross_door.rs` (2 bodies spell literals BIGINT).
  Each change moves the pin toward the recorded Spark answer; nothing else in
  those files was edited.
- Second width-flip wave (facade suite, all the same disease — incidental Int64
  pins on literal-built fixtures, each verified Spark-true before flipping):
  `test_case_insensitive_conform.py`, `test_catalog_flow.py` (×2),
  `test_eager_own_1.py` (mapInArrow declares INT now),
  `test_explode_rewrite.py` (`x` only; `e` stays Int64 over explicit BIGINT),
  `test_filter_predicate_rewrite.py` (×3 year/YEAR legs),
  `test_merge_into.py` (×3), `test_sql_dml_eager.py` (×3, stale fixture comment
  removed), `test_eager_budget_1.py` (byte budget re-measured 312 → 300, 12
  bytes saved by narrower rows). Map rows updated per file.
- Native-door follow-up (hand-off, not absorbed): the native door still answers
  Int64 for `SELECT 1` while the Spark door now answers Int32 — the ANSI side of
  the chartered literal-width split (F-Y10-1 decided it, TYPES-1 kept it for
  catalog rows). Narrowing native needs the rule installed for core sessions,
  but the rule lives in `repark-spark` and the DAG forbids core depending on
  the door crate — a home question for its own card, with native pins.
  Recorded as BACKLOG row BL-20-NATIVE. The two `cross_door.rs`
  schema-equality tests keep testing CTAS/MERGE mechanics with explicit BIGINT
  literals; their mutation coverage is unchanged.

## 3. Design (Rust-first per Q-17a-2)

- New analyzer rule in `crates/repark-spark` (the Spark door owns literal
  typing): narrow `Int64` literals that fit i32, type `UInt64` literals as
  `Decimal128(digits, 0)`, refuse decimal literals past precision 38 with
  Spark's class and message, fold `-2147483648`. Installed from
  `SparkExtension::configure_analyzer_rules` immediately before the first
  `type_coercion`, so DataFusion's own coercion then produces Spark's
  promotion with no per-operator retag (Q-15c-6). The late `SparkIntegerLiteral`
  stays untouched (idempotent no-op afterwards).
- Skips `Limit` plans and rebuilds `Values` rows, mirroring the existing rule,
  so `LIMIT`, `range(n)`, ordinals and indices see no new shape.

## 4. Verification

- `cargo test -p repark-spark --lib` — exit 0, 1035 passed, 0 failed, 4 ignored
  (includes the 12 `spark_literal_typing` unit tests and the updated order pin;
  count from the final `make verify` run).
- `make rust-clippy` — exit 0.
- `make verify` — exit 0 (56 `test result: ok`, zero failures; covers lint,
  format, clippy, workspace Rust tests, map/manifest/ledger gates).
- Release native rebuilt (`maturin develop --release` in `python/repark`,
  exit 0) from the final shipped sources.
- `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider` —
  exit 0: 9098 passed, 367 skipped, 34 xfailed.
- `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
  python/repark-parity/tests -q` — exit 0: 757 passed, 2 skipped, 12 xfailed.
- BL-20 probe `rp.py` over the 64 oracle cells — 30 DIFFs to 14; every
  remaining DIFF is declared out of scope (11 LOGICAL-WIDTH-1 display cells
  whose `typeof` agrees, `div`, unary `~`) except LIT-SQL-52 (C-005 declared
  divergence pin).
- Not run here: the live-Spark legs (`test_live_recoercion_shapes_match_the_oracle`
  needs `REPARK_PARITY_LIVE=1` + JVM; its pair updated to `(int32, int32)` for
  nightly to confirm) and tier-2 live AWS (never against unmerged code).

## 7. Remediation round 1 verification (2026-09-16, head `d5622025`)

- `cargo test -p repark-spark --lib` — exit 0: 1041 passed, 0 failed,
  4 ignored (18 `spark_literal_typing` tests incl. the HOF regression pin).
- `cargo test -p repark-python --lib` — exit 0: 75 passed, 0 failed
  (door-parity factorial row green after the dispatch fix).
- `make rust-clippy` — exit 0. `make verify` — exit 0 (56 ok, zero failures).
- Release native rebuilt (`uvx maturin@1.14.1 develop --release`,
  exit 0) from the final shipped sources.
- `make py-test-facade` — exit 0: 9108 passed, 372 skipped, 34 xfailed
  (9098 round-1 + the 10 new LIT2/F.expr pins; the 24 FNP-8 HOF pins broke
  mid-round on the recompute skip and are green after the fix).
- `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest
  python/repark-parity/tests -q -p no:cacheprovider` — exit 0: 757 passed,
  2 skipped, 12 xfailed (unchanged from round-1).
- Not run here: live-Spark legs (no JVM) and tier-2 live AWS (never against
  unmerged code).

## 5. Decisions

- C-005 (2026-09-16): stays OPEN with BACKLOG residue row BL-20-OVF. Measured
  after the C-001…C-004 fix: the cell still answers -128 because neither side
  is an unsuffixed literal, so no planner rule can see a width to keep. The
  fix is checked Int8/Int16 `+`/`-`/`*` kernels (ANSI raise with the
  `127S + 1S` error shape, legacy wrap) — new kernels in `repark-functions`,
  outside this round's fence. Not a silent skip: the wrap is pinned by
  `test_tinyint_overflow_wrap_is_declared`, which flips red when the kernel
  lands.
- `div` / `~`: out of scope, named in the BL-20 registry row as still open
  (SPARK-SQL-GRAMMAR-1 C-001/C-002 own them).

## 6. Remediation round 1 (2026-09-16, head `996094c8` rebased, red commit `e94d57bb`)

Critic-logic, rustperf and pyperf reads over the 8 round-1 commits; the 13
measured LIT2 cells (`/tmp/oc-worker/sc/oracle/lit2-oracle.json`, batch
`sc18-lit2`) appended verbatim to the fixture. Red on the round-1 build: 10
fail (LIT2-SQL-00/01/02, LIT2-SQL-03, LIT2-PY-00…04,
`test_fexpr_mixed_width_matches_sql_door`); LIT2-SQL-04/05/06/07 already
green and guard the fix. LIT2-SQL-06 carries
`conf: {spark.sql.ansi.enabled: false}`, matching the wrap it records; the
other LIT2 cells carry no conf (ANSI on), so no oracle conflict exists with
LIT-SQL-22/53.

- R-18c-4 (this lane): L-002 is a plan-construction check, not an analyzer
  order gap. `to_field` verifies ScalarFunction arguments
  (`verify_function_arguments`) while the logical plan is built, before every
  analyzer rule runs — seating the narrowing first cannot reach it. The fix
  follows the `add_months` house pattern: shadow UDFs whose `coerce_types`
  accepts every integer width (literals narrow in the early rule first;
  explicit widths coerce to Int32 exactly like `add_months` already does).
- R-18c-5 (this lane): `insert_literal_rule_before_coercion` keeps its name
  and seats the rule immediately before `type_coercion`. One shared function
  serves the session door and the `F.expr` door.
- R-18c-8 (this lane): the first-among-pre-coercion seat was built, measured
  (24 FNP-8 HOF pins red) and reverted — and the revert stayed red, which
  exonerates the seat. The true cause is the PERF-001 `recompute_schema`
  skip: narrowing runs bottom-up, so a parent whose own expressions are
  unchanged (e.g. `column1 AS a` over narrowed `VALUES`) keeps its
  construction-time Int64 schema, and the lambda variable binds the stale
  width. The fix always recomputes after `map_expressions` (round-1
  behavior) and only gates `resolve_lambda_variables`; the pre-check,
  the `Values` check-first and the conditional `Union` rebuild stay.
  L-002 needs no seat change at all (plan-construction verify precedes every
  rule). Proven by the `values_fed_hof_body_narrows_without_late_rule` unit
  pin, which also holds PERF-005's premise (no late rule in its pipeline).
- R-18c-6 (this lane): the late `SparkIntegerLiteral` leaves both Spark
  doors (it is subsumed by the early rule) but stays in
  `repark_functions::analyzer_rules()` for the non-door test contexts that
  never seat the early rule. Removal is also what keeps L-003 fixed: the
  late fold would re-fold the parenthesized form after coercion.
- R-18c-7 (this lane): PERF-004 and the two pyperf P3s are ledger notes and
  hand-off rows; the Python files belong to runs 18a/18b and are not edited
  here.

| Finding | Disposition | Evidence |
|---|---|---|
| L-001 (P1) | FIXED | `build_expr_context` seats the early rule through the shared insert function; `test_fexpr_mixed_width_matches_sql_door` plus LIT2-PY-00…04 green. |
| L-002 (P2) | FIXED | `DateOffset` (`date_add`/`date_sub`) and `Factorial` shadows accept integer widths in `coerce_types` and delegate kernels upstream; LIT2-SQL-00/01/02 green. The whole upstream `Exact(Int32)` family was surveyed: the only strict-Exact int UDFs repark does not already shadow are these three (`add_months`, `make_date`, `sequence` have local UDFs). |
| L-003 (P2) | FIXED | The `Negative` fold is gone: only the lexer-level negative token (planned as a negative `Int64` literal) narrows; `-(2147483648)` stays `Negative(Int64)` → bigint. LIT2-SQL-03/04/05/07 green. |
| PERF-001 (P2) | FIXED | Plan-level apply pre-check skips the walk when no narrowable literal exists; `resolve_lambda_variables` runs only with a higher-order function on the node. `recompute_schema` keeps running unconditionally: a first version gated it on a real transform and broke 24 FNP-8 HOF pins (stale parent schemas after bottom-up narrowing), fixed the same day. Release-native medians before → after (`/tmp/bl20-perf-probe.py` shapes): (a) 500-col int 30.63 → 30.89, str 30.50 → 29.86; (b) 2000-arm CASE int 189.26 → 187.92, str 195.67 → 185.76; (c) VALUES 10k×2 int 518.31 → 502.46, str 502.01 → 492.28; (d) 200 withColumn F.lit+schema 4454 → 4270, F.expr+schema 4583 → 4429, build-only 3513 → 3850 (no analysis; noise); UNION 200 int 35.21 → 34.52, str 35.34 → 34.01. The int-vs-str rewrite deltas stay in the noise; the 6–7× Spark/native gap is the whole Spark analyzer roster, out of scope. |
| PERF-002 (P2) | FIXED | `rewrite_values` applies over `&Expr` first and clones a row only when a cell changes. |
| PERF-003 (P3) | FIXED | `Union` rebuilds only when a child schema field type moved. |
| PERF-005 (P3) | FIXED | Late `SparkIntegerLiteral` uninstalled on both Spark doors; full facade plus parity suites green. |
| PERF-004 (P3) | NOTED | Mixed-width hash joins keep a per-row `CAST AS Int64`: Spark-true (unsuffixed `1` is INT), a join/kernel change, not a literal-typing change. `CAST(1 AS BIGINT)` recovers the old kernel. |
| pyperf P3 (udtf) | HAND-OFF | No 15+ digit token present in `udtf.py` at this head; run 18a to confirm against the read. |
| pyperf P3 (functions) | HAND-OFF | The `CAST(... AS INT)` foldable workarounds in `functions.py` (`add_months`, `date_add`) are engine-redundant now that the shadows accept Int64; runs 18a/18b own removal. |
