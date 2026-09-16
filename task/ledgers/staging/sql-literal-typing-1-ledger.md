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
| C-001 | Unsuffixed integral literals answer Spark's type: Int32 when the value fits i32, Int64 when it needs 64 bits, DECIMAL(p,0) past i64, refusal past 38 digits. | `test_sql_literal_typing_1.py` LIT-SQL-00…09, 13…15 cells (value, Arrow type, error class/message). | OPEN | RED on base: LIT-SQL-06 errors `typeof(UInt64) is not implemented`, LIT-SQL-09 answers decimal(39,0) as decimal256. §1. |
| C-002 | Binary arithmetic meeting a literal answers Spark's type: `+ - * %`, `pmod`, `1 + 1L` bigint, `2147483647 + 1` raises ARITHMETIC_OVERFLOW, `1 + 1.5` decimal(3,1), `1 / 2` double, `1 + decimal(5,2)` decimal(6,2), tinyint plus 2^63 decimal(20,0). | Same pin file, LIT-SQL-16, 19…25, 27, 35, 36, 40…42, 53, LIT-PY-08 cells. | OPEN | RED on base: 16, 19, 21, 27, 41, 42 answer bigint; 35 answers decimal(21,0). 20, 22…25, 36, 40, 53 already green, pinned. §1. |
| C-003 | Coercion sites answer Spark's type: `coalesce`, `CASE`, `array(...)`, `greatest` over int literal plus tinyint answer int / array<int>; `1 = CAST(1 AS TINYINT)` stays boolean true. | Same pin file, LIT-SQL-30…34 cells. | OPEN | RED on base: 30…33 answer bigint / array<bigint>. 34 already green, pinned. §1. |
| C-004 | `hex(CAST(<expr> AS BINARY))` under ANSI off answers Spark's byte count on both doors; LIT-SQL-44 and LIT-PY-05 agree. | Same pin file, LIT-SQL-44…51 and LIT-PY-05 cells. | OPEN | RED on base: 44 and 47 answer 8 bytes. 45, 46, 48…51, PY-05 already green, pinned. §1. |
| C-005 | `CAST(127 AS TINYINT) + CAST(1 AS TINYINT)` under ANSI raises BINARY_ARITHMETIC_OVERFLOW. | Same pin file, LIT-SQL-52 cell; or BACKLOG residue row BL-20-OVF naming the seam. | OPEN | RED on base: answers -128 (wraps). Needs a checked Int8/Int16 kernel beyond the planner; decision recorded in §5. |
| C-006 | The Python door does not regress: LIT-PY-00…09 stay as recorded. | Same pin file, LIT-PY-00…09 cells; PY-07 pins today's refusal. | OPEN | All green on base except the LOGICAL-WIDTH-1 display on PY-03/04, pinned through `to_arrow()` (int8/int16). §1. |
| C-007 | BL-20 moves to FIXED with pin names and the oracle path; residues (`div`, `~`, C-005 if open) are their own rows. | Registry diff plus this ledger. | OPEN | Done last, in the registry commit. |

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

(commands with real exit codes pasted here at gate time)

## 5. Decisions

- C-005: measured first (this section records fix vs BACKLOG row BL-20-OVF).
- `div` / `~`: out of scope, named in the BL-20 registry row as still open
  (SPARK-SQL-GRAMMAR-1 C-001/C-002 own them).
