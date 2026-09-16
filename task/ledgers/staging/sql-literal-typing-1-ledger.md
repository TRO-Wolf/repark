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
