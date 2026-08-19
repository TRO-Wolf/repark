# Unit ledger — FN-GT1 retro (GT1-FIX)

**Unit:** GT1-FIX · conductor-26 · **Date:** 2026-08-18 ·
**Executor:** Grok (grok-4.6) ·
**Worktree:** conductor-26 isolated worktree · **Branch:** `grok/c26-gt1-fix` ·
**Base:** origin/main post-#179.

**Oracle.** Throwaway venv outside the repo (never mixed with the worktree `.venv`):

- CPython **3.12.3** (3.11 is not present offline; PySpark 4.1.2 is what binds)
- `pyspark==4.1.2`
- `py4j==0.10.9.9` (4.1.2's pin; 0.10.9.7 is the 3.5.x bundled copy)
- `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`
- `SPARK_LOCAL_IP=127.0.0.1`

Every expected value / type / signature / NULL claim in this unit cites that
live oracle.

## Findings

| ID | Fix |
|---|---|
| G1 | `regexp_count` / `regexp_instr` drop force-lit on the pattern. Pins rewritten to `F.lit(...)`. Direction pin: column `regexp` vs `F.lit("regexp")`. |
| G2 | `split_part` drops force-lit on delimiter and `partNum`. Keyword restored `partNum`. |
| G3 | `width_bucket` drops force-lit on min/max/`numBucket`. Keyword restored `numBucket`. Silent-wrong-answer (not raise). |
| G4 | `bit_get` / `getbit` drop force-lit on `pos`. |
| G5 | `bin` / `rint` unix_date-mold casts (`long` / `double`). `bit_length` / `octet_length` owned kernel (`spark_length.rs`) stringifies non-binary, passes BINARY through. Ledger: G5 forces this kernel. |
| G6 | Facade `idx: Column \| int \| None = None`. Dispatch 3-arg arm: Spark `idx` is NULL-propagate-only — `RegExpInStr.nullSafeEval` ignores the value (`MatchResult.start()+1`). Never forward as DataFusion start-position. Discriminator pin: `regexp_instr('abcde','b(c)d',1)` is **2** not 3. |
| G7 | `F.getbit` calls `_scalar("getbit", ...)` so the name goes down the wire. Projection `getbit(x, 1)` matches live PySpark. The `"getbit"` match arm is now reachable (maps to the `bit_get` kernel). |
| G8 | `numBucket`, `partNum`, `numBits` ×3 restored (`# noqa: N803`). Keyword-form pins. |
| P1 | NULL-input row per name. `regexp_count` NULL is **NULL** (the masked NULL→0 break). |
| P2 | Exact Arrow types (Spark oracle): factorial int64, rint float64, width_bucket int64, bit_count int32, bit_get/getbit int8, regexp_count/instr int32, bit_length/octet_length int32, shifts follow input width (Python-int frame → int64 for the negative-unsigned pin). |
| P3 | `shiftrightunsigned(-2)` / `(-8)` vs `shiftright` (2^63-1 / 2^63-4). |
| P4 | `unhex('C3')`: `is_valid_utf8` False; `make_valid_utf8` → U+FFFD. |
| P5 | Direction pin per name. |
| F1 | This ledger + `task/map.md` row. |
| F2 | FN-A/B/F Deferred lists: GT1-shipped names removed; genuine deferrals left. |
| F3 | Docstring examples are call forms live PySpark accepts; `test_gt1_docstring_examples_execute`. |

## lit_indices sweep (38 sites)

PySpark 4.1.2 `inspect.signature` + live probe. Verdict: **FIX** = GT1 class
(force-lit on a `ColumnOrName` str); **PASS** = PySpark really is literal-only
(`str` / `int` / `Column|str` meaning str is a literal).

| # | Wrapper | Param | Pos | PySpark 4.1.2 | Repark before | Verdict |
|---|---|---|---|---|---|---|
| 1 | `bit_get` | `pos` | 1 | `ColumnOrName` | force-lit | **FIX** (G4) |
| 2 | `shiftleft` | `numBits` | 1 | `int` | force-lit | **PASS** |
| 3 | `shiftright` | `numBits` | 1 | `int` | force-lit | **PASS** |
| 4 | `shiftrightunsigned` | `numBits` | 1 | `int` | force-lit | **PASS** |
| 5 | `element_at` | `extraction` | 1 | `Any` (str = literal key) | force-lit unless Column | **PASS** (GT2 W1; live `element_at(map,'b')` = 2) |
| 6 | `str_to_map` | `pairDelim`/`keyValueDelim` | 1,2 | `Optional[ColumnOrName]` | force-lit unless Column | **FIX** (GT2 claimed `str`; live inspect + `str_to_map(t,p,k)` column-name path) |
| 7 | `make_date` | y/m/d | 0,1,2 | `ColumnOrName`; ints auto | force-lit only non-Column/non-str | **PASS** |
| 8 | `make_interval` | parts | * | `Optional[ColumnOrName]` | `make_date` mold | **PASS** |
| 9 | `make_dt_interval` | parts | * | `Optional[ColumnOrName]` | `make_date` mold | **PASS** |
| 10 | `width_bucket` | min/max/`numBucket` | 1,2,3 | `ColumnOrName`; `numBucket` also `int` | force-lit {1,2,3} | **FIX** (G3) |
| 11 | `lpad` | `len`/`pad` | 1,2 | `len: Column\|int`; `pad: Column\|str` | force-lit | **PASS** (str pad is a literal; live `lpad(s,10,'*')` works) |
| 12 | `rpad` | `len`/`pad` | 1,2 | same | force-lit | **PASS** |
| 13 | `instr` | `substr` | 1 | `Column\|str` | force-lit | **PASS** (str is a literal; live `instr(s,'ell')` = 2) |
| 14 | `concat_ws` | `sep` | 0 | `sep: str` | force-lit | **PASS** (Column sep → `NOT_ITERABLE`) |
| 15 | `regexp_replace` | pattern/replacement | 1,2 | `Union[str, Column]` | force-lit unless Column | **PASS** (str is a literal) |
| 16 | `round` | `scale` | 1 | `Column\|int` | force-lit | **PASS** |
| 17 | `array_contains` | `value` | 1 | `Any` | force-lit unless Column | **PASS** (str is a literal needle) |
| 18 | `repeat` | `n` | 1 | `ColumnOrName\|int` | force-lit only `int` | **PASS** (str `n` is already a column) |
| 19 | `translate` | matching/replace | 1,2 | `str`, `str` | force-lit | **PASS** (Column → `NOT_ITERABLE`) |
| 20 | `substring_index` | delim/count | 1,2 | `str`, `int` | force-lit | **PASS** |
| 21 | `encode` | `charset` | 1 | `str` | force-lit | **PASS** |
| 22 | `decode` | `charset` | 1 | `str` | force-lit | **PASS** |
| 23 | `array_join` | `delimiter` | 1 | `str` | force-lit | **PASS** |
| 24 | `next_day` | `dayOfWeek` | 1 | `str` | force-lit | **PASS** |
| 25 | `date_part` | `field` | 0 | typed `Column`; live str = column name | force-lit | **FIX** (live `date_part('YEAR', d)` unresolved `YEAR`) |
| 26 | `rand` | `seed` | 0 | `Optional[int]` | force-lit | **PASS** |
| 27 | `randn` | `seed` | 0 | `Optional[int]` | force-lit | **PASS** |
| 28 | `substring` | pos/len | 1,2 | `ColumnOrName\|int` | force-lit only `int` | **PASS** |
| 29 | `contains` | `right` | 1 | `ColumnOrName` | force-lit str | **FIX** |
| 30 | `like` | `pattern` | 1 | `ColumnOrName` | force-lit str | **FIX** |
| 31 | `ilike` | `pattern` | 1 | `ColumnOrName` | force-lit str | **FIX** |
| 32 | `regexp_like` | `regexp` | 1 | `ColumnOrName` | force-lit str | **FIX** |
| 33 | `btrim` | `trim` | 1 | `Optional[ColumnOrName]` | force-lit str | **FIX** |
| 34 | `startswith` | `prefix` | 1 | `ColumnOrName` | force-lit str | **FIX** |
| 35 | `endswith` | `suffix` | 1 | `ColumnOrName` | force-lit str | **FIX** |
| 36 | `split_part` | delim/`partNum` | 1,2 | `ColumnOrName` ×3 | force-lit | **FIX** (G2) |
| 37 | `regexp_count` | `regexp` | 1 | `ColumnOrName` | force-lit | **FIX** (G1) |
| 38 | `regexp_instr` | `regexp` (+ `idx`) | 1 (+2) | regexp `ColumnOrName`; `idx: Column\|int` | force-lit regexp | **FIX** (G1+G6) |

No numbered questions: every FIX row is a live-signature + live-probe match of the
GT1 class. `str_to_map` is FIX because live 4.1.2 contradicts the GT2 "plain
`str`" claim (inspect + `str_to_map(t, p, k)` on columns).

## Kernel edits (G5 / G6 / G7)

| Finding | Edit |
|---|---|
| G5 | `crates/repark-functions/src/spark_length.rs` — `bit_length` / `octet_length` UDFs. Registered from `string::functions()` (SQL door overwrite) and `expr_fn::{bit_length,octet_length}` (facade). `bin`/`rint` are facade casts, not a new kernel. Round-2 A3: refuse ARRAY/STRUCT/MAP; decimal scale-padded stringify (`12.50` → octet 5 / bit 40). |
| G6 | Round-2 A1/A2: `spark_regexp.rs` is the **one** semantics source. `regexp_count` 2-arg NULL-in NULL-out INT. `regexp_instr` 2–3 args; 3rd is Spark idx (CAST to Int32, NULL-propagate, **value ignored** — never DF start). Dispatch + `expr_fn` embed the same UDF. |
| G7 | No kernel. Facade sends the name `"getbit"`; existing `"bit_get" \| "getbit"` arm is now reachable. SQL door already projects `getbit(...)` via the datafusion-spark alias (F-6e REFUTED). |

## Mutation-proof pins

| If this is dropped… | this test reds |
|---|---|
| regexp pattern force-lit | `test_regexp_count_str_is_column_name` (col 3,0 vs lit 0,0) |
| split_part delim force-lit | `test_split_part_str_is_column_name` |
| bit_get pos force-lit | `test_bit_get_pos_is_column_name` |
| getbit renamed to bit_get on the wire | `test_getbit_projects_getbit_name` (`getbit(x, 1)` vs `bit_get(x, 1)`) |
| regexp_instr idx forwarded as DF start | `test_regexp_instr_idx_matches_live_spark` + `test_sql_door_regexp_instr_idx_is_ignored` (`b(c)d` idx=1 is 2, not 3; idx=99 still 2) |
| regexp_count NULL→0 | `test_null_inputs` + `test_sql_door_regexp_count_null_is_null` |
| ARRAY stringify on bit_length | `test_sql_door_bit_length_refuses_array` |
| decimal scale dropped | `test_sql_door_decimal_length_scale_padding` |
| STRING partNum planning-error | `test_sql_door_split_part_string_part_num` |
| omitted idx 2-arg display | `test_regexp_instr_omitted_idx_projects_zero` (`…, 0`) |
| getbit SQL renamed to bit_get | `test_sql_door_getbit_projects_getbit_name` |
| unsigned==signed on +8 | `test_shiftrightunsigned_negative_diverges` |
| is_valid only on "ok" | `test_utf8_invalid_bytes` |
| contains str force-lit | `test_fn_b_str_is_column_name` |
| str_to_map delim force-lit | `test_str_to_map_delimiters_are_regex` column-name row |
| docstring bare `F.bin(13)` | `test_gt1_docstring_examples_are_not_bare_literals` |

## Files

- `python/repark/src/repark/spark/functions_{math,bitwise,expr,collections,datetime}.py`
- `crates/repark-python/src/column/function_dispatch.rs` + `column/map.md`
- `crates/repark-functions/src/spark_length.rs` / `spark_regexp.rs` / `spark_split_part.rs` / `string.rs` / `expr_fn.rs` / `lib.rs` / `map.md`
- `python/repark/tests/test_functions_{gt1,gt2,b,d}.py` / `test_fn_batch3.py` / `tests/map.md`
- `python/repark/src/repark/spark/map.md`
- `task/fn-gt1-ledger.md` / `task/map.md`

## Gates

| Gate | Result |
|---|---|
| `make preflight` (round-1) | exit **0**; facade **3418 passed / 70 skipped** |
| `make preflight` (round-2) | exit **0**; facade **3427 passed / 70 skipped** |
| two-pass hygiene | PASS1 added-line needles **0**; PASS2 new files **0** |

## Residuals (honest)

- **A4 (kept, named):** Facade `F.bin(F.lit(True))` is `"1"`; `F.rint(F.lit(True))` is `1.0`. Spark 4.1.2 analysis-refuses BOOLEAN: `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` (`bin` requires BIGINT; `rint` requires DOUBLE). Pinned: `test_bin_bool_over_accepts_where_spark_refuses`. Registry row is orchestrator-side — not a 0.4.0 kernel fix.
- `date_part` column-valued field still fail-louds (`requires non-empty constant string`); bare `'YEAR'` is an unresolved column name (GT1 class).
- `factorial` Int64 outside `i32` fail-louds on the CAST (pre-existing mold); domain `[0,20]` NULL is pinned for in-range Int32.
- **F-6f (CAST, not a GT1 kernel):** `CAST(unhex('C3') AS STRING)` is Arrow-invalid (incomplete UTF-8). Spark preserves the byte in STRING (`is_valid_utf8` False). BINARY-typed invalid UTF-8 remains the P4 pin. Escalated as CAST(binary→utf8). Pin: `test_cast_invalid_utf8_string_is_fail_loud`.
- **Java `.` vs Unicode scalar (named, not 0.4.0 matcher rewrite):** Spark `Matcher` treats `.` as one UTF-16 unit (`regexp_count('🐈', '.')` = 2). The `regex` crate matches one Unicode scalar (1). Empty-pattern count and `instr` start already use UTF-16. Escalated; not a NULL→0 class.

## Round-2 (2026-08-19) — A1–A6

Binding: BRIEF-conductor-26.md addendum 2026-08-19. Same branch, no force-push.

### A1 ignore-value probe (live PySpark 4.1.2, 2026-08-19)

`SELECT regexp_instr('abcde', 'b(c)d', N)`:

| N | Spark | group-index would be | DF start would be |
|---|---|---|---|
| 0 | 2 INT | — | 2 |
| 1 | 2 INT | **3** | 2 |
| 3 | 2 INT | — | 0 (past match) |
| 99 | 2 INT | — | 0 |
| NULL | NULL INT | NULL | 0 or error |

Unicode: `regexp_instr('🐈ab','ab')` = **3** (Java UTF-16 code units — 🐈 is 2 — not UTF-8 bytes 5 and not Unicode scalars 2). Empty pattern: `regexp_count('aaa','')` = 4; `regexp_instr('aaa','')` = 1. Overlap: `regexp_count('aaa','aa')` = 1.

**Do not "fix" idx back to group-index.** The discriminator is idx=1 → 2.

### A2 kernel

`spark_regexp.rs` registered from `string::functions()` (SQL overwrite) + `expr_fn` (facade). Dispatch arms call those builders only — no CASE wrap, no `spark_int32`. Residual SQL pins flipped to NULL and 2.

### F-5 — 19-row per-name signature table (live `inspect.signature`, PySpark 4.1.2)

| Name | PySpark 4.1.2 signature |
|---|---|
| `bin` | `(col: ColumnOrName) -> Column` |
| `hex` | `(col: ColumnOrName) -> Column` |
| `unhex` | `(col: ColumnOrName) -> Column` |
| `factorial` | `(col: ColumnOrName) -> Column` |
| `rint` | `(col: ColumnOrName) -> Column` |
| `bit_count` | `(col: ColumnOrName) -> Column` |
| `bit_get` | `(col: ColumnOrName, pos: ColumnOrName) -> Column` |
| `getbit` | `(col: ColumnOrName, pos: ColumnOrName) -> Column` |
| `shiftleft` | `(col: ColumnOrName, numBits: int) -> Column` |
| `shiftright` | `(col: ColumnOrName, numBits: int) -> Column` |
| `shiftrightunsigned` | `(col: ColumnOrName, numBits: int) -> Column` |
| `split_part` | `(src: ColumnOrName, delimiter: ColumnOrName, partNum: ColumnOrName) -> Column` |
| `regexp_count` | `(str: ColumnOrName, regexp: ColumnOrName) -> Column` |
| `regexp_instr` | `(str: ColumnOrName, regexp: ColumnOrName, idx: Column \| int \| None = None) -> Column` |
| `bit_length` | `(col: ColumnOrName) -> Column` |
| `octet_length` | `(col: ColumnOrName) -> Column` |
| `is_valid_utf8` | `(str: ColumnOrName) -> Column` |
| `make_valid_utf8` | `(str: ColumnOrName) -> Column` |
| `width_bucket` | `(v, min, max: ColumnOrName, numBucket: ColumnOrName \| int) -> Column` |

### F-6 / F-7 dispositions

| ID | Verdict | Evidence |
|---|---|---|
| F-6c | **FIX** | Spark `split_part('a.b.c', '.', '2')` → `'b'`. Repark planned `Int64` only. `spark_split_part.rs` coerces Utf8 → Int64. |
| F-6d | **FIX** | Spark wraps a bare-str idx as a literal then `CAST` to INT (`CAST_INVALID_INPUT` on `'i'`). Kernel now coerces idx to Int32 so the CAST bites (was silently ignored). |
| F-6e | **REFUTE** | SQL `SELECT getbit(6, 1)` already projects `getbit(...)` (datafusion-spark alias). Spark is `getbit(6, 1)`; DF wraps literals as `Int64(6)` — general display, not a rename to `bit_get`. Pin: name starts with `getbit(`. |
| F-6f | **REFUTE** as CAST / escalate | `CAST(unhex('C3') AS STRING)`: Spark keeps invalid byte (hex `C3`, `is_valid_utf8` False). Arrow Utf8 cannot hold it — CAST fail-loud. P4 BINARY path is the GT1 pin. |
| F-7 map.md | **FIX** | Stray `)` after "later" in `crates/repark-functions/map.md` `lib.rs` row. |
| F-7 display | **FIX** | Omitted idx now wraps `0` as a literal so F.* projects `regexp_instr(abcde, c, 0)` (live Spark). |

## Octo cycle 1 remediations

C1-Q-001/002 btrim+ilike discriminator pins; C1-Q-003 `unhex(C3)` octet=1; C1-Q-004 all three `numBits` keywords; C1-Q-005 `No field named`; C1-Q-006 SQL `bit_length(12)`; C1-Q-007/CL-001/002 `str_to_map` lead sentence; CL-003 `task/map.md` FN-A/B/F lists; CL-004 docstring mutation pin; CL-005 crate-root `spark_length.rs`; C1-SAF-001 overflow fail-loud; C1-L-001 documented+pinned; C1-L-003 SQL residual pinned.

## Pre-PR critic report (/repark-harden)

Engine: critic-octo N=3 (HIGH, claims_critic on) then finder-battery HIGH
(5 dims × 3-vote; loop-until-dry). Tier HIGH — engine dispatch arm +
0.4.0 gate. Review-only after Actor; gates were green before the critic
pass.

Critic-1 (quality/parity): attacked wrappers, pins, crates contracts,
docstrings, `spark_length.rs` overflow/casts. Cycle 1: S1 hollow pins
(btrim/ilike discriminators, `unhex(C3)` octet=1, all three `numBits`
keywords, date_part `No field named`, SQL `bit_length(12)`, F3
source-text, overflow fail-loud). Cycle 2: leftover pin/ledger nits.
Cycle 3: CLEAN. Null-report: unwrap/expect in prod, file-size ceilings
(`functions_expr.py` 2098/2500), `lib.rs` ceiling.

Critic-2 (security/safety): attacked `spark_length.rs` (i32 overflow,
ReDoS, panics), regexp CASE wrap, no secrets/injection. C1-SAF-001
(NULL on `i32::try_from` overflow) remediating to `exec_err!` +
`checked_mul(8)` + unit pin `spark_int_overflow_is_fail_loud`. Cycles
2–3 CLEAN. Null-report: unsafe, secrets, destructive AWS, production
panics.

Critic-3 (logic): attacked G6 idx, P1 NULL, C3 binary pass-through, SQL
residuals, unsigned negative, getbit name. Round-1 residuals (SQL
`regexp_count` NULL→0 / DF-start) are **closed in round-2** (A2 kernel +
`test_sql_door_regexp_count_null_is_null` /
`test_sql_door_regexp_instr_idx_is_ignored`). Cycle 3 CLEAN at round-1 tip.

Critic-4 (claims): attacked G1–G8/P1–P5/F1–F3 vs tree, 38-row sweep vs
remaining `lit_indices`, F2 Deferred lists, `str_to_map` STRUCK in
FN-GT2, mutation-table pointers. CL-IDENTITY N/A until this commit
(`%ae`/`%ce` set on the branch commit). Cycle 3 CLEAN.

Signature table: 19 GT1 names + 38 `lit_indices` sites checked against
live PySpark 4.1.2 (`inspect.signature` + probes). Sweep table: **14 FIX**
+ **24 PASS** (FIX includes `#6 str_to_map` and `#25 date_part`, live
ColumnOrName, not the GT1-wrapper subset). No leftover force-lit on a
live ColumnOrName param among the changed wrappers.

Oracle probes: signatures, NULL-in, exact Arrow types, `regexp_instr`
idx ignored (NULL-propagate), `getbit(x, 1)` projection,
`shiftrightunsigned(-2)` = 2^63-1, `unhex('C3')` invalid UTF-8,
direction pins, G5 stringify vs BINARY. Round-2 closed the SQL-door
DF leftovers (NULL-in NULL-out + idx ignore-value). Remaining named
residual: `F.bin(True)` / `rint` BOOLEAN over-accept (A4).

Pin audit: mutation-proof table in this ledger. Each named pin kills a
named revert (force-lit, getbit rename, idx-as-DF-start via `i99`,
NULL→0, unsigned-on-negative, invalid UTF-8, FN-B ColumnOrName,
`str_to_map` named delims, docstring `F.bin(13)`).

Convergence: **OCTO-CONVERGED** (N=3; cycle 3 all four Critics CLEAN;
early-stop after CLEAN findings half).

### Finder-battery report

Target: worktree `grok/c26-gt1-fix` vs origin/main post-#179 | dimensions: 5
(wiring/semantics, pins/tests, fence/contract, removed-behavior,
cross-file/caller) | rounds: 2 (R1 remediations then R2 quiet)

R1 raw: fence S1s (FN-A/B/F shipped notes, two-doors wording,
`regexp_count` 2–4 arity, math/factorial docs) — remediating before R2.
Wiring R1 hung (104 tools) — killed; R2 wiring completed.

R2 raw: 11 findings (all S2/S3) → 11 deduped. 3-vote on each.

Survivors (0 S0/S1; S2/S3 only, ranked most-severe first):
  [S2/CONFIRMED] width_bucket min/max Parameters omit the ColumnOrName
    sentence that `numBucket` has — `functions_math.py:135` — docs
    honesty, runtime already pinned.
  [S2/CONFIRMED] `str_to_map` extra “discrimination” row is same-valued
    — `test_functions_gt2.py:447` — named-column path still reds on
    force-lit revert; opposite `F.lit("pair")` is absent.
  [S2/PLAUSIBLE] `tests/map.md` GT1 blurb is F.*-scoped and does not
    name SQL-door residuals (SSOT is this ledger).
  [S2/PLAUSIBLE] `column/map.md` names `regexp_instr` idx fence, not
    `regexp_count` `need(2)` + NULL wrap (behavior pinned in tests).
  [S3/CONFIRMED] F3 execute test does not parse docstring source —
    sibling `test_gt1_docstring_examples_are_not_bare_literals` is the
    mutation lock.

Refuted (majority): extract/datepart G7 analog (out of 19 names,
pre-existing alias); date_part column-valued field (accepted residual);
G6 `idx=1` hollow-test (same test’s `i99` kills DF-start); startswith
wire name (FN-B pre-existing, out of charter); P2 types on NULL rows
(happy-path schema already pins). Mixed: unary P5 does not catch *this*
PR’s revert (unaries never force-lit arg0) but still kills a real
arg0-force-lit class.

Null attestations: removed-behavior — no leftover literal-intended
`F.*` callers; cross-file — remaining bare str are intentional
ColumnOrName pins; wiring — leftover force-lit on live ColumnOrName
among changed wrappers: none.

Verdict: **CLEAN** (zero S0/S1 survivors). Write paths not touched —
one quiet round after remediations satisfies loop-until-dry. S2/S3
left as report items (not a second Actor pass).

## Pre-PR critic report (/repark-harden) — Round-2

Engine: critic-octo N=3 (HIGH, claims_critic on) then finder-battery HIGH
(5 dims × 3-vote intent; R1 two dims hung and were retried tight; R2
quiet on wiring/pins/fence). Tier HIGH — engine kernels + 0.4.0 gate.

Critic-1 (quality/parity): C1 CLEAN at S1 (S2 src/map.md + door-level 🐈
pin remediating). C2 S2s remediating (F.split_part 0, UTF-16 rename,
map blurbs, int32 on UTF-16 test). C3 CLEAN.

Critic-2 (security/safety): C1–C3 CLEAN. Null-report: ReDoS (regex crate
linear), batch-scoped cache, no prod unwrap, CAST fail-loud, no secrets.

Critic-3 (logic): C1 S1 empty-pattern UTF-16 count remediating; S2 ASCII
`\d` + split_part 0 remediating. C2–C3 CLEAN. A1 idx ignore-value holds.

Critic-4 (claims): C1 stale residual prose + 14/24 sweep count remediating.
C2 S3 pointer remediating. C3 CLEAN. CL-IDENTITY N/A until this commit.

Signature table: 19-row F-5 table in this ledger (live inspect 4.1.2).
Oracle probes: A1 matrix, UTF-16 🐈, empty count, ASCII `\d`, decimal
5/40, ARRAY/STRUCT/MAP refuse, STRING partNum, partNum 0, CAST `'i'`,
`getbit` SQL name, omitted-idx `, 0`.

Pin audit: SQL NULL→NULL + idx 0/1/3/99→2 both doors; ARRAY refuse
SQL+F.*; decimal 5/40 + int32; split_part `'2'` + 0; UTF-16 + `\d`.

Convergence: **OCTO-CONVERGED** (N=3; cycle 3 all four CLEAN).

### Finder-battery report (round-2)

Target: worktree `grok/c26-gt1-fix` working tree vs face7c96 | dimensions: 5
(wiring, pins, fence, removed-behavior, cross-file) | rounds: 2

R1: 0 S0/S1. S2 coverage (SQL instr NULL, F.* array refuse, decimal
type) remediating. Java `.` on supplementary plane escalated (named
residual). Fence+cross-file hung → tight retry: one UDF, lib.rs 171/175.

R2: wiring / pins / fence **quiet**.

Verdict: **CLEAN** (zero S0/S1 survivors).

