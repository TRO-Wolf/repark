# Unit ledger — FN-REGEXP-EXTRACT-1 · Spark `regexp_extract(str, regexp[, idx])`

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FN-REGEXP-EXTRACT-1 merges, or when the owner closes the slate row.

**Unit:** FN-REGEXP-EXTRACT-1 · **Date:** 2026-09-04 · **Executor:** Muse Spark (muse-spark-1.3), Actor ·
**Branch:** `feat/fn-regexp-extract-1` · **Base:** `e3600a1`
**Model:** muse-spark-1.3
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on and off, 2026-09-04.
No Spark cell contradicts the sibling kernels' pinned behaviour; no HALT.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `SparkRegexpExtract` shares the translator and coercion of `regexp_extract_all`; registered in `string.rs` `functions()` and wrapped in `expr_fn.rs`; the facade refusal replaced on both doors. Nullability any-arg-nullable. Spark ANSI error text for a bad group index. | Kernel + dispatch + facade. | **PROVEN** |
| C-002 | Every oracle-table row pinned on both doors in `test_fn_regexp_extract.py`; the FN-FIX-2-CTRL-1 refusal pin flips only if that PR has merged, else left with a note; Rust unit tests beside the sibling kernels' tests; live cells co-collected on the shared `spark_engine`. | Pins + live tests. | **PROVEN** |
| C-003 | Every new pin red under one mutation (`N red of M`); `make verify` and the facade suite green. | Mutation table below. | **PROVEN** |
| C-004 | Registry §7 — the R-FN-BATCH1 disclosure loses `regexp_extract`, `FN-REGEX-POSIX-1` controls line updated; `docs/examples/backlog.txt` untouched; `STATUS.md` one line; ledger + `staging/map.md`; `map.md` lockstep in every touched directory. | Registry + maps. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-04, JDK 17, `TZ=UTC`)

| Row | Spark cell, ANSI on | Spark cell, ANSI off |
|---|---|---|
| basic groups | `regexp_extract('100-200', '(\\d+)-(\\d+)', 1)` → `'100'`; idx 2 → `'200'` | same |
| idx 0 / omitted | idx 0 → `'100-200'`; omitted idx → `'100'` (default 1) | same |
| no match / NULLs | no match → `''`; NULL str / regexp / idx → NULL | same |
| idx > groups / idx < 0, MATCHING input | `SparkRuntimeException [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX]` naming `` `regexp_extract` ``, `between 0 and 2`, SQLSTATE 22023 | same error, both cells (round-2 oracle `scratch/oracle_s1.py`) |
| idx > groups / idx < 0 / idx 1 of groupless pattern, NON-matching input | `''` for all three (`'abc'` idx 3, `'abc'` idx -1, `'ABC'`/`[a-z]+` idx 1); validation runs only inside `if (m.find())` | same `''` triple (round-2 oracle `scratch/oracle_s1.py`) |
| groupless pattern | idx 1 of `[a-z]+` → same condition, `between 0 and 0` | same |
| POSIX union | `'([[:alpha:]]+)'` on `'alpha'` → `'alpha'`; on `'fox'` → `''` | same |
| `\p{L}` | `'(\p{L}+)'` on `'alpha'` → `'alpha'` | same |
| Java-only lookbehind | `'(?<=foo)bar'` on `'foobar'` → `'bar'`; repark refuses (`invalid regular expression`), matching the sibling kernels | n/a (refusal, not ANSI-gated) |
| non-ASCII / empty | `'ünï' (\w+) idx 0` → `'n'`; `''` input → `''`; `''` pattern → `''`; idle group `(a)(b)?` idx 2 → `''` | same |
| doors | SQL door and `F.regexp_extract` (Column and str args) agree on every row above | same |

## Kernels

| Name | Layer |
|---|---|
| `regexp_extract` | `spark_regexp.rs::SparkRegexpExtract`; `invoke_extract` takes the FIRST match's group, `''` on no match or idle group; `validate_group_index` takes the caller's name and runs only inside the match arm (round 2: Spark validates `idx` only after `m.find()`) |
| widening | facade `functions_expr.py::regexp_extract` accepts a 2-arg call (`idx` defaults to 1); PySpark 4.1.2 requires `idx` (`TypeError` measured 2026-09-04); SQL door matches Spark exactly; source-compatible, recorded in the API freeze |
| dispatch | `string.rs::functions()` + `expr_fn::regexp_extract` + `function_dispatch.rs` arm |
| facade | `functions_expr.py::regexp_extract` (`Column \| str`, bare pattern forced-lit, `idx: int \| Column = 1`) |

## Mutation

| Knob | Red of M |
|---|---|
| disable `invoke_extract` (`exec_err!` first line) | 16 red of 16 (`test_fn_regexp_extract.py`, all items) |
| revert the round-2 move (`validate_group_index` before `regex.find`) | 3 red of 3 (`test_extract_nomatch_bad_index_returns_empty`, all params) |

## Next EX batch names

`F.regexp_extract`. Not added to examples in this unit.

## 9. Delivery

| Item | Path |
|---|---|
| Registry | `docs/spark-sql-iceberg-parity.md` FN-REGEX-POSIX-1 controls line; no §7 R-FN-BATCH1 row names `regexp_extract` (verified by grep — the disclosure lived in the refusal string, now removed, and the frozen census snapshot) |
| Live legs | `test_parity_live.py::test_live_fn_regexp_extract` on `spark_engine` |
| Maps | lockstep on every touched directory |
| Control-branch note | round-2 correction: FN-FIX-2-CTRL-1 merged as `9e1a057`; the flip to `test_regexp_extract_answers_on_both_doors` landed in merge `60ad77b0`; its docstring now cites `pins: fn-regexp-extract-1/C-002` |
| Round-2 filings | §7 `FN-REGEX-LOOKAROUND-1` (Spark `'bar'` vs repark refusal on every regexp kernel; pin `test_extract_java_lookbehind_is_loud`); facade 2-arg widening disclosed in the registry bullet and §Kernels; `spark_regexp.rs` at 999/1000 lines — the NEXT kernel must split the file (not split now) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fn-regexp-extract-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Live PySpark 4.1.2 cells recorded before code; pins assert Spark answers.
      artifacts: [python/repark/tests/test_fn_regexp_extract.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls cover non-ASCII, NULL-input, empty, idle group, POSIX, lookbehind refusal.
      artifacts: [python/repark/tests/test_fn_regexp_extract.py]
    - id: AT-3
      status: ATTACKED
      evidence: Out-of-range idx raises REGEX_GROUP_INDEX naming regexp_extract on matching input (ANSI on and off); non-matching input answers '' for any idx (round-2 oracle).
      artifacts: [crates/repark-functions/src/spark_regexp.rs]
    - id: AT-4
      status: N/A
      justification: Scalar string kernels have no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change.
      artifacts: [crates/repark-functions/src/spark_regexp.rs]
    - id: AT-6
      status: ATTACKED
      evidence: F.regexp_extract is _scalar onto the kernel; bare pattern forced-lit, idx default 1.
      artifacts: [python/repark/src/repark/spark/functions_expr.py]
    - id: AT-7
      status: ATTACKED
      evidence: Always-run pins are repark-only; Spark is behind REPARK_PARITY_LIVE=1.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-8
      status: ATTACKED
      evidence: spark_regexp.rs at 999/1000 lines (NEXT kernel must split); functions_expr.py held at exact baseline 2259.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cited in tests and maps; registry FN-REGEX-POSIX-1 controls line updated.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
```
