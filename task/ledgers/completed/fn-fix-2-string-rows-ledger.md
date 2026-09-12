# Unit ledger — FN-FIX-2 · six silent string divergences become Spark-equal

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FN-FIX-2 merges, or when the owner closes the slate row.

**Unit:** FN-FIX-2 · **Date:** 2026-09-04 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/fn-fix-2-string-rows` · **Base:** `8cb965f`
**Model:** grok-4.6
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on, 2026-09-04.
Registry cells matched; no HALT.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | One live-oracle script per row (cells plus non-NULL / NULL-input / empty controls) recorded before code. A Spark cell contradicting a registry row → HALT. | Oracle table below. | **PROVEN** |
| C-002 | Smallest change at the owning layer. No new dependency. No unnamed functions. Files under size ceilings. | Kernels + dispatch + facade. | **PROVEN** |
| C-003 | Each row's pin RED after the fix; rewritten to Spark's answer; controls; two live co-collected legs; mutations one knob at a time. | Pins + live tests + mutation table. | **PROVEN** |
| C-004 | Every row **FIXED 2026-09-04 (FN-FIX-2)** per §6; next EX names in this ledger; maps lockstep. | Registry + maps. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-04, JDK 17, ANSI on, `TZ=UTC`)

| Row | Spark cell | repark before |
|---|---|---|
| FN-INITCAP-1 | `'a-b'`→`'A-b'`; `'foo.bar'`→`'Foo.bar'`; `"o'neil"`→`"O'neil"`; `'x\ty'`→`'X\ty'`; `'  leading'`→`'  Leading'`; NULL→NULL; `''`→`''` | `'A-B'` / `'Foo.Bar'` / `"O'Neil"` / `'X\tY'` |
| FN-CHR-1 | `[256, 300, 321, 65601, -1]` → `['\x00', ',', 'A', 'A', '']`; NULL→NULL | `'Ĭ'` at 300; `-1` raises |
| FN-TRIM-CHARS-1 | `F.trim('xxSparkxx', 'x')` → `'Spark'`; `ltrim` → `'Sparkxx'`; `rtrim` → `'xxSpark'`; one-arg whitespace kept | TypeError two-arg |
| FN-ELT-1 | n=1 `'a'`; n=2 `'b'`; n=NULL NULL; n=3/0/−1 `INVALID_ARRAY_INDEX` SQLSTATE 22003; ANSI off → NULL | n=3/0 → NULL |
| FN-REGEX-POSIX-1 | `[[:alpha:]]` on `['a1b2 Ünï_9','foo','aabbaa']`: count `[1,0,4]`, rlike `[True,False,True]`, replace `['#1b2 Ünï_9','foo','##bb##']`; extract group 0 `['a','','a']` | count `[3,3,6]`; rlike all True |
| FN-LIKE-ESCEND-1 | `like('ab','ab\\')` raises `[INVALID_FORMAT.ESC_AT_THE_END]` SQLSTATE 42601; control `like('a\\b','a\\\\b')` True | False |

## Kernels

| Name | Layer |
|---|---|
| `initcap` | `spark_initcap.rs`; word break is SPACE only |
| `chr` / `char` | `spark_chr.rs`; `n % 256`; negative → `''` |
| `trim` / `ltrim` / `rtrim` | facade optional charset; DataFusion `btrim`/`ltrim`/`rtrim` |
| `elt` | `spark_elt.rs`; ANSI `INVALID_ARRAY_INDEX`; NULL n → NULL |
| POSIX class | `java_regex.rs` nested-class union before `regex` crate |
| `regexp_like` / `rlike` / `regexp_replace` | `spark_regexp_match.rs` via `compile_spark_regex` |
| LIKE escape-at-end | `analyzer/like_escape.rs` Plan error on foldable dangling escape |

## Mutation

| Knob | Red of M |
|---|---|
| restore DataFusion `initcap` dispatch | 1 red of 1 (`test_fn_initcap_starts_word_only_after_space`) |
| restore Unicode-scalar `chr` | 1 red of 1 (`test_fn_chr_modulo_256_and_negative_empty`) |
| drop two-arg `trim` | 1 red of 1 (`test_fn_trim_two_arg_charset`) |
| restore `array_element` elt | 2 red of 2 (index 3 and 0 raise pins) |
| skip Java nested-class translate | 2 red of 2 (`test_regexp_count_posix_alpha_is_java_union`, rlike pin) |
| skip LIKE escape-at-end refuse | 1 red of 2 (`test_like_pattern_ending_in_escape_raises`) |

## Next EX batch names

`F.initcap`, `F.chr`, `F.char`, `F.trim` (two-arg), `F.ltrim` (two-arg), `F.rtrim` (two-arg),
`F.elt`, `F.regexp_count` (`[[:alpha:]]`), `F.rlike` (`[[:alpha:]]`), `F.regexp_replace`
(`[[:alpha:]]`), `F.like` (escape-at-end). Not added to examples in this unit.

EX-4/EX-5 dropped names still on the example backlog: `F.initcap`, `F.chr`, `F.char`,
`F.elt`, plus `F.base64`, `F.encode`, `F.decode`, `F.format_number`, `F.split`,
`F.regexp_extract`, `F.sentences`, `F.validate_utf8`, `F.replace`.

## 9. Delivery

| Item | Path |
|---|---|
| Registry | `docs/spark-sql-iceberg-parity.md` FIXED 2026-09-04 (FN-FIX-2) |
| Live legs | `test_parity_live.py` two tests on `spark_engine` |
| Maps | lockstep on every touched directory |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fn-fix-2-string-rows
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Live PySpark 4.1.2 cells recorded before code; pins assert Spark answers.
      artifacts: [python/repark/tests/test_fn_initcap_divergence.py, python/repark/tests/test_fn_chr_divergence.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls cover non-ASCII, NULL-input, empty, negative chr, ANSI elt, LIKE control.
      artifacts: [python/repark/tests/test_fn_trim_chars.py, python/repark/tests/test_fn_elt_out_of_range.py]
    - id: AT-3
      status: ATTACKED
      evidence: Out-of-range elt raises INVALID_ARRAY_INDEX; LIKE dangling escape raises ESC_AT_THE_END.
      artifacts: [python/repark/tests/test_fn_like_escape_end.py, crates/repark-functions/src/spark_elt.rs]
    - id: AT-4
      status: N/A
      justification: Scalar string kernels have no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change.
      artifacts: [crates/repark-functions/src/spark_initcap.rs]
    - id: AT-6
      status: ATTACKED
      evidence: F.trim/ltrim/rtrim gain an optional charset argument matching PySpark.
      artifacts: [python/repark/src/repark/spark/functions_expr.py]
    - id: AT-7
      status: ATTACKED
      evidence: Always-run pins are repark-only; Spark is behind REPARK_PARITY_LIVE=1.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-8
      status: ATTACKED
      evidence: analyzer.rs ratcheted 1161→1142; functions_expr.py stayed 2261; no ceiling raised.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cited in tests and maps; registry rows FIXED 2026-09-04 (FN-FIX-2).
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
```
