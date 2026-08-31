# Unit ledger — FNP-7a/7b · the twelve try_* inversions

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when FNP-7a/7b merges, or when
the owner closes the slate row.

**Unit:** FNP-7a + FNP-7b · **Date:** 2026-08-31 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/fnp-7-try-inversions` · **Base:** `bb7fa54af48632c52d28aa8f7f446fac1dbf3742`
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) C-004 (kernels both
doors) and design §3.5 / §7 rows FNP-7a and FNP-7b.
**Design:** [docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md)
§7 FNP-7a (8 names) and FNP-7b (4 names, unblocked by F-Y10-1 2026-08-30).
**Raising-path predecessor:** [f-y10-1-int-overflow-ledger.md](../completed/f-y10-1-int-overflow-ledger.md).
**Registry / armed-name pattern:** [fnp-4c-higher-order-kernels-ledger.md](../completed/fnp-4c-higher-order-kernels-ledger.md)
and [fnp-15-16-ledger.md](../completed/fnp-15-16-ledger.md).
**Slate:** [briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).

**Rubric:** STANDARD (public facade interface; new kernels; quantified three-door
claim). Floor S1. `risk_tier: standard`.

**Writable paths:** `crates/repark-functions` (new try_* kernels + registration);
`crates/repark-python/src/column/function_dispatch.rs` (facade dispatch);
facade wrappers (install_into so `functions.py` stays at its 1985 baseline);
Rust and facade pins; maps in lockstep; this ledger; STATUS under its ceiling.
Closed: `Cargo.toml [patch]`, lockfiles, `.github/`, `briefs/next-sequence.md`,
DML-A/B/C, MAINT, V3-*, W-0, FNP-4b dialect / write-path quoting, the
SMALLINT/Int16 wrap residue of F-Y10-1 (measure and record; match Spark's
try_* there or refuse loud — do not "fix wrap").

These twelve names are **absent** from the facade (`hasattr` False except
`try_to_timestamp`, which is a separate stub) and are **not** members of the
FNP-15/16 `armed_names()` roster of 62. That roster stays 62.
`execute_refuses_every_armed_declared_name` stays green because the armed set
does not gain or lose these names. `try_sum` already exists as a
`datafusion-spark` aggregate; this unit evaluates reuse vs own against the
live oracle.

## Names (design §7)

| Spark name | Slice | Raising path today |
|---|---|---|
| `try_divide` | 7a | ANSI `/0` raises `DIVIDE_BY_ZERO` |
| `try_mod` | 7a | ANSI `% 0` raises `DIVIDE_BY_ZERO` |
| `try_element_at` | 7a | `element_at` index 0 raises; ANSI out-of-range / missing key is a recorded divergence (NULL today) |
| `try_to_date` | 7a | `to_date` parse raises |
| `try_to_number` | 7a | parse / format mismatch raises (FNP-12 owns the non-try spelling) |
| `try_to_binary` | 7a | parse / format mismatch raises (FNP-12 owns the non-try spelling) |
| `try_to_time` | 7a | parse raises |
| `try_sum` | 7a | `datafusion-spark` kernel already registered; overflow of `sum` raises under ANSI |
| `try_add` | 7b | int32/int64 `+` raises `ARITHMETIC_OVERFLOW` under ANSI |
| `try_subtract` | 7b | int32/int64 `-` raises `ARITHMETIC_OVERFLOW` under ANSI |
| `try_multiply` | 7b | int32/int64 `*` raises `ARITHMETIC_OVERFLOW` under ANSI |
| `try_avg` | 7b | accumulator overflow of `avg` |

Acceptance bar per name: three-door Spark-equal (Spark SQL door, ANSI door
where reachable, facade Column API) on values, Arrow types, and the named
nasty edges versus live PySpark 4.1.2. Measure every cell first. Where Spark
still raises (for example a malformed format string), match the error class.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `try_divide` is the NULL-yielding inversion of `/`. Divide-by-zero yields NULL (including float 1.0/0.0). Happy-path values and Arrow type are Float64. Spark SQL + facade. ANSI-door SQL does not load the Spark UDF registry (SparkExtension). | Facade + Spark SQL pins. | **PROVEN** |
| C-002 | `try_mod` is the NULL-yielding inversion of `%`. Modulo-by-zero yields NULL. INT stays Int32. | Facade + Spark SQL pins. | **PROVEN** |
| C-003 | `try_element_at`: 1-based; negative from end; OOB NULL; map miss NULL; index 0 raises `INVALID_INDEX_OF_ZERO` (ANSI off too). | Facade + SQL pins. | **PROVEN** |
| C-004 | `try_to_date` yields NULL on parse failure and NULL input. Well-formed ISO and `dd/MM/yyyy` match. Illegal pattern raises `INVALID_DATETIME_PATTERN`. | Facade + SQL pins. | **PROVEN** |
| C-005 | `try_to_number` yields NULL on value/format mismatch. Malformed format raises `INVALID_FORMAT`. `'123'+'999'` is decimal(3,0); money format is decimal(8,2). | Facade + SQL pins. | **PROVEN** |
| C-006 | `try_to_binary` default format is hex (odd length left-pads 0). `utf-8`/`utf8`/`base64`/`hex` match. Invalid hex/base64/unknown format → NULL (Spark does not raise on a bad format token). | Facade + SQL pins. | **PROVEN** |
| C-007 | Live Spark 4.1.2 `try_to_time` raises `UNSUPPORTED_TIME_TYPE` (TIME is not enabled on this oracle). RePark matches that class, not a parse-to-TIME kernel. | Facade + SQL pins. | **PROVEN** |
| C-008 | `try_sum` reuses the datafusion-spark kernel. Int sum is bigint; overflow of long max+1 is NULL. Sliding-window retract stays the W-0 refusal. | Facade + SQL pins. | **PROVEN** |
| C-009 | `try_add` yields NULL on int32 overflow. Always NULL-on-overflow (independent of ANSI). | Facade + SQL pins. | **PROVEN** |
| C-010 | `try_subtract` yields NULL on int32 `INT_MIN - 1`. | SQL pin. | **PROVEN** |
| C-011 | `try_multiply` yields NULL on int32 `INT_MAX * 2`. | SQL pin. | **PROVEN** |
| C-012 | `try_avg` aliases RePark `avg`. Mean of 1,2,3 is double 2.0. Long overflow of avg is a double mean (Spark-equal; not NULL). | Facade + SQL pins. | **PROVEN** |
| C-013 | One kernel per Spark name on the Spark door and facade. Python does not compute rows. `armed_names()` stays 62. Facade `F.try_*` lands for the twelve names. Native ANSI `repark.sql()` does not install SparkExtension, so Spark `try_*` names are not reachable there. | Identity test; hasattr pin. | **PROVEN** |
| C-014 | Spark `try_add(SMALLINT 32767, 1)` is NULL smallint. RePark matches Int16 NULL. F-Y10-1 wrap residue of `+` is untouched. | SQL pin. | **PROVEN** |
| C-015 | Spark `try_add(INTERVAL 1 DAY, INTERVAL 1 DAY)` is 2 days. RePark IntervalMonthDayNano add matches the non-null happy path. | SQL pin. | **PROVEN** |
| C-016 | Docs and maps stay in lockstep. `functions.py` stays 1985. New Rust files under 1000. | `check-map-sync`; `check_lib_py` / `check_rust_file_size`. | **PROVEN** |
| C-017 | Gates before done: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742`, full `make py-test`, `make py-test-facade` for facade tests added. Real exit codes. | Recorded at close. | **PROVEN** |

## Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Measure Spark 4.1.2 oracle cells (values, Arrow types, NULL/empty, errors).
3. 7a numeric: `try_divide`, `try_mod`, `try_sum`.
4. 7a parse/element: `try_element_at`, `try_to_date`, `try_to_number`, `try_to_binary`, `try_to_time`.
5. 7b: `try_add`, `try_subtract`, `try_multiply`, `try_avg` + C-014 / C-015.
6. Facade wrappers, `__all__` install, pins, registry retirement of the
   absent-name fixtures.
7. Gates. Ledger verdicts flip when the pins exist.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-fnp-7-charter
  agent: Actor
  action: File the FNP-7a/7b staging ledger and lockstep maps, no kernel code yet
  charter_trace: FNP-0 C-004; this unit C-001..C-017
  preconditions:
    - AGENTS.md read path and design §7 FNP-7a/7b: SATISFIED (docs/design/spark-function-parity.md:423-424)
    - F-Y10-1 delivered, FNP-7b unblocked: SATISFIED (task/ledgers/completed/f-y10-1-int-overflow-ledger.md)
    - FNP-4c delivered, next in order is FNP-7a/7b: SATISFIED (STATUS.md Active workstreams)
    - Branch is feat/fnp-7-try-inversions at bb7fa54: SATISFIED (git)
    - Disk headroom: SATISFIED (571 G free of 1.8 T)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Chartering to_number as a full FNP-12 kernel: HANDLED(C-005/C-006 implement try_* only; non-try spellings stay FNP-12)
    - Growing functions.py ceiling: HANDLED(C-016; install_into pattern)
    - Touching F-Y10-1 Int16 wrap as a "fix": HANDLED(C-014 measure and match or refuse; out of scope to retype wrap)
  contingencies:
    - Revert this commit if grammar-red: EXECUTABLE(additive git revert)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Disk (AGENTS.md "Resource discipline")

Checked 2026-08-31 at pickup: `/` 571 G free of 1.8 T (68% used). No worktree.
Incremental `target/` reuse. No `cargo clean`. Spark oracle at
the local `c26-oracle` tree (PySpark 4.1.2) is read-only measurement.

## Oracle

Live PySpark **4.1.2** via the local `c26-oracle` tree + Zulu 17 (2026-08-31).
Hand-computed expectations are not an oracle.

| Name | Cell | Spark 4.1.2 |
|---|---|---|
| `try_divide` | `1/0`, `int/0`, `1.0/0.0`, decimal/0 | NULL double (decimal/0 is NULL decimal) |
| `try_divide` | `6/2` ints | double `3.0` |
| `try_mod` | `7%0` | NULL int; raising `%` is `REMAINDER_BY_ZERO` |
| `try_element_at` | index 0 | `INVALID_INDEX_OF_ZERO` (try_ and ANSI off too) |
| `try_element_at` | OOB / map miss | NULL |
| `element_at` | OOB ANSI on | `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` (RePark still NULL; recorded divergence) |
| `try_to_date` | `'not-a-date'` | NULL date |
| `try_to_date` | bad pattern | `INVALID_DATETIME_PATTERN.ILLEGAL_CHARACTER` |
| `try_to_number` | `'abc','999'` | NULL decimal(3,0) |
| `try_to_number` | `'not-a-format'` | `INVALID_FORMAT.WRONG_NUM_DIGIT` |
| `try_to_binary` | default `'abc'` | hex, odd pad → `b'\n\xbc'` |
| `try_to_binary` | bad hex / bad fmt token | NULL (not raise) |
| `try_to_time` | any | `UNSUPPORTED_TIME_TYPE` |
| `try_sum` | 1+2+3 / empty / long overflow | bigint 6 / NULL / NULL |
| `try_avg` | 1+2+3 / long overflow | double 2.0 / double mean (not NULL) |
| `try_add` | INT_MAX+1 / SMALLINT 32767+1 | NULL int / NULL smallint |
| `try_add` | INTERVAL 1 DAY + 1 DAY | interval day, 2 days |
| ANSI off | `try_*` | still NULL; `+` wraps; `/0` NULL |

`try_sum` reuse: datafusion-spark kernel already registered; facade `F.try_sum` now dispatches it.
`try_avg` reuse: alias of RePark `avg` (integer/float mean is Float64).

## Execution record (2026-08-31)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make check-map-sync check-ledger-grammar` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742` | 0 |
| `make py-test` | 0 (472 passed) |
| `make py-test-facade` | 0 (4187 passed, 75 skipped) |

pins: fnp-7-try-inversions/C-017

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-fnp-7-gates
  agent: Actor
  action: Record C-017 exits and file the coverage attestation
  charter_trace: FNP-7 C-001..C-017
  preconditions:
    - Kernels and facade pins land: SATISFIED (test_fnp7_try_inversions.py)
    - make verify / map-sync / ledger-grammar / lifecycle / py-test / py-test-facade: SATISFIED
  success_condition: C-017 PROVEN with recorded exits; grammar accepts the attestation
  step_risks:
    - Attestation required once no OPEN remains: HANDLED(COVERAGE_ATTESTATION complete true)
  contingencies:
    - Revert this docs commit: EXECUTABLE
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: FNP-7
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Twelve try_* names on the Spark door and facade Column API match the 2026-08-31 PySpark 4.1.2 cells for values, Arrow types, and named error classes.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py, crates/repark-functions/src/try_invert/arith.rs, crates/repark-functions/src/try_invert/convert.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Divide-by-zero, remainder-by-zero, integer overflow including SMALLINT, parse failure, NULL input, OOB element_at, missing map key, and accumulator overflow are pinned.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py]
    - id: AT-3
      status: ATTACKED
      evidence: Index 0 still INVALID_INDEX_OF_ZERO; illegal datetime pattern INVALID_DATETIME_PATTERN; malformed number format INVALID_FORMAT; try_to_time UNSUPPORTED_TIME_TYPE.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py]
    - id: AT-4
      status: N/A
      justification: Kernels are batch-pure scalar/aggregate UDFs; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, secrets, or SQL built from user text.
    - id: AT-6
      status: ATTACKED
      evidence: try_sum reuses datafusion-spark; try_avg aliases RePark avg; try_element_at aliases element_at; both doors share register_all.
      artifacts: [crates/repark-functions/src/lib.rs, crates/repark-functions/src/aggregate.rs, crates/repark-functions/src/collection.rs]
    - id: AT-7
      status: N/A
      justification: No new resource claim; checked arithmetic is the existing integer/decimal path with NULL instead of raise.
    - id: AT-8
      status: ATTACKED
      evidence: Spark error classes preserved; try_to_number format grammar matches Spark INVALID_FORMAT structure.
      artifacts: [crates/repark-functions/src/try_invert/convert.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Failures name DIVIDE_BY_ZERO's try_divide needle, INVALID_INDEX_OF_ZERO, INVALID_FORMAT, UNSUPPORTED_TIME_TYPE.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py]
    - id: AT-10
      status: ATTACKED
      evidence: One pin per clause C-001..C-016; functions.py stays 1985; armed_names stays 62; try_avg leaves the W-0 absent roster.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py, python/repark-parity/bench/windows/roster.py]
  reattested: []
  complete: true
```

