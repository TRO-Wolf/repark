# Unit ledger — FNP-7a/7b · the twelve try_* inversions

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when FNP-7a/7b merges, or when
the owner closes the slate row.

**Unit:** FNP-7a + FNP-7b · **Date:** 2026-08-31 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/fnp-7-try-inversions` · **Base:** `bb7fa54af48632c52d28aa8f7f446fac1dbf3742`
**Charter:** [fnp-0-charter-ledger.md](../../staging/fnp-0-charter-ledger.md) C-004 (kernels both
doors) and design §3.5 / §7 rows FNP-7a and FNP-7b.
**Design:** [docs/design/spark-function-parity.md](../../../../docs/design/spark-function-parity.md)
§7 FNP-7a (8 names) and FNP-7b (4 names, unblocked by F-Y10-1 2026-08-30).
**Raising-path predecessor:** [f-y10-1-int-overflow-ledger.md](2026-08-31-f-y10-1-int-overflow-ledger.md).
**Registry / armed-name pattern:** [fnp-4c-higher-order-kernels-ledger.md](2026-08-31-fnp-4c-higher-order-kernels-ledger.md)
and [fnp-15-16-ledger.md](2026-08-30-fnp-15-16-ledger.md).
**Slate:** [briefs/spark-function-parity.md](../../../../briefs/spark-function-parity.md).

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

Acceptance bar per name: Spark-equal on the two reachable doors (Spark SQL +
facade Column API) on values, Arrow types, and the named nasty edges versus
live PySpark 4.1.2. Native ANSI `repark.sql()` does not load SparkExtension —
the twelve names are unresolved there (C-013). Measure every cell first. Where
Spark still raises (for example a malformed format string), match the error
class. `try_avg(INTERVAL)` is a dated FNP-11 loud refuse (C-018 / registry
BL-13), not a silent NULL.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `try_divide` is the NULL-yielding inversion of `/`. Divide-by-zero yields NULL (including float 1.0/0.0). Happy-path values and Arrow type are Float64. Spark SQL + facade. ANSI-door SQL does not load the Spark UDF registry (SparkExtension). | Facade + Spark SQL pins. | **PROVEN** |
| C-002 | `try_mod` is the NULL-yielding inversion of `%`. Modulo-by-zero yields NULL. INT stays Int32. | Facade + Spark SQL pins. | **PROVEN** |
| C-003 | `try_element_at`: 1-based; negative from end; OOB NULL; map miss NULL; index 0 raises `INVALID_INDEX_OF_ZERO` (ANSI off too). | Facade + SQL pins. | **PROVEN** |
| C-004 | `try_to_date` yields NULL on parse failure and NULL input. Well-formed ISO, `dd/MM/yyyy`, `yyyy` (defaults 01-01), `yyyy-MM`, `MMM dd yyyy`, `MMMM dd yyyy`, `yy`, `d/M/yyyy`, `yyyyMMdd` match Spark. Illegal pattern raises `INVALID_DATETIME_PATTERN`. | Facade + SQL pins including the format sweep. | **PROVEN** |
| C-005 | `try_to_number` yields NULL on value/format mismatch. Malformed format raises `INVALID_FORMAT`. `'123'+'999'` is decimal(3,0); money format is decimal(8,2). | Facade + SQL pins. | **PROVEN** |
| C-006 | `try_to_binary` default format is hex (odd length left-pads 0). `utf-8`/`utf8`/`base64`/`hex` match. Invalid hex/base64/unknown format → NULL (Spark does not raise on a bad format token). | Facade + SQL pins. | **PROVEN** |
| C-007 | Live Spark 4.1.2 `try_to_time` raises `UNSUPPORTED_TIME_TYPE` (TIME is not enabled on this oracle). RePark matches that class, not a parse-to-TIME kernel. | Facade + SQL pins. | **PROVEN** |
| C-008 | `try_sum` reuses the datafusion-spark kernel. Int sum is bigint; overflow of long max+1 is NULL. Sliding-window retract stays the W-0 refusal. | Facade + SQL pins. | **PROVEN** |
| C-009 | `try_add` yields NULL on int32 overflow. Always NULL-on-overflow (independent of ANSI). | Facade + SQL pins. | **PROVEN** |
| C-010 | `try_subtract` yields NULL on int32 `INT_MIN - 1`. | SQL pin. | **PROVEN** |
| C-011 | `try_multiply` yields NULL on int32 `INT_MAX * 2`. | SQL pin. | **PROVEN** |
| C-012 | `try_avg` is a distinct UDAF (`avg` still raises on decimal overflow). Mean of 1,2,3 is double 2.0. Long overflow of avg is a double mean (Spark-equal; not NULL). Decimal(38,0) max+max overflow is NULL decimal(38,4). | Facade + SQL pins including overflow cells. | **PROVEN** |
| C-013 | One kernel per Spark name on the Spark door and facade. Python does not compute rows. `armed_names()` stays 62. Facade `F.try_*` lands for the twelve names. Native ANSI `repark.sql()` does not install SparkExtension, so each of the twelve names is `Invalid function`. | hasattr pin + `repark.sql` unreachability pin. | **PROVEN** |
| C-014 | Spark `try_add(SMALLINT 32767, 1)` is NULL smallint. RePark matches Int16 NULL. F-Y10-1 wrap residue of `+` is untouched. | SQL pin. | **PROVEN** |
| C-015 | Spark `try_add(INTERVAL 1 DAY, INTERVAL 1 DAY)` is 2 days. DATE + INTERVAL 1 DAY is date 2024-01-02; TIMESTAMP + 1 HOUR is 01:00; DATE 2024-01-31 + 1 MONTH is 2024-02-29. DATE + INTERVAL 1 HOUR is timestamp 01:00; DATE + 25 HOUR is timestamp 2024-01-02 01:00. `try_divide(INTERVAL 2 DAYS, 2)` is 1 day; `/0` is NULL. | SQL pins with exact values and types. | **PROVEN** |
| C-016 | Docs and maps stay in lockstep. `functions.py` stays 1985. New Rust files under 1000. | `check-map-sync`; `check_lib_py` / `check_rust_file_size`. | **PROVEN** |
| C-017 | Gates before done: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742`, full `make py-test`, `make py-test-facade` for facade tests added. Real exit codes. | Recorded at close. | **PROVEN** |
| C-018 | `try_avg(INTERVAL)` refuses loud with `[FNP-11]` dated 2026-08-31. Spark 4.1.2 returns interval day to second. Deferred to FNP-11; registry BL-13. Silent NULL is not this cell. | SQL pin of the refusal wording. | **PROVEN** |
| C-019 | Spark `try_add` overflows the ANSI day-time Duration (i64 microseconds; max whole days 106751991) to NULL. 106751990+1 is 106751991 days; 106751991+1 is NULL. Same bound for the negative side. | SQL pins both sides of the bound. | **PROVEN** |

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
| `try_avg` | DECIMAL(38,0) max+max | NULL decimal(38,4). `avg` of the same RAISES `ARITHMETIC_OVERFLOW` (2026-08-31 re-measure) |
| `try_to_date` | `'2024','yyyy'` / `'Jan 15 2024','MMM dd yyyy'` / `'yyyy-MM'` / `'yy'` / `'d/M/yyyy'` / `'yyyyMMdd'` / `'MMMM dd yyyy'` | 2024-01-01 / 2024-01-15 / 2024-01-01 / 2024-01-01 / 2024-01-05 / 2024-01-15 / 2024-01-15 |
| `try_add` | DATE '2024-01-01' + INTERVAL 1 DAY | date 2024-01-02 |
| `try_add` | TIMESTAMP '2024-01-01 00:00:00' + INTERVAL 1 HOUR | timestamp 01:00 |
| `try_add` | DATE '2024-01-31' + INTERVAL 1 MONTH | date 2024-02-29 |
| `try_divide` | INTERVAL 2 DAYS / 2 ; / 0 | 1 day ; NULL interval |
| `try_avg` | INTERVAL 1 DAY | Spark: interval 1 day. RePark: loud `[FNP-11]` refuse (C-018 / BL-13) |
| `try_add` | INT_MAX+1 / SMALLINT 32767+1 | NULL int / NULL smallint |
| `try_add` | INTERVAL 1 DAY + 1 DAY | interval day, 2 days |
| ANSI off | `try_*` | still NULL; `+` wraps; `/0` NULL |

`try_sum` reuse: datafusion-spark kernel already registered; facade `F.try_sum` now dispatches it.
`try_avg` is a distinct UDAF (integer/float mean is Float64; decimal overflow NULL; INTERVAL refuses `[FNP-11]`).

## Execution record (2026-08-31)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make check-map-sync check-ledger-grammar` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742` | 0 |
| `make py-test` | 0 (472 passed) |
| `make py-test-facade` | 0 (4187 passed, 75 skipped) |

### Critic remediation gates (2026-08-31)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make check-map-sync` | 0 (163 maps) |
| `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742` | 0 |
| `make py-test` | 0 (472 passed) |
| `make py-test-facade` | 0 (4194 passed, 75 skipped) |
| targeted `test_fnp7_try_inversions.py` | 0 (28 passed) |

### Recritic 2 gates (2026-08-31)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make check-map-sync` | 0 (163 maps) |
| `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742` | 0 |
| `make py-test` | 0 (472 passed) |
| `make py-test-facade` | 0 (4196 passed, 75 skipped) |
| targeted `test_fnp7_try_inversions.py` | 0 (30 passed) |

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
      evidence: try_sum reuses datafusion-spark; try_avg is a distinct UDAF (null_on_overflow); try_element_at aliases element_at; Spark door register_all.
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
      evidence: One pin per clause C-001..C-019; functions.py stays 1985; armed_names stays 62; try_avg leaves the W-0 absent roster.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py, python/repark-parity/bench/windows/roster.py]
  reattested: []
  complete: true
```

## Critic remediation (2026-08-31)

Five S1 + three S2 from `/tmp/grok-worker/fnp7/critic-report.md`.

| Finding | Disposition |
|---|---|
| L-001 `try_avg` decimal overflow RAISES | **FIXED.** Distinct `try_avg` UDAF; `null_on_overflow` on the decimal accumulator. `avg` still raises. Pin `test_try_avg_decimal_overflow_is_null`. |
| L-002 DATE/TIMESTAMP ± INTERVAL PLAN_ERR | **FIXED.** Existing Arrow interval fields + chrono month/day/nanos. No new interval engine. |
| L-003 INTERVAL / numeric PLAN_ERR | **FIXED.** Divide days+nanos; `/0` is NULL. |
| L-004 `yyyy` / `MMM dd yyyy` silent NULL | **FIXED.** Java pattern scanner defaults missing month/day to 01; `MMM`/`MMMM` English months. Sweep: `yyyy-MM`, `yy`, `d/M/yyyy`, `yyyyMMdd`, `MMMM`. |
| L-005 `try_avg(INTERVAL)` PLAN_ERR | **LOUD REFUSE (doctrine b).** Exact message `[FNP-11] try_avg(INTERVAL) is deferred to the FNP-11 temporal family (2026-08-31). …` Registry BL-13. Pin C-018. Spark computes interval day to second; averaging intervals is FNP-11. Silent NULL rejected. |
| Q-001 hollow pins | **FIXED.** C-012 overflow + long-mean pins; C-015 exact 2 days + date/ts/interval-div; C-013 `repark.sql` `Invalid function` for all twelve names. |
| Q-002 red-first transcript | **Recorded below.** |
| CL-001 three-door overclaim | **FIXED.** tests/map.md names the two reachable doors + ANSI unreachability. |

### Red-first (sabotage → red → restore, 2026-08-31)

Each new assert was mutated, the named node run, then the file restored from a backup (not `git checkout`). File compared equal after the last restore; 28 pins green.

| Pin | Mutant | Exit | Needle |
|---|---|---|---|
| C-012 `test_try_avg_decimal_overflow_is_null` | `[None]` → `[0]` | 1 | `None != 0` |
| C-004 `test_try_to_date_java_formats_match_spark` | `yyyy` expected 2024-01-01 → 1999-01-01 | 1 | `2024` vs `1999` |
| C-013 `test_try_names_unresolved_on_ansi_sql_door` | match `Invalid function` → `definitely-not-this` | 1 | `Invalid function 'try_add'` |
| C-015 `test_try_add_interval_day` | days `2` → `99` | 1 | `MonthDayNano(months=0, days=2, …)` `2 == 99` |
| C-018 `test_try_avg_interval_refuses_fnp11` | match `[FNP-11]…2026-08-31` → `definitely-not-this` | 1 | `[FNP-11] try_avg(INTERVAL) is deferred to the FNP-11 temporal family (2026-08-31)` |

Logs: `/tmp/fnp7-sabotage-C-012.log` … `C-018.log`.

### Oracle re-measure (live PySpark 4.1.2, Zulu 17, 2026-08-31)

All L-001..L-004 and C-015 cells match Spark. L-005 Spark still returns `timedelta(days=1)` interval; RePark refuses `[FNP-11]` by design. `avg` decimal overflow still `ARITHMETIC_OVERFLOW`. Log: `/tmp/fnp7-oracle-remeasure.log`.

## Recritic 2 (2026-08-31)

Prior L-001..L-005 and Q-001/Q-002/CL-001 closed. New findings from
`/tmp/grok-worker/fnp7/recritic-report.md` at `942ad43`.

| Finding | Disposition |
|---|---|
| L-006 DATE + INTERVAL 1 HOUR stays Date32 | **FIXED.** Spark promotes DATE + HOUR/MINUTE/SECOND (MonthDayNano nanos ≠ 0, including 24 HOUR kept in nanos) to timestamp; DATE + DAY/MONTH (nanos = 0) stays date. 25 HOUR → 2024-01-02 01:00. Pin `test_try_add_date_plus_hour_promotes_to_timestamp`. |
| L-007 INTERVAL 106751991 DAY + 1 DAY computes | **FIXED.** Spark Duration is i64 microseconds; max whole days 106751991. `duration_micros` NULLs past that bound. 106751990+1 is 106751991. Pin `test_try_add_interval_duration_max_overflow_is_null`. |
| Q-004 hollow L-006/L-007 | **FIXED.** The two pins above. Red-first below. |
| CL-002 STATUS remaining-order date | **FIXED.** `Next, in order (revised 2026-08-31)` and the PLAN-1 start marker. Order tokens unchanged. |

### Red-first (L-006 / L-007)

| Pin | Mutant | Exit | Needle |
|---|---|---|---|
| C-015 `test_try_add_date_plus_hour_promotes_to_timestamp` | type `timestamp` → `date32` | 1 | `'date32' in 'timestamp[us, tz=utc]'` (value `2024-01-01 01:00:00`) |
| C-019 overflow half | `[None]` → `[0]` | 1 | `None != 0` |
| C-019 inside half | days `106751991` → `0` | 1 | `106751991 == 0` |

Logs: `/tmp/fnp7-sabotage-C-015-hour.log`, `/tmp/fnp7-sabotage-C-019-overflow.log`, `/tmp/fnp7-sabotage-C-019-inside.log`.

### Oracle (L-006 / L-007, live 4.1.2)

DATE+1 HOUR timestamp 01:00; DATE+25 HOUR timestamp 2024-01-02 01:00; DATE+24 HOUR timestamp 2024-01-02 00:00 (unit is HOUR, not value-normalized to DATE); DATE+1 DAY stays date; DATE+1 MONTH stays date. INTERVAL 106751991+1 DAY NULL; 106751990+1 = 106751991 days; 106751991+1 HOUR still fits (remainder micros). Log: `/tmp/fnp7-l006-oracle.log`.

## 12. Final Critic pass (recritic2) — below-floor residuals into the record

The final fresh Critic pass at `73deca8` CONVERGED (no S0/S1; report
`/tmp/grok-worker/fnp7/recritic2-report.md`). Its three below-floor residuals land here:

| Finding | Disposition |
|---|---|
| L-008 (S2) DATE + INTERVAL 0 HOUR stays Date32; Spark types it timestamp midnight | **RECORDED**, not fixed: registry row **BL-14** (the `{0,0,0}` MonthDayNano value is unit-blind; a fix needs literal-unit retention upstream). Pin `test_try_add_date_plus_zero_hour_stays_date_bl14` asserts the current Date32 so a silent change is loud. Calendar day equal on both engines; the divergence is type + midnight clock. |
| Q-005 (S2) DATE + 24 HOUR promotion unpinned | **FIXED.** The 24 HOUR cell (timestamp 2024-01-02 00:00, type-asserted) joins `test_try_add_date_plus_hour_promotes_to_timestamp`. |
| CL-003 (S3) C-019 "both sides" pinned only positive | **FIXED** by making the claim true: negative-bound cells pinned in `test_try_add_interval_duration_max_overflow_is_null` (−106751991 + −1 DAY → NULL; −106751990 + −1 → −106751991 days), measured before pinning. |

### Red-first (final pass)

| Pin | Mutant | Exit | Needle |
|---|---|---|---|
| Q-005 24 HOUR cell | `hour == 0` → `hour == 1` | 1 | `test_try_add_date_plus_hour_promotes_to_timestamp` FAILED |
| C-019 negative half | `[None]` → `[0]` | 1 | `test_try_add_interval_duration_max_overflow_is_null` FAILED |
| BL-14 type assert | `date` → `timestamp` | 1 | `test_try_add_date_plus_zero_hour_stays_date_bl14` FAILED |

Logs: `/tmp/fnp7-sabotage-Q005-24hour.log`, `/tmp/fnp7-sabotage-C019-negative.log`,
`/tmp/fnp7-sabotage-BL14-zerohour.log`. All three restored; file suite 31 passed.

### Oracle (final pass, engine probe 2026-08-31)

`try_add(DATE, INTERVAL 24 HOUR)` → timestamp[us, UTC] 2024-01-02 00:00;
`try_add(INTERVAL −106751991 DAY, INTERVAL −1 DAY)` → NULL;
`try_add(INTERVAL −106751990 DAY, INTERVAL −1 DAY)` → −106751991 days;
`try_add(DATE, INTERVAL 0 HOUR)` → date32 2024-01-01 (BL-14; Spark: timestamp midnight per
the recritic2 live-oracle cell).

