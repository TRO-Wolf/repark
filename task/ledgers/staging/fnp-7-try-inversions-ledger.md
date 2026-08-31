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
| C-001 | `try_divide` is the NULL-yielding inversion of `/`. Divide-by-zero and overflow (integer and decimal) yield NULL. Happy-path values and Arrow types match Spark 4.1.2 on all three doors. ANSI off vs on does not change `try_divide`. | Facade + Spark SQL + ANSI-door pins; red-first vs `hasattr(F, "try_divide")` False / `Invalid function 'try_divide'`. | **OPEN** |
| C-002 | `try_mod` is the NULL-yielding inversion of `%`. Modulo-by-zero and overflow yield NULL. Types and values match Spark 4.1.2 on all three doors. | Same three-door pins as C-001 for `%`. | **OPEN** |
| C-003 | `try_element_at` is the NULL-yielding inversion of `element_at` on the edges Spark maps to NULL. Array is 1-based; negative index counts from the end. Index 0, missing map key, and out-of-range are measured (raise vs NULL) and matched. NULL container yields NULL. | Facade + SQL pins including array 0 / negative / OOB and map missing-key. | **OPEN** |
| C-004 | `try_to_date` yields NULL on a parse failure. A NULL input is NULL, not a parse failure. A well-formed date matches `to_date`. Format-string cells that Spark still raises on match the error class. | Three-door pins: good string, malformed, NULL in, optional format. | **OPEN** |
| C-005 | `try_to_number` yields NULL on a value/format mismatch. A malformed format string itself matches Spark's error class (not silently NULL). | Measured format cells; three-door pins. | **OPEN** |
| C-006 | `try_to_binary` yields NULL on a decode failure. Supported format tokens match Spark (`hex` / `utf-8` / `utf8` / `base64` as measured). A bad format token matches Spark's error class. | Three-door pins including hex and invalid hex. | **OPEN** |
| C-007 | `try_to_time` yields NULL on a parse failure. NULL input is NULL. Well-formed TIME matches Spark's TIME/Arrow type. | Three-door pins: good, malformed, NULL in. | **OPEN** |
| C-008 | `try_sum` is Spark-equal on values, Arrow type, empty-group NULL, and accumulator overflow → NULL (not a partial sum). Reuse the `datafusion-spark` kernel when it matches; own it when it does not. Sliding-window retract stays the existing W-0 refusal. | Facade + SQL pins; overflow cell; reuse-vs-own recorded in this ledger. | **OPEN** |
| C-009 | `try_add` yields NULL on int32/int64 overflow under ANSI; happy-path `+` is unchanged. ANSI off vs on does not change `try_add` (always NULL-on-overflow). | Three-door pins at `INT_MAX + 1` and `LONG_MAX + 1`. | **OPEN** |
| C-010 | `try_subtract` yields NULL on int32/int64 overflow. Happy-path `-` is unchanged. | Three-door pins at `INT_MIN - 1` / `LONG_MIN - 1`. | **OPEN** |
| C-011 | `try_multiply` yields NULL on int32/int64 overflow. Happy-path `*` is unchanged. | Three-door pins at `INT_MAX * 2` / `LONG_MAX * 2`. | **OPEN** |
| C-012 | `try_avg` yields NULL on accumulator overflow and is Spark-equal on happy-path mean, empty-group NULL, and Arrow type. | Facade + SQL pins including overflow. | **OPEN** |
| C-013 | One kernel per Spark name, both doors. Python does not compute rows. The FNP-15/16 `armed_names()` roster stays 62; `execute_refuses_every_armed_declared_name` stays green. Facade `F.try_*` lands for the twelve names. | Registry tests; facade `hasattr`; the armed-roster pin. | **OPEN** |
| C-014 | SMALLINT/Int16 `try_add` / `try_subtract` / `try_multiply` is measured against Spark 4.1.2. RePark matches that cell or refuses loud. The F-Y10-1 wrap residue is not "fixed" here. | Oracle cell in this ledger; a pin or a loud refusal pin. | **OPEN** |
| C-015 | Interval arithmetic under `try_add` / `try_subtract` is measured. Spark-equal, or a loud refusal naming the gap. | Oracle cell; pin or refusal. | **OPEN** |
| C-016 | Docs and maps stay in lockstep. File-size ceilings ratchet down only. `functions.py` does not raise its 1985 baseline. New Rust files stay at or under the 1000-line default. | `make check-map-sync`; `check_lib_py` / `check_rust_file_size` green. | **OPEN** |
| C-017 | Gates before done: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base bb7fa54af48632c52d28aa8f7f446fac1dbf3742`, full `make py-test`, `make py-test-facade` for facade tests added. Real exit codes. | Recorded at close. | **OPEN** |

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

Cells land in the next commit. Hand-computed expectations are not an oracle.
