# Unit ledger — SEM-1 · Spark-answer parity for RE-1 and LOG-1

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when SEM-1 merges, or when the
owner closes the slate row.

**Unit:** SEM-1 · **Date:** 2026-08-31 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/sem-1-spark-answer-parity` · **Base:** `be2d754066e4ab3a5d61b4ec32418a10b8a31804`
**Charter:** [sem-0-charter-ledger.md](../../staging/sem-0-charter-ledger.md) (measured scope).
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
`RE-1` (already retired by the 2026-08-21 SEM-1) and `LOG-1`.
**risk_tier:** standard.

**Owner ruling, 2026-08-31:** both RE-1 and LOG-1 are fixed to Spark semantics.
SEM-0 was held at its approval gate for this dated ruling; it is now given.
The 2026-08-21 ruling that tabled LOG-1 is superseded for LOG-1 only. RE-1's
value change was already authorized 2026-08-21 and delivered as
`task/ledgers/archive/2026-08/2026-08-21-sem-1-extract-all-group-default-ledger.md`
(PR #193). This unit re-measures that default against live PySpark 4.1.2, then
closes LOG-1.

This unit changes what a working query returns on the Spark door.

## Scope (charter, followed exactly)

1. **RE-1** — `regexp_extract_all` two-argument default is capture group 1.
   One site: `extract_rows` `None` arm in `crates/repark-functions/src/spark_regexp.rs`.
   On this tree that arm is already `None => 1`. Collateral the charter named:
   the RE-1 pin, the `[0-9]*` stepping test, the empty-pattern critic test
   (four parametrizations). Adjacent string-`idx` defect is SEM-3 (already
   delivered). `REGEX_GROUP_INDEX` wording is SEM-4 (already delivered).
2. **LOG-1** — Spark-door `log` is natural log, dual-arity, Spark null-guard
   on both arities. New `ScalarUDFImpl` (`SparkLog`), not a redirect to `ln`.
   Ratchet: drop `log` from `EXPECTED_DIVERGENCES`, `len()` 24 → 23. Point the
   facade `"log"` arm at the same kernel. Land `F.log(base, expr)`. Native
   ANSI `repark.sql()` `log` stays base 10 (ADR-0002; out).

Oracle: live PySpark 4.1.2. Measure every assertion, including incidental
controls, before flipping a pin. HALT on unmeasured collateral.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Owner ruling 2026-08-31 authorizes both value changes to Spark semantics. | Dated ruling in this ledger and in the LOG-1 registry row. | **PROVEN** |
| C-002 | RE-1 default is capture group 1 on both Spark doors. Explicit `idx=0` still returns the whole match. A pattern with no groups raises `REGEX_GROUP_INDEX` on the two-argument form. Null input propagates. `regexp_substr` is untouched. | Live-oracle table plus facade and Spark SQL pins. | **PROVEN** |
| C-003 | The three charter collateral sites match Spark after the default. No unnamed `regexp_extract_all` call site in `python/repark/tests/` goes red. | Named tests plus a call-site grep recorded here. | **PROVEN** |
| C-004 | Spark-door `log(expr)` is the natural log. `log(base, expr)` is log at that base. Both arities return NULL on the Spark domain edges (zero, negative, null, base <= 0) measured on live PySpark 4.1.2. Base 1 is IEEE (`inf` / `nan`), not NULL. Not a redirect of one-arg `log` to `ln`. | Kernel plus Spark SQL and facade pins, value AND Arrow type. | **PROVEN** |
| C-005 | `EXPECTED_DIVERGENCES` drops its `log` row in the same commit as the kernel. `len()` moves 24 → 23 with the reason written there. | `door_parity_tests.rs`. | **PROVEN** |
| C-006 | Facade `"log"` arm embeds the same `SparkLog` instance the Spark SQL door registers. `F.log` accepts PySpark's `log(arg1, arg2=None)`. `F.ln` stays `ln`. | Kernel-identity pin plus two-arg facade pin. | **PROVEN** |
| C-007 | Native ANSI `repark.sql()` `log` stays DataFusion base-10. This unit does not register `SparkLog` on the extension-less session. | Native-door pin of `log(8)`. | **PROVEN** |
| C-008 | Registry `LOG-1` moves to FIXED with the 2026-08-31 ruling date and the measured evidence. Prior LOG-1 pins that asserted the divergent answer flip red-first to the Spark answer. `RE-1` stays retired (already gone). STATUS / docs truth-up only for what this unit changed. | Registry row + flipped pins + STATUS SEM / `F.log` lines. | **PROVEN** |
| C-009 | Gates before done: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base be2d754066e4ab3a5d61b4ec32418a10b8a31804`, `make py-test`, and `make py-test-facade` because the facade is touched. Real exit codes. | All exit 0 on 2026-09-01: `make develop` (wheel rebuilt with `SparkLog`), `make py-test-facade` (4209 passed, 75 skipped), `make py-test` (472 passed), `make verify` (47 suites ok, `spark_log` tests 5/5), `make check-map-sync check-ledger-grammar` (163 maps clean; 7 live ledgers clean), `ledger_lifecycle.py check --base be2d754` (173 ledgers in bins, 600 links resolve). | **PROVEN** |
| C-010 | Live PySpark 4.1.2 measurements (RE-1 edges, LOG-1 both arities, both reachable Spark doors, native-door control, incidental `log2`/`log1p`/`ln`) are transcribed here before any pin flip. | Oracle transcript in this ledger. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-08-31, JDK 17)

RE-1 — already Spark-equal on this tree (`extract_rows` `None => 1`). Measured:

| Call | Spark |
|---|---|
| `regexp_extract_all('a1b2', '([a-z])([0-9])')` | `['a','b']` |
| same, idx 0 / 1 / 2 | `['a1','b2']` / `['a','b']` / `['1','2']` |
| `'[a-z]([0-9])'` two-arg | `['1','2']` |
| `'[0-9]*'` / `''` / `'b'` two-arg | `REGEX_GROUP_INDEX` … `between 0 and 0, but got 1` |
| null str / null pattern / null idx | NULL |
| `regexp_substr('a1b2', pairs)` | `'a1'` |
| `F.regexp_extract_all` two-arg | `['a','b']` |

LOG-1 — Spark SQL, Arrow type `double` throughout:

| Call | Spark |
|---|---|
| `log(8)` / `ln(8)` | `2.0794415416798357` |
| `log10(8)` | `0.9030899869919435` |
| `log(2, 8)` / `log(8, 8)` / `log(0.5, 8)` | `3.0` / `1.0` / `-3.0` |
| `log(0)`, `log(-1)`, `log(null)` | NULL |
| `log(0, 8)`, `log(-2, 8)`, `log(10, 0)`, `log(10, -1)` | NULL |
| `log(1, 8)` | `inf` |
| `log(1, 1)` | `nan` |
| `-Infinity` one-arg | NULL |
| `F.log(2.0, col)` on `[8,0,-1]` | `[3.0, None, None]` |
| `F.log2` / `F.ln` / `F.log1p` on the same | `[3.0, None, None]` / `[2.079…, None, None]` / `[log1p(8), 0.0, None]` |

`F.log(Column, Column)` is a PySpark binding miss (`Column is not iterable`); SQL two-arg columns work. RePark's `_scalar` accepts both as columns. Recorded, not a HALT: the charter scoped `F.log(2.0, col)`, which Spark runs.

Composition collateral (`log2`/`log1p`/`ln` domain NULL) matches Spark after the kernel. Not unnamed divergence.

## Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. Measure live PySpark 4.1.2. Record C-010. HALT on unnamed collateral.
3. RE-1: re-confirm the default and incidental controls; flip remaining
   divergent pins only if any still assert group 0.
4. LOG-1: `SparkLog` kernel, register_all overwrite, facade arm, `F.log`
   two-arg, ratchet, red-first pin flip.
5. Registry FIXED + STATUS truth-up. Gates. Verdicts flip when pins exist.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-sem-1-ledger
  agent: Actor
  action: File the SEM-1 staging ledger and lockstep staging/map.md, no kernel code yet
  charter_trace: SEM-0 SEM-1 and SEM-2 scope; owner ruling 2026-08-31
  preconditions:
    - AGENTS.md read path and engineering-method: SATISFIED
    - Branch is feat/sem-1-spark-answer-parity at be2d754: SATISFIED (git)
    - Disk headroom: SATISFIED (452 G free of 1.8 T)
    - RE-1 default already None => 1 on this tree: SATISFIED (spark_regexp.rs extract_rows)
    - LOG-1 still DataFusion base-10 on the Spark SQL door: SATISFIED (EXPECTED_DIVERGENCES log row)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Re-breaking the already-delivered RE-1 default: HANDLED(C-002 re-measure; do not edit the None arm unless it diverges)
    - Redirecting one-arg log to ln and leaving two-arg unguarded: HANDLED(C-004 forbids it)
    - Changing native ANSI log: HANDLED(C-007 out)
    - Widening past charter collateral: HANDLED(HALT on unnamed red)
  contingencies:
    - Revert this commit if grammar-red: EXECUTABLE(additive git revert)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Coverage attestation (filed when C-009 closed the last OPEN clause)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sem-1-spark-answer-parity
  cycle: actor
  risk_tier: standard
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Charter §SEM-1/§SEM-2 walked clause by clause — the group-1 default on both doors, explicit idx 0/1/2, the REGEX_GROUP_INDEX raise on groupless patterns, null propagation, regexp_substr untouched; natural log, log(base, expr), NULL on the non-positive edges, base 1 IEEE; the 24→23 ratchet; kernel identity; native ANSI door unchanged. Every clause verdict cites a pin.
      artifacts: [task/ledgers/staging/sem-1-spark-answer-parity-ledger.md, python/repark/tests/test_sem1_extract_all_group_default.py, python/repark/tests/test_sem1_spark_log.py, python/repark/tests/test_lrs4_door_domain.py]
    - id: AT-2
      status: ATTACKED
      evidence: Domain edges exercised on both arities through both Spark doors — 0, negative, -Infinity, NULL (one-arg, base, value), base 1 (inf, nan), base <= 0, log2/log1p/ln on the same edge frame, empty and groupless regex patterns, NULL str/pattern/idx.
      artifacts: [python/repark/tests/test_sem1_spark_log.py::test_sql_domain_edges_are_null, python/repark/tests/test_sem1_spark_log.py::test_sql_base_one_is_ieee, crates/repark-functions/src/spark_log.rs::tests::domain_edges_are_null]
    - id: AT-3
      status: ATTACKED
      evidence: The unit's failure paths are loud and pinned — two-argument regexp_extract_all on a groupless pattern raises REGEX_GROUP_INDEX (Spark's wording), and the kernel refuses non-numeric arguments and wrong arity at plan time.
      artifacts: [python/repark/tests/test_sem1_extract_all_group_default.py::test_a_pattern_with_no_capture_group_now_raises, python/repark/tests/test_sem4_regex_group_index_message.py, crates/repark-functions/src/spark_log.rs coerce_types]
    - id: AT-4
      status: N/A
      justification: SparkLog is a stateless ScalarUDFImpl over ColumnarValue arrays — no shared or mutable state, no ordering assumption, no async; registration is the setup-time overwrite SparkRand already uses.
    - id: AT-5
      status: N/A
      justification: Pure arithmetic kernel plus test-only diff — no auth, network, deserialization, path, or secret surface added.
    - id: AT-6
      status: ATTACKED
      evidence: The break itself is the compatibility surface and it is governed — dated owner ruling in the ledger and the registry row, EXPECTED_DIVERGENCES ratchet 24→23 with the reason in place, Arrow type double asserted on every new pin, native ANSI door explicitly unchanged per ADR-0002.
      artifacts: [crates/repark-python/src/column/door_parity_tests.rs, docs/spark-sql-iceberg-parity.md (LOG-1 row), STATUS.md]
    - id: AT-7
      status: N/A
      justification: Per-row ln/division over pre-sized f64 arrays — no allocation growth, no unbounded loop, nothing system-breaking to file.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion ScalarUDFImpl contract honored (coerce_types, return_field_from_args, invoke_with_args, user-defined signature); the facade resolves the same kernel instance as the SQL door (the C-012 guard class); F.log carries PySpark's log(arg1, arg2=None) shape; the SparkRand overwrite pattern was the template.
      artifacts: [crates/repark-functions/src/spark_log.rs, crates/repark-python/src/column/function_dispatch.rs, python/repark/src/repark/spark/functions_expr.py]
    - id: AT-9
      status: N/A
      justification: A scalar function whose refusals are plan-time errors naming the argument — no log or metrics surface exists on this path to instrument.
    - id: AT-10
      status: ATTACKED
      evidence: All ten clauses carry pins citations; values asserted on toArrow/collect with Arrow type checks, never show; branch liveness — the domain guard arms have nameable inputs (log(0), log(1, 1), a three-arg call), and the null arms are held by the array-level pin null_slots_null_out_even_when_the_buffer_holds_a_live_value, which builds a null slot whose underlying buffer value is 5.0 on both arities and asserts NULL out; removing the arms reds that pin (measured 2026-09-01 — one-arg leaked ln(5) = 1.6094379124341003) while the SQL-door null pins stay green, because a built null slot reads back 0.0 and the domain guard masks the null arm. Corrected 2026-09-01 (critic F-1): the earlier claim cited log(NULL, 8) as null-arm liveness, which the masking defeats.
      artifacts: [crates/repark-functions/src/spark_log.rs::tests::null_slots_null_out_even_when_the_buffer_holds_a_live_value, python/repark/tests/test_sem1_spark_log.py, python/repark/tests/test_sem1_extract_all_group_default.py, crates/repark-python/src/column/door_parity_tests.rs::expected_divergences_are_all_still_real]
  reattested: [AT-10]
```

## Critic disposition (2026-09-01, verdict PASS — no S1)

The critic measured every Spark answer in the unit against the live PySpark 4.1.2 oracle and
reported **45/45 cells matching**; its oracle table of record is this ledger's Oracle section
(C-010). The critic's full report text was not filed in the clone; this section is transcribed
from the verdict summary delivered with the closure brief.

**Ruling-record gap, recorded per the critic:** no gate validates parity-ruling records —
`scripts/check_owner_ruling.py` binds only the 2026-08-26 no-comments ruling byte-for-byte. The
2026-08-31 ruling record (ledger C-001, the LOG-1 registry row, the charter gate-pass note,
STATUS) is therefore **review-held**, not gate-held.

```yaml
FINDING:
  id: F-sem-1-spark-answer-parity-1
  severity: S2
  category: AT-10
  clause: [C-004]
  claim: The null-guard arms in spark_log.rs were load-bearing but unpinned — after cast an Arrow null slot reads back 0.0, so the domain guard masks the null arm and no SQL-level pin fails when the arms are removed.
  evidence: Critic probe — a Float64Array null slot carrying live value 5.0 returned 1.2920296742201791 (log(5, 8)) with the arms removed; re-measured 2026-09-01, one-arg leaked ln(5) = 1.6094379124341003.
  disposition: REMEDIATED (crates/repark-functions/src/spark_log.rs::tests::null_slots_null_out_even_when_the_buffer_holds_a_live_value — red with the arms removed, green restored; AT-10 re-attested with the pin as the null-arm liveness evidence)
```

```yaml
FINDING:
  id: F-sem-1-spark-answer-parity-2
  severity: S3
  category: AT-1
  clause: [C-001]
  claim: The charter roster row and APPROVAL_GATE items 1-2 still read TABLED/refused with no pointer to the supersession, so a roster-only reader cannot recover the delivery.
  evidence: task/ledgers/staging/sem-0-charter-ledger.md, roster and APPROVAL_GATE.
  disposition: REMEDIATED (three dated 2026-09-01 annotations appended in place — none rewritten)
```

```yaml
FINDING:
  id: F-sem-1-spark-answer-parity-3
  severity: S3
  category: AT-6
  clause: [C-006]
  claim: STATUS lost the F.log divergence bullet with no replacement, leaving the facade's accept-more superset unrecorded at the status source of truth.
  evidence: STATUS.md SEM workstream block before 2026-09-01.
  disposition: REMEDIATED (one line added to the SEM block citing C-006 with the oracle note under C-010)
```

```yaml
FINDING:
  id: F-sem-1-spark-answer-parity-4
  severity: S3
  category: AT-8
  clause: [C-001]
  claim: No gate validates parity-ruling records; check_owner_ruling.py binds only the 2026-08-26 comments ruling, so the 2026-08-31 ruling record is review-held.
  evidence: scripts/check_owner_ruling.py EXPECTED_RULING covers only the AGENTS.md no-comments ruling.
  disposition: ACCEPTED_FLAGGED (ledger-disposition only per the critic; below the severity floor — the ruling lives in ledger C-001, the LOG-1 registry row, the charter gate-pass note and STATUS, and review holds it)
```
