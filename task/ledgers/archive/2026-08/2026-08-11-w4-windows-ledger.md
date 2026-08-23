# Unit ledger — W-4 / H-2 gap G5: window-function differential corpus

**Unit:** H-2 gap **G5** of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Lane:** overnight conductor W-4 · **Worktree:** `/tmp/grok-w4` · **Branch:**
`grok/w4-windows-corpus` · **Base:** `origin/main` `396ffdd`

**This ledger covers the record-side differential only.** Out of scope per brief: fixing found
divergences; the registry file; joins (W-3). Critic: `octo` cycles=3 early_stop +
`claims_critic=true`.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | [`python/repark/tests/test_window_parity.py`](../../../../python/repark/tests/test_window_parity.py) | 27 rows + budget pin + classifier |
| Record driver | [`python/repark/tests/_record_window_goldens.py`](../../../../python/repark/tests/_record_window_goldens.py) | re-derive Spark halves; `--emit` paste helpers |
| Tests map | [`python/repark/tests/map.md`](../../../../python/repark/tests/map.md) | lockstep navigation + debug |
| This ledger | `task/w4-windows-ledger.md` | linked from [`task/map.md`](../../../map.md) |

### 1.1 Budget (met)

| Bucket | Budget | Landed |
|---|---|---|
| G5 differential rows | 20–28 | **27** (19 equality + 8 disclosure) |
| Control equalities (min) | ≥6 | **19** |
| Disclosure ceiling | ≤22 | **8** |
| DataFrame-API rows (CP-11) | ≥2 | **2** |
| Default-frame family (name-gated) | ≥3 `default_frame_*` | **4** |

### 1.2 Row inventory (27)

**Equalities (19)** — repark == Spark on value AND Arrow type AND nullability:

| # | Name | Family | Intent |
|---|---|---|---|
| 1 | `default_frame_sum_with_ties` | default_frame | RANGE peers: sum under default frame with ties on `k` |
| 2 | `default_frame_avg_with_ties` | default_frame | same trap on `avg` |
| 3 | `default_frame_count_with_ties` | default_frame | same trap on `count(*)` |
| 4 | `default_frame_partitioned_sum_with_ties` | default_frame | partitioned peers |
| 5 | `rows_unbounded_preceding_current_total_order` | explicit_frame | ROWS unbounded→current, total order |
| 6 | `range_unbounded_preceding_current_with_ties` | explicit_frame | written form of Spark default |
| 7 | `rows_vs_range_peers_differ_on_ties` | explicit_frame | side-by-side ROWS≠RANGE on ties |
| 8 | `rows_sliding_1_preceding_1_following` | explicit_frame | sliding ROWS |
| 9 | `rows_current_to_unbounded_following` | explicit_frame | suffix sum |
| 10 | `range_value_offset_numeric_order` | explicit_frame | RANGE ±1 value offset |
| 11 | `rows_partitioned_sliding` | explicit_frame | partitioned sliding |
| 12 | `percent_rank_with_ties` | ranking | percent_rank peers (double type matches) |
| 13 | `lag_default_offset_1` | offset | lag default |
| 14 | `lag_offset_2_with_default_value` | offset | lag(v,2,-1) |
| 15 | `lead_default_offset_1` | offset | lead default |
| 16 | `lead_offset_1_with_default_value` | offset | lead(v,1,0) |
| 17 | `lag_over_null_values` | offset | lag over NULL payloads |
| 18 | `df_api_partition_by_row_number` | dataframe_api | CP-11 Window.partitionBy + row_number |
| 19 | `df_api_rows_between_sum` | dataframe_api | CP-11 rowsBetween + sum |

**Disclosures (8)** — VALUE matches; TYPE diverges (SQL-door ranking `uint64` vs Spark `int32`):

| # | Name | Spark type | repark type |
|---|---|---|---|
| 20 | `rank_with_ties` | `r: int32` | `r: uint64` |
| 21 | `dense_rank_with_ties` | `r: int32` | `r: uint64` |
| 22 | `row_number_total_order` | `rn: int32` | `rn: uint64` |
| 23 | `ntile_4_total_order` | `bucket: int32` | `bucket: uint64` |
| 24 | `rank_partitioned_with_ties` | `r: int32` | `r: uint64` |
| 25 | `partitioned_vs_unpartitioned_row_number` | `rn_*: int32` | `rn_*: uint64` |
| 26 | `order_by_nulls_first_row_number` | `rn: int32` | `rn: uint64` |
| 27 | `order_by_nulls_last_row_number` | `rn: int32` | `rn: uint64` |

**Cross-door note (scoped claim):** the DF-API door already casts `row_number` to IntegerType
(equality on `df_api_partition_by_row_number`); the SQL door leaves DataFusion's `UInt64`. The
corpus claims the **facade surface** (sql + DataFrame API) and pins both; it does not claim the
ANSI / native doors.

### 1.3 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_window_goldens.py
```

Captured: **`record mode: 27 spark halves re-derived, 0 mismatch(es)`** (2026-08-11, zulu-17,
PySpark 4.1.2, `local[2]`, ANSI on, shuffle=2). Serialized under
`/tmp/grok-jvm-record.lock` with W-3.

Seed discipline: SQL rows register `win_seed` / `win_null_seed` via `createDataFrame` +
`createOrReplaceTempView` (same as DF-API rows) so the corpus measures WINDOW behaviour, not
VALUES literal-type noise (Spark VALUES → int32 non-null; repark VALUES → int64 null).

---

## 2. Decisions, with rationale

**D-W4-1 — Template is the decimal/MERGE mold.** Rows as data, disclosure runner with
CONVERGED-vs-regression classification on `actual`, control equalities, budget pin, committed
record driver importing the same `ROWS` / `run_row`.

**D-W4-2 — Seed via createDataFrame temp view, not bare VALUES.** First record against VALUES
made every row a type disclosure (int32 vs int64). Switched to the dual-engine seed view so
frame/offset/default-frame traps can be honest equalities. Documented in the module and here.

**D-W4-3 — Default-frame trap is name-gated (`default_frame_*` ≥3).** CP-2: a control equality
cannot satisfy the family pin. Aggregate-over-window with ties is required by the brief.

**D-W4-4 — Ranking SQL TYPE disclosure, not a value bug.** Values match bit-for-bit; only Arrow
type differs (`uint64` vs `int32`). DF-API already casts — scoped claim, not silent. Out of scope
to fix (brief).

**D-W4-5 — Determinism (CP-7).** row_number / lag / lead / ROWS frames use total ORDER BY
(`id` or `k, id`). rank / dense_rank / default RANGE intentionally ORDER BY the tied key; measured
columns are peer-determined; assertion is order-insensitive by default (nulls rows use
`order_sensitive=True` with total order via `id` tie-break).

**D-W4-6 — Registry rows stay in this ledger.** Ready-to-paste §6 only; do not edit
`docs/spark-sql-iceberg-parity.md`.

**D-W4-7 — No engine production source edits.** Corpus + maps + ledger only.

**D-W4-8 — JVM lock serialization.** Recorded under exclusive `/tmp/grok-jvm-record.lock`; W-3 had
not taken the lock when W-4 recorded.

---

## 3. Gate evidence

### 3.1 Record mode

```
record mode: 27 spark halves re-derived, 0 mismatch(es)
```

### 3.2 Facade unit suite

```
python/repark/tests/test_window_parity.py — 28 passed in 0.84s
(27 differential + 1 budget)
```

### 3.3 `make ci`

```
make ci — green (fmt, clippy, panic/async bans, crate-dag, lib-rs, rust-file-size, lib-py,
manifest, parity-live dual-wire, cargo check, ruff, uv-lock, taplo, typos)
```

### 3.4 Oracle re-derivation spot-check (≥3 rows)

Independent re-record of sampled rows (fresh driver, same lock basis) matched committed goldens
bit-for-bit for: `default_frame_sum_with_ties`, `rows_vs_range_peers_differ_on_ties`,
`lag_offset_2_with_default_value` (full driver: 27/27 PASS).

---

## 4. Provocations

### 4.1 Verbatim (2026-08-11)

```
=== P1: equality golden value corrupted ===
RED as expected: FrameMismatchError: value mismatch at row 0:
  actual  : {'id': 1, 'k': 1, 'v': 10, 's': 70}
  expected: {'id': 1, 'k': 1, 'v': 10, 's': 71}

=== P2: disclosure halves made identical (both constants = repark uint64 pin) ===
RED as expected: AssertionError: rank_with_ties: the row's two recorded halves are
IDENTICAL, so it is not a disclosure at all - flip it to an equality row (repark=None)
or re-record it. ...

=== P3: disclosure repark pin wrong (r all zeros) ===
RED as expected: AssertionError: rank_with_ties: repark moved OFF its pinned disclosure
and does NOT match the recorded Spark golden either - this is a regression, not a
convergence. Re-derive both halves in record mode ...

=== P4: CONVERGED classification (run_row monkeypatched to return spark golden) ===
RED as expected: AssertionError: rank_with_ties: repark and Spark have CONVERGED -
repark now produces the RECORDED SPARK output, so this disclosure is stale. Do not
delete the row: flip it to an equality row (repark=None) and record the convergence. ...

=== P5: budget pin — default_frame family emptied ===
BUDGET RED as expected: G5 must keep the default-frame-trap family
(>=3 rows named default_frame_*); got 0

All provocations done; suite re-green 28 passed; tree clean.
```

---

## 5. Deviations from brief

1. **27 rows (budget 20–28)** — full coverage of all seven brief bullets without padding.
2. **No raise-class rows** — window surface under test does not refuse these recipes; both engines
   succeed. Raise shape remains available in the dataclass for follow-ups.
3. **Registry file not edited** — paste-true rows in §6 only.
4. **No engine fixes** — disclosures are honest type pins; fix is out of scope.

---

## 6. Ready-to-paste divergence-registry rows

> Do **not** paste into `docs/spark-sql-iceberg-parity.md` from this unit. Orchestrator lands rows
> after the PR merges.

Pin node-id pattern:
`python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[<row.name>]`.

Oracle basis: **recorded** via `_record_window_goldens.py` against live PySpark 4.1.2 (ANSI on).

### G5-RANK-TYPE-1 — SQL-door `rank()` Arrow type

- **repark** — `rank() OVER (ORDER BY k)` yields Arrow `uint64` non-null (values match Spark).
- **Apache Spark** — yields `int32` non-null with the same values. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[rank_with_ties]`
- **Rationale** — BACKLOG, intent to FIX (gap G5). SQL door leaves DataFusion UInt64; DF-API door
  already casts row_number to IntegerType (equality on `df_api_partition_by_row_number`). Same
  class covers dense_rank / row_number / ntile / partitioned rank / nulls-order row_number pins.

### G5-RANK-TYPE-2 — SQL-door `row_number()` Arrow type (total order)

- **repark** — `row_number() OVER (ORDER BY k, id)` → `uint64`.
- **Apache Spark** — `int32`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[row_number_total_order]`
- **Rationale** — BACKLOG, intent to FIX (gap G5). Sibling of G5-RANK-TYPE-1; total-order control
  so the pin is not flaky (CP-7).

### G5-RANK-TYPE-3 — SQL-door `ntile` Arrow type

- **repark** — `ntile(4) OVER (ORDER BY id)` → `uint64`.
- **Apache Spark** — `int32`. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_window_parity.py::test_window_row_matches_spark_or_still_diverges[ntile_4_total_order]`
- **Rationale** — BACKLOG, intent to FIX (gap G5). Completes the ranking-family type class.

### G5-DEFAULT-FRAME (equality evidence, not a divergence)

- Default-frame trap rows (`default_frame_*`) are **equalities** today — repark's default RANGE
  peers match Spark. They stay as control equalities so a future frame regression reds.

---

## 7. Corpus-failure-taxonomy null report (CP-1…CP-12)

| ID | Status |
|---|---|
| CP-1 Dead classifier | **null** — classifier drives `actual` on disclosure path; P4 proves CONVERGED arm |
| CP-2 Tautological coverage pin | **null** — default_frame / ranking needles name-gated |
| CP-3 Simulated-not-executed | **null** — real `session.sql` / DF-API + `to_arrow` |
| CP-4 Vacuous refuse | **null** — no refuse rows |
| CP-5 Non-paste-true handoff | **null** — §6 template + node ids |
| CP-6 Message-format defect | **null** — f-strings for FIX_G5 in TYPE_DISC notes |
| CP-7 Golden drift / hand-edit | **null** — record driver 27/27; total-order discipline |
| CP-8 Oracle-pin tautology | **null** — pyspark pin from project extra, not restated |
| CP-9 Landmine non-vacuity | **null** — N/A |
| CP-10 Budget drift | **null** — pin 20–28 / min eq / max disc in test |
| CP-11 Entry-point blind spot | **null** — ≥2 DF-API rows; claim scoped to facade |
| CP-12 Leftover state | **null** — per-row session; temp views replaced |

---

## 8. Critic / octo

**Engine:** `critic-octo` cycles=3 early_stop + `claims_critic=true` (quad Half-A). No overload.

| Cycle | Half A | Half B | Notes |
|---|---|---|---|
| 1 | OPEN: soft budget floors only applied when spark halves present (C1-Q-001) | Tightened floors always; require every row has spark golden | Pinned |
| 2 | CLEAN (claims match tree: 27/19/8; CP null-report holds; classifier arms P1–P5 proven) | empty | early path |
| 3 | early_stop — CLEAN | — | |

**Label: `OCTO-CONVERGED`** (early_stop after CLEAN findings; verify green; no OPEN ≥ S1).

Claims-critic: ledger budget/inventory/node-id claims match `ROWS` and
`pytest --collect-only` node ids.

---

## 9. Authorship

Commits: author `TRO-Wolf <64240326+TRO-Wolf@users.noreply.github.com>` via per-command `-c`;
trailer `Authored-By: Grok (<model>) <noreply@x.ai>`.

## Landing note (L-1, 2026-08-12)

G5-RANK-TYPE-1/2/3 classified **LANDED** as registry BACKLOG rows (no live-mirror — type-only
ranking family; not in the L-1 live-tier both-halves set). G5-DEFAULT-FRAME remains equality
evidence, not a row.
