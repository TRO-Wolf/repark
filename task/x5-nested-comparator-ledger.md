# Unit ledger — X-5 / H-2 gap G18: nested comparator + nested-container rows

**Unit:** H-2 gap **G18** of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Lane:** overnight conductor #3 X-5 (heaviest) · **Worktree:** `/tmp/grok-x5` · **Branch:**
`grok/x5-g18-nested-comparator` · **Base freeze:** `origin/main` `9acb566`

**This ledger covers Part 1 (comparator) + Part 2 (4–6 nested rows).** Critic: `octo` cycles=3
early_stop + `claims_critic=true`. Out of scope: G10 census; comparator API breaks; registry
file; locks; planning/hardening.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Nested comparator | [`python/repark-parity/src/repark_parity/compare.py`](../python/repark-parity/src/repark_parity/compare.py) | order-insensitive path for list/struct/map |
| Comparator unit tests | [`python/repark-parity/tests/test_compare.py`](../python/repark-parity/tests/test_compare.py) | 4 hard invariants + extras |
| Nested corpus | [`python/repark/tests/test_nested_container_parity.py`](../python/repark/tests/test_nested_container_parity.py) | 6 rows + budget pin + classifier |
| Record driver | [`python/repark/tests/_record_nested_container_goldens.py`](../python/repark/tests/_record_nested_container_goldens.py) | re-derive Spark halves; `--emit` |
| Maps | parity `map.md` ×2, `python/repark/tests/map.md` | lockstep navigation |
| This ledger | `task/x5-nested-comparator-ledger.md` | linked from [`task/map.md`](map.md) |

### 1.1 Budget (met)

| Bucket | Budget | Landed |
|---|---|---|
| G18 differential rows | 4–6 | **6** (3 equality + 3 disclosure) |
| Equalities (min) | ≥2 | **3** |
| Disclosures (min) | ≥2 | **3** |
| Struct family (`*struct*`) | ≥1 | **3** |
| Map family (`*map*`) | ≥1 | **1** |
| Array/list family (`*array*` / `*collect_list*`) | ≥2 | **3** |

### 1.2 Row inventory (6)

**Equalities (3)** — repark == Spark on value AND Arrow type AND nullability:

| # | Name | Family | Intent |
|---|---|---|---|
| 1 | `struct_column_roundtrip` | struct | createDataFrame struct column; multiset + duplicate nested payload |
| 2 | `struct_sql_select` | struct | SQL-door projection of struct seed view (CP-11) |
| 3 | `map_column_roundtrip` | map | createDataFrame map column; entry-order normalized by comparator |

**Disclosures (3)** — VALUES match; TYPE diverges (`list<item:…>` vs `list<element:…>`):

| # | Name | Spark type | repark type |
|---|---|---|---|
| 4 | `array_column_roundtrip` | `items: list<element: int64>` nullable | `list<item: int64>` nullable |
| 5 | `collect_list_grouped` | `list<element: int64 not null>` **non-null field** | `list<item: int64>` nullable |
| 6 | `array_of_struct_roundtrip` | `list<element: struct<…>>` | `list<item: struct<…>>` |

### 1.3 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark/src:python/repark-parity/src \
  python python/repark/tests/_record_nested_container_goldens.py
```

Captured: **`record mode: 6 rows re-derived, 0 mismatch(es)`** (2026-08-11, zulu-17,
PySpark 4.1.2, `local[2]`, ANSI on, shuffle=2). Serialized under `/tmp/grok-jvm-record.lock`.

---

## 2. Design note — nested order-insensitive comparator (Part 1)

### Chosen mechanism

1. **Flat schemas unchanged:** when no top-level field is list/large_list/fixed_size_list/struct/map,
   keep historical `Table.sort_by` on all columns. Existing corpora do not re-record.
2. **Nested schemas:**  
   a. **Normalize map entry order** recursively (sort key→value pairs by canonical key) so equal
      maps with different storage order still match under Arrow `equals` (which is entry-order
      sensitive). List element order is **not** rewritten (Spark arrays are ordered).  
   b. Build a **total, deterministic** per-row sort key by recursive canonical encoding
      (nulls first; list order significant; struct fields in schema order; map entries sorted).  
   c. Reorder with `Table.take`, then Arrow `equals` as before.

### Rejected alternatives

| Alternative | Why rejected |
|---|---|
| Sort by flat columns only; bag-compare nested | Not total when nested values distinguish rows, or when every column is nested |
| Multiset `Counter` of `to_pylist()` rows | Drops Arrow `equals` path; changes `_first_difference` messaging; harder flat-identity pin |
| JSON / text serialization as sort key | Not bit-exact for float / decimal / binary |
| Always take-based path for flats | Risks changing Arrow null placement / multi-column sort order → golden re-records |

### Hard invariants (each unit-tested, JVM-free)

1. Flat-schema `sort_by` path byte-identical to historical helper order.
2. Nested row-permutation invariance for list, struct, map.
3. Multiset sensitivity — mutation per nested kind (list / struct / map).
4. `order_sensitive=True` path untouched (nested permutation still fails).

---

## 3. Decisions, with rationale

**D-X5-1 — Total canonical keys + map normalization.** Required for collect_list-style
unordered outer rows with nested cells; map entry order differs across engines/storage.

**D-X5-2 — Flat path forever separate.** "Existing facade suite green without re-record" is a
hard gate; only nested schemas take the new path.

**D-X5-3 — Array field-name is a TYPE disclosure, not a value bug.** Values match bit-for-bit;
`item` vs `element` (and collect_list nullability) are honest Arrow-type divergences. Fix is
out of scope (G10/follow-on).

**D-X5-4 — createDataFrame seed, not bare VALUES.** VALUES would inject int32/non-null noise;
struct/map equalities would collapse into type disclosures.

**D-X5-5 — Registry rows stay in this ledger.** Ready-to-paste §6 only; do not edit
`docs/spark-sql-iceberg-parity.md`.

**D-X5-6 — Combined Part 1+2 in one PR.** Combined added lines ~1.1k (under the ~1200 ceiling).

---

## 4. Gate evidence

### 4.1 Comparator unit suite

```
make py-test — 155 passed (includes G18 nested invariants)
```

### 4.2 Record mode

```
record mode: 6 rows re-derived, 0 mismatch(es)
```

### 4.3 Nested corpus suite

```
python/repark/tests/test_nested_container_parity.py — 7 passed
(6 differential + 1 budget)
```

### 4.4 Ruff

```
uvx ruff@0.15.22 check <touched py> — All checks passed
```

### 4.5 Oracle re-derivation spot-check (≥3 rows)

Full driver re-run: all 6 PASS bit-for-bit against committed goldens
(`struct_column_roundtrip`, `map_column_roundtrip`, `collect_list_grouped` sampled among them).

---

## 5. Provocations

### 5.1 Verbatim (2026-08-11)

```
=== P1: equality golden value corrupted (struct) ===
RED as expected: FrameMismatchError: value mismatch at row 0:
  actual  : {'id': 1, 'payload': {'x': 10, 'y': 'a'}}
  expected: {'id': 2, 'payload': {'x': 20, 'y': 'b'}}

=== P2: disclosure halves made identical ===
RED as expected: AssertionError: array_column_roundtrip: the row's two recorded halves are
IDENTICAL, so it is not a disclosure at all - flip it to an equality row (repark=None)
or re-record it.

=== P3: disclosure repark pin wrong (items all zeros on row 0) ===
RED as expected: AssertionError: array_column_roundtrip: repark moved OFF its pinned
disclosure and does NOT match the recorded Spark golden either - this is a regression...

=== P4: CONVERGED classification (run_row monkeypatched to return spark golden) ===
RED as expected: AssertionError: array_column_roundtrip: repark and Spark have CONVERGED -
repark now produces the RECORDED SPARK output, so this disclosure is stale. Do not
delete the row: flip it to an equality row (repark=None)...

=== P5: budget pin — struct family emptied ===
BUDGET RED as expected if emptied: G18 must keep the struct family
(>=1 rows named *struct*); got 0  (currently 3)

=== P6: comparator multiset sensitivity list mutation (unit) ===
RED as expected: FrameMismatchError: value mismatch at row 0:
  actual  : {'id': 1, 'items': [1, 2]}
  expected: {'id': 1, 'items': [1, 9]}

All provocations done.
```

---

## 6. Ready-to-paste divergence-registry rows

> **Do not edit `docs/spark-sql-iceberg-parity.md` in this unit.** Orchestrator pastes after merge.

- **repark** — array / collect_list / array-of-struct columns export Arrow lists whose value
  field is named `item` (DataFusion default); `collect_list` leaves the list field nullable and
  elements nullable.
- **Apache Spark 4.1.2** — same recipes export list value field named `element`; `collect_list`
  marks the list field non-nullable and elements non-nullable when the input column is non-null.
  *(oracle: recorded.)*
- **Pin**
  - `python/repark/tests/test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges[array_column_roundtrip]`
  - `python/repark/tests/test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges[collect_list_grouped]`
  - `python/repark/tests/test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges[array_of_struct_roundtrip]`
- **Rationale** — TYPE disclosure class (list field name + collect_list nullability). Values
  match. Unlocked by G18 nested order-insensitive comparator; fix is G10 / list-type follow-on,
  not silent absorption.

---

## 7. Critic record (octo cycles=3 early_stop + claims_critic)

### Cycle 1 — Actor self-check vs corpus-failure taxonomy (CP-1…CP-12)

| CP | Finding | Disposition |
|---|---|---|
| CP-1 Dead classifier | CONVERGED + regression arms driven on disclosure lifecycle; P4 monkeypatch hits CONVERGED | PASS |
| CP-2 Tautological coverage | Families name-gated `*struct*` / `*map*` / `*array*`/`*collect_list*` | PASS |
| CP-3 Simulated-not-executed | Recipes call createDataFrame / sql / groupBy.agg for real; record driver runs same `run_row` | PASS |
| CP-4 Vacuous refuse | No refuse rows in this corpus | N/A |
| CP-5 Non-paste-true handoff | §6 uses paste-true bullets + fully-qualified node ids | PASS |
| CP-6 Message-format defect | Guidance strings are plain / f-strings; P4 printed correctly | PASS |
| CP-7 Golden drift | Full record re-derive 6/6 PASS | PASS |
| CP-8 Oracle-pin tautology | No version literal pin restated; basis in driver config only | PASS |
| CP-9 Landmine non-vacuity | N/A (no leak-as-proof pins) | N/A |
| CP-10 Budget drift | Budget pin asserts min/max + family floors | PASS |
| CP-11 Entry-point blind spot | DF API + SQL struct rows; claim scoped to facade | PASS |
| CP-12 Leftover state | Session stop + `_reset_active_session_for_tests` after each row | PASS |

### Cycle 2 — Claims critic (claims_critic=true)

| Claim | Evidence | Verdict |
|---|---|---|
| Flat corpora unchanged | `test_flat_schema_sort_path_unchanged` + `make py-test` 155 green | PROVEN |
| Nested permutation-invariant | `test_nested_row_permutation_invariance_list_struct_map` | PROVEN |
| Multiset sensitive per kind | three mutation tests + P6 | PROVEN |
| order_sensitive untouched | `test_order_sensitive_nested_untouched` | PROVEN |
| 4–6 nested rows vs Spark | 6 rows recorded; 3 eq + 3 type disclosures | PROVEN |
| Value AND type AND nullability | equality uses assert_frames_equal; disclosures pin both schema signatures | PROVEN |
| Registry not edited | no diff under `docs/spark-sql-iceberg-parity.md` | PROVEN |

### Cycle 3 — Residual / early-stop

No OPEN claims; no taxonomy miss requiring a fix cycle. **OCTO-CONVERGED** (early_stop).

**ACC:** accept for PR.

---

## 8. Deviations from brief

1. **6 rows (budget 4–6)** — full struct/map equalities + three array-family disclosures.
2. **No raise-class rows** — nested shapes under test succeed on both engines; refuse shape left
   available for follow-ups.
3. **Registry file not edited** — paste-true rows in §6 only.
4. **No engine production source edits** — disclosures are honest type pins.

---

## 9. Diff size / sequencing

- Part 1 + Part 2 combined in **one PR** (under ~1200 added lines ceiling).
- No stacked second PR (conductor B1).
