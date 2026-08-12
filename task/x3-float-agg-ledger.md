# Unit ledger — X-3 / G7: float aggregation determinism pins

**Unit:** G7 (X-3) of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Worktree:** `/tmp/grok-x3` · **Branch:** `grok/x3-g7-float-agg` · **Executor:** Grok ·
**Freeze:** `9acb566` (conductor A11) · **Engine:** ACC (Actor→C1→C2) + claims_critic

**Charter:** `planning/grok/BRIEF-x3-g7-float-agg.md` (conductor-3 B-rules + A1–A11 bind).
Mold: `crates/repark-spark/src/tests/decimal.rs` (G-7b) + corpus-lane skill.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Spark-door `f64::to_bits` pins | `crates/repark-spark/src/tests/float_agg.rs` | sum/avg at 3 partition counts + stability + spread |
| Module manifest | `crates/repark-spark/src/tests/mod.rs` | `mod float_agg;` |
| Differential corpus (2 rows) | `python/repark/tests/test_float_agg_parity.py` | sum/avg vs live Spark (disclosures) |
| Record driver | `python/repark/tests/_record_float_agg_goldens.py` | re-derive Spark halves; never edits corpus |
| Maps | `crates/repark-spark/src/tests/map.md`, `python/repark/tests/map.md`, `task/map.md` | lockstep |
| This ledger | `task/x3-float-agg-ledger.md` | unit record |

### 1.1 Fixture (catastrophic cancellation)

Order-sensitive vector of 8 doubles. Exact `f64::to_bits` re-asserted by
`pin_fixture_element_bit_patterns`:

| Index | Value | Bits |
|---|---|---|
| 0 | `1.0e16` | `0x4341c37937e08000` |
| 1 | `1.0` | `0x3ff0000000000000` |
| 2 | `-1.0e16` | `0xc341c37937e08000` |
| 3 | `2.0` | `0x4000000000000000` |
| 4 | `1.0e16` | `0x4341c37937e08000` |
| 5 | `0.5` | `0x3fe0000000000000` |
| 6 | `-1.0e16` | `0xc341c37937e08000` |
| 7 | `0.25` | `0x3fd0000000000000` |

True mathematical sum = `3.75`. Left-to-right IEEE sum loses small addends under the large
magnitudes → `2.25`. Compensated / reordered partials can recover `3.75`.

### 1.2 Engine config knob

**Name:** DataFusion `target_partitions` (degree of intra-query parallelism).

**How repark exposes it:**

| Layer | Spelling |
|---|---|
| Rust `SessionConfig` | `SessionConfig::with_target_partitions(n)` |
| `ReparkSessionBuilder` | `.target_partitions(n)` |
| Builder conf / facade | `spark.sql.shuffle.partitions` / `repark.target.partitions` → builder `target_partitions` |
| Runtime conf | `datafusion.execution.target_partitions` |

Verified against `crates/repark-core/src/session.rs` and
`python/repark/src/repark/session/session_core.py` (`_resolve_shuffle_partitions`).
Tests lock input MemTable partition count to the same `n` so partial aggregation fans out.

### 1.3 Rust pin inventory (6 absolute `f64::to_bits` + supporting)

Budget charter: **6–8** `f64::to_bits` pins. Absolute goldens:

| # | Test | Op | `target_partitions` | Value | Bits |
|---|---|---|---|---|---|
| 1 | `pin_sum_f64_bits_at_target_partitions_1` | sum | 1 | 3.75 | `0x400e000000000000` |
| 2 | `pin_sum_f64_bits_at_target_partitions_2` | sum | 2 | 3.75 | `0x400e000000000000` |
| 3 | `pin_sum_f64_bits_at_target_partitions_8` | sum | 8 | 2.25 | `0x4002000000000000` |
| 4 | `pin_avg_f64_bits_at_target_partitions_1` | avg | 1 | 0.46875 | `0x3fde000000000000` |
| 5 | `pin_avg_f64_bits_at_target_partitions_2` | avg | 2 | 0.46875 | `0x3fde000000000000` |
| 6 | `pin_avg_f64_bits_at_target_partitions_8` | avg | 8 | 0.28125 | `0x3fd2000000000000` |

Supporting (not counted against the 6–8 absolute budget, but load-bearing):

| Test | Role |
|---|---|
| `pin_fixture_element_bit_patterns` | SSOT guard on FIXTURE element bits |
| `pin_sum_f64_run_to_run_stable_at_three_partition_counts` | determinism: same config → same bits twice |
| `pin_avg_f64_run_to_run_stable_at_three_partition_counts` | same for avg |
| `pin_sum_f64_cross_count_spread_p8_differs_from_p1` | **discloses** p=1/2 vs p=8 bit divergence |

**Cross-count claim (honest):** p=1 and p=2 **agree** (3.75); p=8 **differs** (2.25). Never
pinned cross-count equality. Spread is a first-class disclosure pin so a future fix that
collapses the counts flips it red.

### 1.4 Differential rows (2)

| Name | Kind | Spark | Repark |
|---|---|---|---|
| `sum_catastrophic_cancellation_fixture` | disclosure | 2.25 (nullable f64) | 3.75 (nullable f64) |
| `avg_catastrophic_cancellation_fixture` | disclosure | 0.28125 (nullable f64) | 0.46875 (nullable f64) |

Oracle: live PySpark **4.1.2**, zulu-17, `master("local[2]")`, ANSI on,
`spark.sql.shuffle.partitions=2`, UI off. Repark session built with
`spark.sql.shuffle.partitions=2`. Record driver held `/tmp/grok-jvm-record.lock` (B4).

**Why disclosure, not equality or last-ulp tolerance:** value divergence is large (2.25 vs
3.75 — not a last-ULP), type+nullability agree. Declared-tolerance path exists in-module
(`max_ulps`) but is unused; fudging equality would be dishonest.

**Live-tier implication (A4):** any live DISCLOSURE for these rows is **§6 paste-true handoff
only** — this lane never edits `_live_parity.py`. Flagged in `X3-COMPLETE.md`.

---

## 2. Decisions

**D-X3-1 — New leaf `tests/float_agg.rs`, not an existing DDL leaf.** Subject is expression
aggregation determinism. Mapping rule: new leaf + map row (G-4 discipline). B2: X-3 owns new
modules under `crates/repark-spark/src/tests/` tonight.

**D-X3-2 — Per-count pins + explicit spread disclosure; never cross-count equality.** Measured
bits differ at p=8; the charter's caveat binds: *"the honest outcome may be a declared
tolerance, not equality"*.

**D-X3-3 — Differential rows are both disclosures.** Spark's sequential VALUES sum loses small
addends; repark's accumulator on the same VALUES keeps them. Same type; different value.

**D-X3-4 — No production code, no `_live_parity.py`, no registry, no Cargo.lock/uv.lock,
no AGENTS/STATUS (B6).**

**D-X3-5 — Input MemTable partitions match `target_partitions`.** A single-partition MemTable
can keep partial aggregation sequential even when the config advertises more partitions; the
fan-out is real only when the scan produces multiple input partitions.

---

## 3. Gate evidence

### 3.1 Targeted tests

```
cargo test -p repark-spark --lib tests::float_agg
  test result: ok. 10 passed; 0 failed; …

PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest \
  python/repark/tests/test_float_agg_parity.py -q
  3 passed
```

### 3.2 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_float_agg_goldens.py

[G7] sum_catastrophic_cancellation_fixture PASS
[G7] avg_catastrophic_cancellation_fixture PASS
record mode: 2 spark halves re-derived, 0 mismatch(es)
```

Lock: exclusive `/tmp/grok-jvm-record.lock`. Standing containerized cluster ignored (B4).

### 3.3 File-size

`float_agg.rs` ~303 lines (under DEFAULT_CEILING 1500). No EXCEPTIONS row required.

### 3.4 `make ci` + `make verify`

```
make ci      →  EXIT:0
make verify  →  EXIT:0   (ci + cargo test --locked --workspace)
```

Targeted re-confirm after restore-from-provocation:

```
cargo test -p repark-spark --lib tests::float_agg  →  10 passed
pytest test_float_agg_parity.py                     →  3 passed
```

---

## 4. Provocations

| # | Perturbation | Expected | Observed |
|---|---|---|---|
| P1 | `SUM_BITS_P1` +1 ulp | pin_sum p=1 RED | **RED** (`left != right` bits) |
| P2 | FIXTURE[1] `1.0`→`1.1` | fixture pin RED | **RED** (fixture[1] bit pattern drifted) |
| P3 | (structural) disclosure halves equal | well-formedness RED | covered by `assert _frames_differ(repark, spark)` |
| P4 | repark returns Spark 2.25 | CONVERGED message | covered by disclosure classifier branch |

Suite re-green after P1/P2 restore: 10 Rust + 3 Python passed.

---

## 5. Deviations from brief

None material. Budget 6 absolute `f64::to_bits` pins (charter 6–8) + 4 supporting tests.
Both differential rows are disclosures (honest); equality was attempted and rejected by
measurement.

---

## 6. Registry handoff (paste-true — orchestrator lands; lane does NOT edit registry)

> **FLAG (A4 / conductor A3):** live-tier DISCLOSURE for these rows is **not** landed in this
> PR. Below is paste-true text for **both halves** — (a) registry row with `- live-mirror:`
> bullet, (b) exact `Disclosure(...)` code block for `_live_parity.py` + the size-pin update
> in `test_parity_live.py`. Orchestrator lands both sides together post-merge so the
> two-direction mirror gate stays green.

### 6.1 Registry rows (for `docs/spark-sql-iceberg-parity.md`)

- **FLOAT-AGG-1 — sum of catastrophic-cancellation float vector**
  - **repark** — `sum(v)` over the G7 fixture lands **3.75** (`f64` bits `0x400e000000000000`)
    at `target_partitions` / `spark.sql.shuffle.partitions = 2` on a VALUES source. Type:
    Arrow `float64` nullable.
  - **Apache Spark** — same recipe under `local[2]`, ANSI on, `spark.sql.shuffle.partitions=2`
    lands **2.25** (`f64` bits `0x4002000000000000`). Type: Arrow `double` nullable.
    *(oracle: recorded.)*
  - **Pin** —
    `python/repark/tests/test_float_agg_parity.py::test_float_agg_parity_row[sum_catastrophic_cancellation_fixture]`
    and `crates/repark-spark/src/tests/float_agg.rs::pin_sum_f64_bits_at_target_partitions_2`
  - **Rationale** — accumulation-order sensitivity on a catastrophic-cancellation fixture;
    value diverges, type agrees. DECLARE candidacy until a G7 fix lands. Cross-count repark
    internal spread at p=8 is pinned separately in Rust.
  - live-mirror: `sum_catastrophic_cancellation_fixture`

- **FLOAT-AGG-2 — avg of the same fixture**
  - **repark** — `avg(v)` lands **0.46875** (`f64` bits `0x3fde000000000000`).
  - **Apache Spark** — lands **0.28125** (`f64` bits `0x3fd2000000000000`). *(oracle: recorded.)*
  - **Pin** —
    `python/repark/tests/test_float_agg_parity.py::test_float_agg_parity_row[avg_catastrophic_cancellation_fixture]`
    and `crates/repark-spark/src/tests/float_agg.rs::pin_avg_f64_bits_at_target_partitions_2`
  - **Rationale** — follows FLOAT-AGG-1 (avg = sum/8); same accumulation-order class.
  - live-mirror: `avg_catastrophic_cancellation_fixture`

### 6.2 `_live_parity.py` Disclosure blocks (exact — orchestrator pastes; NOT in this PR)

Paste into `python/repark/tests/_live_parity.py` `DISCLOSURES` list (names must match the
`live-mirror:` bullets above exactly — the mirror gate parses that spelling). Positional
constructor matches existing entries (`name, repark_check, spark_check, note`):

```python
# G7 float-agg (X-3 handoff) — land with the registry live-mirror bullets above.
Disclosure(
    "sum_catastrophic_cancellation_fixture",
    _disc_sum_catastrophic_cancellation_repark,
    _disc_sum_catastrophic_cancellation_spark,
    "sum of the G7 catastrophic-cancellation fixture: repark lands 3.75 (f64 bits "
    "0x400e000000000000); Spark 4.1.2 local[2]/shuffle=2 lands 2.25 (0x4002000000000000). "
    "Same Arrow float64 nullable; accumulation order diverges. Corpus: "
    "test_float_agg_parity.py::test_float_agg_parity_row[sum_catastrophic_cancellation_fixture].",
),
Disclosure(
    "avg_catastrophic_cancellation_fixture",
    _disc_avg_catastrophic_cancellation_repark,
    _disc_avg_catastrophic_cancellation_spark,
    "avg of the same fixture (sum/8): repark 0.46875 (0x3fde000000000000) vs Spark 0.28125 "
    "(0x3fd2000000000000). Follows the sum divergence. Corpus: "
    "test_float_agg_parity.py::test_float_agg_parity_row[avg_catastrophic_cancellation_fixture].",
),
```

Helpers (orchestrator implements beside the other `_disc_*` functions; same `Engine` mold):

```python
_G7_FIXTURE_VALUES_SQL = (
    "SELECT * FROM (VALUES "
    "(CAST(1.0e16 AS DOUBLE)), (CAST(1.0 AS DOUBLE)), (CAST(-1.0e16 AS DOUBLE)), "
    "(CAST(2.0 AS DOUBLE)), (CAST(1.0e16 AS DOUBLE)), (CAST(0.5 AS DOUBLE)), "
    "(CAST(-1.0e16 AS DOUBLE)), (CAST(0.25 AS DOUBLE))"
    ") AS t(v)"
)
_G7_SUM_SQL = f"SELECT sum(v) AS s FROM ({_G7_FIXTURE_VALUES_SQL}) src"
_G7_AVG_SQL = f"SELECT avg(v) AS a FROM ({_G7_FIXTURE_VALUES_SQL}) src"


def _disc_sum_catastrophic_cancellation_repark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_SUM_SQL))
    assert out.schema.field("s").type == pa.float64()
    assert out.schema.field("s").nullable is True
    assert out.column("s").to_pylist() == [3.75]


def _disc_sum_catastrophic_cancellation_spark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_SUM_SQL))
    assert out.schema.field("s").type == pa.float64()
    assert out.schema.field("s").nullable is True
    assert out.column("s").to_pylist() == [2.25]


def _disc_avg_catastrophic_cancellation_repark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_AVG_SQL))
    assert out.schema.field("a").type == pa.float64()
    assert out.schema.field("a").nullable is True
    assert out.column("a").to_pylist() == [0.46875]


def _disc_avg_catastrophic_cancellation_spark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_AVG_SQL))
    assert out.schema.field("a").type == pa.float64()
    assert out.schema.field("a").nullable is True
    assert out.column("a").to_pylist() == [0.28125]
```

### 6.3 Exact-set pin update text (`test_parity_live.py`)

The mirror gate and any exact-set pin of `DISCLOSURES` names (≈ line 215 on `9acb566`) must add
exactly:

```text
sum_catastrophic_cancellation_fixture
avg_catastrophic_cancellation_fixture
```

**Do not change** `SCENARIOS` size pin (**42**) or `LIFECYCLE` size pin (**2**). Only the
`DISCLOSURES` exact-set grows by these two names, landed in the **same** orchestrator commit as
the registry `live-mirror:` bullets. **Do not land one side without the other.**

---

## 7. Critic (ACC + claims_critic)

**Engine:** ACC (Actor → C1 → C2) + claims_critic=true. No overload. No HALT.

### C1 (first critic pass)

| Finding | Disposition |
|---|---|
| Cross-count bits differ (p=8) — do not pin equality | **FIXED at authoring** — per-count pins + `pin_sum_f64_cross_count_spread_p8_differs_from_p1` |
| Differential rows cannot be equality (Spark 2.25 vs repark 3.75) | **FIXED at authoring** — both rows are disclosures |
| Clippy `doc_markdown` on MemTable | **FIXED** — backticks |
| Ruff E501 on record driver / tolerance path | **FIXED** |
| A4: no `_live_parity.py` edit | **HELD** — §6 handoff only |
| B2: only new modules under `src/tests/` | **HELD** — `float_agg.rs` only new leaf |
| B6 bans | **HELD** — no AGENTS/STATUS/registry/locks |

### C2 (second critic pass)

| Finding | Disposition |
|---|---|
| VALUES SQL path (Python) vs MemTable multi-partition (Rust) are different surfaces | **DISCLOSED** in ledger §1.3/1.4 — not a silent conflation |
| Nullability of sum/avg asserted `true` | **PROVEN** by green pins on both doors |
| Budget 6 absolute pins (charter 6–8) | **OK** |
| Node ids resolvable | **PROVEN** via `pytest --collect-only` |

### claims_critic

Ledger claims vs tree:

| Claim | Tree |
|---|---|
| 6 absolute `f64::to_bits` pins at 1/2/8 × sum/avg | 6 named `pin_{sum,avg}_f64_bits_at_target_partitions_{1,2,8}` present |
| Cross-count spread pin | `pin_sum_f64_cross_count_spread_p8_differs_from_p1` present |
| 2 differential disclosures | ROWS length 2; both `repark is not None` |
| Record driver imports ROWS | `_record_float_agg_goldens.py` imports from test module |
| map.md lockstep | three maps updated |
| No live_parity / registry / lockfile edits | `git diff --name-only` clean of those paths |

**Verdict: ACC-CONVERGED** (C1 fixes absorbed at authoring; C2 CLEAN; claims_critic zero OPEN).

---

## 8. Commits / PR

| Field | Value |
|---|---|
| **SHA** | `14b5d047a71a3051ff02cc18c63a086eda0a0f7b` |
| **PR** | https://github.com/TRO-Wolf/repark/pull/61 |
| **Author** | TRO-Wolf + `Authored-By: Grok (grok-code) <noreply@x.ai>` |
| **Base** | `origin/main` @ freeze `9acb566` (A11; no mid-flight rebase) |
