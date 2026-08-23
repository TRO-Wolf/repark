# Unit ledger — G-7b: decimal128 Rust bit-exact pins + cross-door

**Unit:** G-7b (W-1) of the V2 Engine Hardening campaign · **Date:** 2026-08-11 ·
**Worktree:** `/tmp/grok-w1` · **Branch:** `grok/w1-g7b-decimal-rust` · **Executor:** Grok

**Charter:** `planning/grok/BRIEF-w1-g7b-decimal-rust.md` (Addendum A7). Continues the deferred
Rust half of archived
[docs/history/hardening-h1/g7-decimal-ledger.md](../../../../docs/history/hardening-h1/g7-decimal-ledger.md)
§9. Python corpus is **read-only** (ZERO edits).

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Spark-door i128 pins (10) | `crates/repark-spark/src/tests/decimal.rs` | bit-exact Decimal128 / disclosed Float64 on Arrow path |
| Module manifest | `crates/repark-spark/src/tests/mod.rs` | `mod decimal;` |
| Cross-door rows (2) | `crates/repark-sql/tests/cross_door.rs` | same SQL, ANSI vs Spark door, schema+nullability+i128 |
| Maps | `crates/repark-spark/src/tests/map.md`, `crates/repark-sql/tests/map.md`, `task/map.md` | lockstep |
| This ledger | `task/g7b-decimal-rust-ledger.md` | unit record |

### 1.1 Pin inventory (10 — charter budget 8–10)

Every pin cites the Python corpus row name from
`python/repark/tests/test_decimal128_parity.py`. Values are **repark-side** (or equality where
repark == Spark). Asserted on `collect` → `Decimal128Array::value` / `Float64Array::to_bits`.

| # | Test | Class | Corpus row | Pin |
|---|---|---|---|---|
| 1 | `pin_add_same_precision_scale_i128` | equality control | `add_same_precision_scale` | (11,2) non-null i128=579 |
| 2 | `pin_mul_money_by_quantity_i128` | equality control | `mul_money_by_quantity` | (21,2) non-null i128=5997 |
| 3 | `pin_literal_1_23_infers_float64` | literal inference | `literal_1_23_infers_decimal_in_spark_double_in_repark` | Float64 bits of 1.23 |
| 4 | `pin_div_same_precision_scale_repark_i128` | division (p,s) | `div_same_precision_scale` | (16,6) nullable i128=269736 |
| 5 | `pin_mul_38_10_keeps_scale_20_i128` | 38-clamp | `mul_38_10_clamps_scale_in_spark` | (38,20) i128=10^20 |
| 6 | `pin_avg_money_stays_decimal128_14_6_i128` | avg | `avg_money_stays_decimal_in_spark_double_in_repark` | (14,6) nullable i128=1650000 † |
| 7 | `pin_int_times_decimal_promotes_to_31_2_i128` | promotion | `int_times_decimal_promotes_wider_in_repark` | (31,2) non-null i128=750 |
| 8 | `pin_overflow_max_decimal38_plus_one_wrong_value_i128` | ANSI overflow | `overflow_max_decimal38_plus_one_raises_in_spark` | (38,0) wrong residue i128 |
| 9 | `pin_div_by_zero_decimal38_returns_null_at_38_4` | ANSI /0 | `div_by_zero_decimal38_raises_in_spark_null_in_repark` | (38,4) nullable NULL |
| 10 | `pin_mul_single_digit_nullability_non_null_i128` | nullability | `mul_single_digit_nullability_differs` | (3,0) non-null i128=81 |

† **Entry-point note (flagged, not a silent deviation):** the Python facade corpus discloses
float64 for `avg(DECIMAL)`; the **Rust Spark door** already returns Spark-matching
`decimal128(14,6)`. The pin records the Rust-door fact. The facade cell remains a separate
matrix row owned by the Python corpus.

### 1.2 Cross-door rows (2)

| Test | SQL (shared) | Golden | Corpus row |
|---|---|---|---|
| `cross_door_decimal_add_same_precision_scale_bit_exact` | `CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2))` | (11,2) non-null 579 | `add_same_precision_scale` |
| `cross_door_decimal_mul_money_by_quantity_bit_exact` | `CAST(19.99 AS DECIMAL(10,2)) * CAST(3 AS DECIMAL(10,0))` | (21,2) non-null 5997 | `mul_money_by_quantity` |

Protocol: two sessions (native `AnsiDialect` vs `SparkDialect`+`SparkExtension`), independent
warehouses, Arrow-path equality of `(p, s, nullable, Option<i128>)`.

---

## 2. Decisions

**D-G7b-1 — New leaf `tests/decimal.rs`, not a monolith or an existing DDL leaf.** Subject is
expression arithmetic, not CTAS/ALTER. Mapping rule: new leaf + map row (G-4 discipline).

**D-G7b-2 — Cross-door lives in `repark-sql/tests/cross_door.rs`.** That file already owns the
two-session protocol and the only legal door→door dev edge. Pins that need only the Spark door
stay in `repark-spark`; comparing doors cannot live in `repark-spark` (no reverse product edge).

**D-G7b-3 — Pin repark's actual Rust-door output, cite corpus row names.** Disclosure goldens
use the repark half; equality goldens use the shared value. Where the Rust door already matches
Spark while the Python facade does not (avg), pin the Rust fact and flag the entry-point split.

**D-G7b-4 — No production code, no Python corpus, no registry, no Cargo.lock/uv.lock edits.**

---

## 3. Gate evidence

### 3.1 Targeted tests

```
cargo test -p repark-spark --lib tests::decimal
  test result: ok. 10 passed; 0 failed; …

cargo test -p repark-sql --test cross_door cross_door_decimal
  test result: ok. 2 passed; 0 failed; …
```

### 3.2 File-size

`crates/repark-spark/src/tests/decimal.rs` measured under `DEFAULT_CEILING` (1500); no
EXCEPTIONS row required. `cross_door.rs` remains under default after the two added rows.

### 3.3 `make ci` + `make verify`

```
make ci      →  EXIT:0   (fmt/clippy/guards/check/ruff/taplo/typos)
make verify  →  EXIT:0   (ci + cargo test --locked --workspace)
```

Targeted re-confirm:

```
cargo test -p repark-spark --lib tests::decimal  →  10 passed
cargo test -p repark-sql --test cross_door cross_door_decimal  →  2 passed
```

---

## 4. Provocations

Not a new mechanical gate — no gate-provocation transcript required. Pin teeth:

- Changing any expected i128 (e.g. `579` → `578`) reds the matching pin.
- Widening the avg pin's type assert to `Float64` reds (Rust door is Decimal128).
- Cross-door: forcing a type widen on one door only reds the equality assert.

---

## 5. Deviations from brief

1. **avg pin records Rust-door Decimal128, not the Python facade's float64 disclosure** — same
   corpus row name cited; entry-point split is honest and flagged above (D-G7b-3). Not a silent
   absorption of a divergence.
2. **Cross-door rows landed in `repark-sql/tests/cross_door.rs`**, not under
   `crates/repark-spark/src/tests/` — required by the crate DAG (door→door only as dev edge from
   repark-sql). Spark-door half of the equality pins still lives in `decimal.rs`.

---

## 6. Ready-to-paste registry notes

No new divergence rows. Existing DEC-* family already covers the Python-facade disclosures.
Optional future note (not landed here): the avg entry-point split (facade float64 vs Rust door
decimal128(14,6)) could refine the avg disclosure's pin surface if a registry sweep wants it —
out of scope (registry FILE ban).

---

## 7. ACC summary

Engine: ACC + `claims_critic=true` → Critic-1 + Critic-2 + Critic-4 (no Critic-3, no octo, no
overload).

### Critic-1 (correctness / pin teeth)

- **W1-C1-001 (informational, absorbed):** avg pin initially asserted Float64 per the Python
  facade half; Rust Spark door returns `Decimal128(14,6)`. Fixed to pin the Rust-door fact;
  corpus row still cited; entry-point split flagged in §1.1 † and §5.
- **W1-C1-002 (null report):** 10/10 spark pins + 2/2 cross-door green on re-run after fmt/clippy.
  Overflow i128 matches the Python corpus string exactly.
- Coverage classes present: literal, division, 38-clamp, avg, promotion, overflow, div-zero,
  nullability, plus two equality controls.

### Critic-2 (scope / structure)

- **W1-C2-001 (null report):** no Python corpus edits; no registry; no production source; no
  lockfiles; no STATUS.md.
- **W1-C2-002 (flagged deviation, true reason):** cross-door rows in `repark-sql/tests/cross_door.rs`
  not under `repark-spark/src/tests/` — required by crate-DAG (door→door only as repark-sql
  dev-dep). Spark-door pins remain in the new leaf.
- File-size: `decimal.rs` 301 lines (≪ 1500); `cross_door.rs` 748 (≪ 1500). No EXCEPTIONS.

### Critic-4 (claims & record)

- **W1-CL-001 (null report on CL-COUNT):** inventory claims 10 pins + 2 cross-door; `--list`
  confirms 10 `tests::decimal::pin_*` + 2 `cross_door_decimal_*`.
- **W1-CL-002 (null report on CL-MANDATE):** charter items all present in tree (pins, cross-door,
  ledger, map lockstep, file-size, no Python edits).
- **W1-CL-003 (null report on CL-TRANSCRIPT):** `make ci` EXIT:0 re-run and captured; targeted
  suites re-confirmed.
- **W1-CL-004 (null report on CL-GHOST):** corpus row names exist in
  `python/repark/tests/test_decimal128_parity.py`; archived g7 ledger path resolves under
  `docs/history/hardening-h1/`.

**ACC label: CLEAN** (one absorbed entry-point correction; two flagged structural deviations with
true reasons; zero open defects).

## Landing note (L-1, 2026-08-12)

§6 classified **ALREADY-LANDED** / no-registry-surface: DEC-1…DEC-9 already live in the
registry. Optional avg entry-point split note **DEFERRED**.
