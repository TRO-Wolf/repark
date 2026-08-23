# Unit ledger — S-2 / H-2 G8: capability value-semantics matrix + test-name liveness

**Unit:** H-2 gap **G8** of the V2 Engine Hardening campaign · **Date:** 2026-08-14 ·
**Lane:** S-2 · **Branch:** `grok/s2-g8-matrix` · **Worktree:** `/tmp/grok-s2` ·
**Base (frozen):** `d9a739123be8b00bc1fc1e6d4bbad875ba6caa76` (`#91`) ·
**SEPMO:** STANDARD acc + C4 (claims accuracy is the whole risk).

**This ledger covers the vocabulary + matrix + liveness gate ONLY.** No engine edits.
No new engine tests. The divergence registry is S-5. `_live_parity` / lockfiles /
`Cargo.toml [patch]` / JVM lock are CLOSED. The JVM lock was **never taken**.

Charter: `briefs/v2-engine-hardening.md` G8 + conductor-8 A9 (exactly these 7 IDs).

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| 7 `SEMANTICS_*` IDs | `crates/repark-common/src/surfaces.rs` | appended after the ergonomics block |
| Reviewed count 43 → 50 | `crates/repark-common/src/surfaces/tests.rs` | `all_has_the_reviewed_surface_count` |
| Spark-door 7 rows | `crates/repark-spark/src/matrix.rs` | 6 Tested + 1 pin-absence |
| ANSI-door 7 rows | `crates/repark-sql/src/matrix.rs` | 4 Tested + 3 pin-absences |
| Liveness gate | `scripts/check_matrix_test_liveness.py` + `.sh` | `cargo test -- --list` vs both matrices |
| Makefile target | `make check-matrix-test-liveness` | in `make ci` (hence `verify` / `preflight`) |
| CI dual-wire | `.github/workflows/ci.yml` rust-test job | same script, after `make rust-test` |
| This ledger | `task/s2-g8-ledger.md` | linked from `task/map.md` |

---

## 2. Completeness — 14 rows

Every `Tested` name was taken from `cargo test --locked --workspace -- --list` on this
freeze tree (1531 listed names captured 2026-08-14). No name was invented. No engine
test was written to fill a cell.

| ID | Spark door | ANSI door |
|---|---|---|
| `SEMANTICS_NULL_ORDERING` | **Tested** `spark_ast::tests::order_by_defaults_are_spark` (SparkExtended) | **Tested** `ansi_door_order_by_asc_defaults_to_nulls_last` (Native) |
| `SEMANTICS_DECIMAL_ARITHMETIC` | **Tested** `tests::decimal::pin_add_same_precision_scale_i128` (SparkExtended) | **Tested** `cross_door_decimal_add_same_precision_scale_bit_exact` (TwoSession) |
| `SEMANTICS_CAST_MATRIX` | **Tested** `spark_door_timestamp_cast_to_bigint_is_epoch_seconds` (SparkExtended) | **Tested** `ansi_door_cast_overflow_int_to_tinyint_raises` (Native) |
| `SEMANTICS_SESSION_TIMEZONE` | **Tested** `year_extractor_resolves_in_the_session_zone` (SparkExtended) | **Tested** `a_native_session_without_the_spark_extension_reads_the_stored_zone` (Native) |
| `SEMANTICS_WINDOW_FRAMES` | **Tested** `tests::window_temporal_range::temporal_range_interval_bounds_still_match_spark` (SparkExtended) | **ABSENT** — no ANSI Native-profile ROWS/RANGE frame-value pin |
| `SEMANTICS_JOIN_NULL_KEYS` | **ABSENT** — G4 corpus is Python-only; no Rust door-binary NULL-key join pin | **ABSENT** — same; facade cannot reach the ANSI door |
| `SEMANTICS_FLOAT_DETERMINISM` | **Tested** `tests::float_agg::pin_sum_f64_bits_at_target_partitions_1` (SparkExtended) | **ABSENT** — G7 rust twins are Spark-door only |

**Tally:** Spark 6 tested / 1 absent · ANSI 4 tested / 3 absent · **10 tested / 4 absent**.

Shipped-count pins moved with the rows:

| Door | Before (43 IDs) | After (50 IDs) |
|---|---|---|
| Spark Tested / Absent | 40 / 3 | 46 / 4 |
| ANSI Tested / Absent | 39 / 4 | 43 / 7 |

The four new absences are **pin-absences** (the door implements the surface via DataFusion;
this freeze tree has no `cargo test -- --list` name that pins the class). They are not
product refusals. That distinction is the §6 handoff.

### Why those four absences (not a weaker Tested cite)

- **JOIN both doors.** `python/repark/tests/test_join_parity.py` (`null_keys_inner_no_match`
  and the outer-orphan family) is the G4 corpus. It is not a cargo-test name. The Rust
  binding pin `join_on_names_left_anti_keeps_unmatched_left_rows_including_null_keys` is the
  native DataFrame API, not a SQL-door binary. Citing it as Spark-door or ANSI-door evidence
  would be a claims-accuracy fail.
- **WINDOW ANSI.** G5 / G5b rust twins live in `repark-spark`
  (`tests::window_temporal_range::*`). The ANSI Q11 TA toll is `TA_FUNCTIONS`, not frame
  bounds. G11 `ansi_door_values.rs` does not cover windows.
- **FLOAT ANSI.** G7 rust twins live in `repark-spark` (`tests::float_agg::*`). No
  Native-profile `f64::to_bits` × `target_partitions` pin exists on the ANSI door.

### Cite honesty (C4)

- Spark `SEMANTICS_CAST_MATRIX` cites the TZ-5 Spark-door `CAST(TIMESTAMP AS BIGINT)` pin,
  not the G6 Python cast-failure corpus (`test_cast_failure_parity.py`). The ID is
  `CAST_MATRIX` (cast/coercion values), and that rust test is the Spark-door CAST value pin
  on this tree. The G6 raise-vs-NULL rust twin does not exist — that residual is §6, not a
  silent fill.
- ANSI `SEMANTICS_SESSION_TIMEZONE` cites the **Native-profile negative**
  (`a_native_session_without_the_spark_extension_reads_the_stored_zone`): stock DataFusion
  extracts in the stored zone. The agree-across-doors test
  (`ansi_door_and_spark_door_agree_under_a_non_utc_session`) runs on a Spark-extended
  session and must not be claimed as Native (the ANSI matrix forbids `SparkExtended`).
- Spark `SEMANTICS_DECIMAL_ARITHMETIC` cites the door-local G-7b pin, not the cross-door
  test: the Spark matrix allows `TwoSession` on `CROSS_DOOR_EQUIVALENCE` only.

---

## 3. Liveness gate

- **Target:** `make check-matrix-test-liveness`
- **Script:** `scripts/check_matrix_test_liveness.py` (wrapper `.sh`)
- **Command:** `cargo test --locked --workspace --lib --tests --bins -- --list`
  (never `--all-features`; doc-tests out of scope)
- **Wired into:** `make ci` (hence `make verify` and `make preflight`)
- **Dual-wire:** ci.yml `rust-test` job step `matrix test-name liveness` runs the same
  script after `make rust-test` (needs compiled test binaries; not the guards job)
- **Fail-closed:** missing matrix, zero extracted cites, zero listed names, cargo
  non-zero, or a dead cite

---

## 4. Provocation proofs (never committed red trees)

Captured 2026-08-14 on this worktree. Restored before commit.

### must-PASS — clean tree

```text
$ ./scripts/check_matrix_test_liveness.sh
matrix-test-liveness: 89 Tested cites live (cargo --list 1529 names)
(exit 0)
```

### must-FAIL — a Tested cite renamed to a ghost

Temporarily replaced the Spark `SEMANTICS_NULL_ORDERING` cite
`spark_ast::tests::order_by_defaults_are_spark` with
`this_test_does_not_exist_g8_liveness`; restored after.

```text
$ ./scripts/check_matrix_test_liveness.sh
ERROR: cited test name is not in `cargo test -- --list`:
  repark-spark: 'this_test_does_not_exist_g8_liveness'
matrix-test-liveness: FAIL — 1 dead cite(s) (89 Tested; --list 1529 names)
(exit 1)
```

---

## 5. Gates

| Gate | Result |
|---|---|
| `make check-matrix-test-liveness` | filled in §7 |
| `make verify` | filled in §7 |
| `make preflight` | filled in §7 |
| Two-pass hygiene | filled in §7 |
| `%ae` | `64240326+TRO-Wolf@users.noreply.github.com` |
| JVM lock | **never taken** |

---

## 6. Handoff (S-5 / next window)

Declared pin-absences — these are the G8 findings, not work deferred by this unit:

1. **`SEMANTICS_JOIN_NULL_KEYS` / Spark door** — need a Rust Spark-door NULL-key join
   value pin (INNER NULL≠NULL + outer-join orphans). G4 Python corpus already exists.
2. **`SEMANTICS_JOIN_NULL_KEYS` / ANSI door** — same class, Native profile. Facade
   cannot reach this door.
3. **`SEMANTICS_WINDOW_FRAMES` / ANSI door** — Native-profile ROWS/RANGE frame-value
   pin. Spark-door G5b twins are not this cell.
4. **`SEMANTICS_FLOAT_DETERMINISM` / ANSI door** — Native-profile `f64::to_bits` ×
   `target_partitions` pin. Spark-door G7 twins are not this cell.

Residual (cited a real CAST pin; G6 rust twin still absent):

5. **G6 cast-failure rust twin on the Spark door** — `CAST('abc' AS INT)` raise-vs-NULL
   is Python-only (`test_cast_failure_parity.py`). The matrix cell cites TZ-5
   `CAST(TIMESTAMP AS BIGINT)` instead. A G6 rust twin would be a stronger cite.

Registry: S-5 owns `docs/spark-sql-iceberg-parity.md`. This unit does not edit it.
The 14-row table above is paste-true classification, not registry text.

---

## 7. Live gate transcript

| Gate | EC | Note |
|---|---|---|
| `./scripts/check_matrix_test_liveness.sh` | **0** | `89 Tested cites live (cargo --list 1529 names)` |
| `make verify` | **0** | includes liveness in `make ci` |
| `make preflight` | **0** | facade `3045 passed, 71 skipped`; audit + zizmor clean |
| JVM lock | never taken | `/tmp/grok-jvm-record.lock` absent |

---

## ACC notes

Actor → Critic-1 (claims accuracy: every cited test name exists) → Critic-2.
`max_cycles=4`, early_stop.

### Critic-1 (claims accuracy)

| ID | Finding | Disposition |
|---|---|---|
| C1-1 | Extractor `t\(` matched the suffix of `absent(` / `audit(` and treated reason strings as cites | **FIXED** — `\bt\(` word boundary; re-extract = 46 + 43 = 89, 0 dead |
| C1-2 | All 10 new Tested names exist on this freeze `--list` | **HOLD** — each quoted in §2 |
| C1-3 | Existing 79 cites still match (exact or last-component) | **HOLD** — gate green on the full 89 |

### Critic-2

| ID | Finding | Disposition |
|---|---|---|
| C2-1 | Spark `CAST_MATRIX` cites TZ-5 timestamp→bigint, not the G6 Python corpus | **HOLD** — ID is CAST_MATRIX; residual G6 rust twin is §6 item 5 |
| C2-2 | ANSI `SESSION_TIMEZONE` cites the Native stored-zone negative, not the Spark-extended agree test | **HOLD** — ANSI matrix forbids `SparkExtended`; Native cite is the honest cell |
| C2-3 | Liveness lives on the rust-test job, not guards | **HOLD** — needs compiled test binaries; dual-wire command matches |
| C2-4 | Four `DeliberatelyAbsent` rows are pin-absences, not product refusals | **HOLD** — reasons + §6 name the trigger; absence pins updated |

Cycle 1 → early_stop (zero open claims-accuracy defects).
