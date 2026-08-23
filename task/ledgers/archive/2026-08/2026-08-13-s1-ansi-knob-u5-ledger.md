# Unit ledger — S-1 / U5: `spark.sql.ansi.enabled` default TRUE + DEC-7 `/0`

**Unit:** S-1 · campaign U5 (DEC-6/7 + DEC-9 rider) · **Date:** 2026-08-14 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-s1` · **Branch:** `grok/s1-ansi-knob-u5` ·
**Base (FROZEN):** `d9a739123be8b00bc1fc1e6d4bbad875ba6caa76`
(`feat(decimal): U3 fromLiteral min-precision + U4a SparkDecimalPrecision clamp (#91)`)

**Charter:** `BRIEF-s1-ansi-knob-u5.md` + `DEC-DESIGN.md` §1.6/1.7/1.9, §4.1 U5,
Q10–Q14 + BRIEF-y9 addendum (Q10=A default TRUE; Q11=A; Q12=A; Q14 fold DEC-9
into U5) + conductor-8 A1–A9. **SEPMO:** HIGH — octo + C4.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` — S-5 owns the
registry. DEC-6/7/9 row texts are paste-true in §6.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | `spark.sql.ansi.enabled` defaults TRUE on the Spark door; `false` restores NULL `/0`. | PROVEN — `configure_defaults_ansi_enabled_true` + `configure_honors_ansi_enabled_false` + passthrough / decimal twins. |
| C-002 | ANSI door / native G11 session builder untouched. | PROVEN — no `session.rs` / ANSI-door builder edits; G11 ANSI halves still raise / Inf. |
| C-003 | DEC-7 `/0` and A2 `% 0` share `guard_zero_divisor`. | PROVEN — one function, both call sites; analyzer_rules() order unchanged. |
| C-004 | DEC-6 overflow raise lands only with an honest ≤unit hook; else DECLARE. | PROVEN — §0.3 DECLARE with 38-nines wrap pin (U4b mold). |
| C-005 | DEC-9 rides only on a recorded row; else named residue. | PROVEN — §0.4 residue (CAST-after does not flip nullability). |
| C-006 | Type-validation seam located first; `notabool` fail-loud. | PROVEN — §0.1; message needle pinned; IllegalArgument class DECLARED. |
| C-007 | New sibling ConfigExtension, not `ReparkSqlSettings`. | PROVEN — §0.2; `ansi.rs` `PREFIX = repark.ansi`. |
| C-008 | Registry / `_live_parity.py` / `test_parity_live.py` / lockfiles / session.rs untouched. | PROVEN — diff names. |
| C-009 | Both-knob-state records under the JVM lock. | PROVEN — §3 lock table. |
| C-010 | `make verify` + `make preflight` exit 0. | PROVEN — §4. |

---

## 0. Blast + seams + land-or-declare

### 0.1 Type-validation seam (A1 — locate FIRST)

**Documented contract** (`crates/repark-common/src/lib.rs` 71–76): Spark rejects
`spark.sql.ansi.enabled=notabool` with JVM `IllegalArgumentException` /
`should be boolean, but was notabool`. `Error::Config` routes to
`ErrorClass::IllegalArgument`.

**Live seam today:**

| Layer | What happens |
|---|---|
| `ReparkSessionBuilder::config` (`session.rs:281–285`) | Stores the pair. Unknown `spark.sql.*` keys are **ignored** (PySpark-tolerant). **CLOSED** — do not edit. |
| `apply_datafusion_config_keys` | Fail-loud `Error::Config` for unknown `datafusion.*` only. |
| `SparkExtension::configure` | `datafusion::error::Result`; `build()` folds via `engine_err`. |
| `engine_err` (`error_map.rs`) | `Plan` → `Analysis`; `Configuration` → `Error::DataFusion` (Base). **Never** `Error::Config`. |

**Taken hook:** `repark_functions::ansi::parse_spark_sql_ansi_enabled` +
`spark_ansi_from_config_map`, called from `configure()`. Fail-loud
`DataFusionError::Configuration` with the Spark needle. Python surfaces as
base `PySparkException`, message contains `should be boolean, but was notabool`.

**DECLARE (validation class gap):** the IllegalArgument *class* lives in
`session.rs` / `Error::Config` (CLOSED). We do not invent a fold.

### 0.2 ConfigExtension vs `ReparkSqlSettings`

**Sibling `SparkAnsiConfig` in `crates/repark-functions/src/ansi.rs`.**

Why not `ReparkSqlSettings` / `cardinality.rs`:

1. Different namespace: Spark `SQLConf` (`spark.sql.ansi.enabled`) vs `repark.sql.*`
   safety ceilings. Mixing is a second spelling.
2. A1 prefers a sibling.
3. `PREFIX = "spark"` would swallow every `spark.*` `SET` (including
   `spark.sql.session.timeZone`). `PREFIX = "repark.ansi"` matches the
   session-timezone carrier (two-segment, `SET`-proof).
4. `ExtensionOptions::set` refuses, naming the builder key — one spelling.

Missing carrier (bare `SessionContext` + `SparkExprSemantics`) defaults **TRUE**
— the analyzer *is* the Spark-door semantics layer. `setup()` now installs the
carrier ON so Rust fixtures match `SparkExtension`.

Runtime `spark.conf.set` after `getOrCreate` remains store-only (existing
facade pattern for most `spark.sql.*`; `RuntimeConfig` CLOSED). The real knob
is builder `.config("spark.sql.ansi.enabled", "false")`.

### 0.3 DEC-6 overflow raise — DECLARE (U4b mold)

**DECLARE DEC-6.** Knob + DEC-7 `/0` is the unit. Overflow raise does not land.

Why there is no honest ≤unit hook on an allowed file (executed, not guessed):

1. **Allowed files cannot see the overflow.** `guard_zero_divisor` owns `/` and
   `%` only. CAST / array / overlay arms CLOSED. `decimal_precision.rs` is
   DEC-9 rider ONLY. U4a CAST-after fires only when Spark `(p,s)` ≠ Arrow
   `(p,s)`. `(38,0)+(38,0)` unbounded is `(39,0)` → both clamp to `(38,0)` —
   **no CAST is inserted**. The wrap is Arrow `decimal_op` at **execution**.
2. **Photographed wrap (U2 leftover, still live).**
   `CAST(999…9 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))` returns
   `10^38` at declared `decimal128(38,0)` — Rust
   `pin_overflow_max_decimal38_plus_one_wrong_value_i128` (i128 = `10^38`);
   Python `overflow_max_decimal38_plus_one_raises_in_spark` (spark_raises,
   repark `_dec_raw_i128(38, 0, 10**38)`).
3. Closing it needs an execution-time overflow check / `try_*` UDF / ExprPlanner
   — the U4b mold (different altitude, not a one-file CAST wrap).

Silent omit forbidden (A3): this paragraph **is** the declaration.

### 0.4 DEC-9 nullability — named residue

Spark marks `CAST(9 AS DECIMAL(1,0))*CAST(9 AS DECIMAL(1,0))` nullable under
**both** ANSI ON and OFF (`DEC-DESIGN.md` §2.6). Not a Q10 leftover.

Tried hook: wrap overflow-capable `+ − *` in CAST. Existing CAST-after pins
(`pin_add_38_18_clamps_to_38_17_i128`) stay **non-null** — DataFusion
`ExprSchemable` keeps CAST of two non-null literals non-null. A
`CASE WHEN false THEN NULL ELSE expr` would smash every equality add/mul
nullability pin (`_dec(..., nullable=False)`).

No recorded row *after a safe rider* proves a flip. **Named residue.** The
three G13 nullability disclosures stay.

### 0.5 `/0` + `% 0` policy (must land)

`guard_zero_divisor(divisor, type, ansi_enabled)`:

- ANSI ON → `__repark_ansi_nonzero_divisor__(divisor)`; zero raises
  `[DIVIDE_BY_ZERO] … (ArithmeticException)`.
- ANSI OFF → `nullif(divisor, 0)` (legacy).
- Call sites: `rewrite_division` **and** `rewrite_modulo` (A2). Formulas CLOSED.
- Integer `/` still promotes both sides to `Float64` then guards the Float64
  divisor — so `1/0` raises instead of IEEE Inf.

`analyzer_rules()` order **UNCHANGED**:
`SparkDecimalPrecision → SparkExprSemantics → cardinality → instant_ts`.

### 0.6 Blast list

| Site | Legacy pin | Disposition |
|---|---|---|
| `test_decimal128_parity.py` `/0` 38 + small | spark raise / repark NULL | OWNED: flip ON to shared-raise; add OFF twins (record) |
| `test_decimal128_parity.py` overflow | spark raise / repark wrap | DECLARE DEC-6; pin kept |
| `test_decimal128_parity.py` DEC-9 ×3 | nullability disclosure | RESIDUE |
| `test_sql_passthrough_parity.py` `:54–62` + DIVERGENCE `1/0` `%0` | NULL | OWNED: default raise + OFF twins |
| `analyzer.rs` unit `/0` `%0` | NULL | OWNED: default raise + `ctx_legacy` |
| `decimal.rs` `/0` | NULL | OWNED: raise + `setup_with_ansi(false)` |
| `cross_door.rs` Spark `/0` ×3 | NULL | BLAST: Spark half now raises; **names kept** (G11 citations) |
| `ctas.rs` UNION `/0` | NULL write-path | BLAST: `setup_with_ansi(false)` — tests UNION-NULL, not ANSI policy |
| `test_ctas_division_writeback.py` `/0` UNION | NULL | BLAST: builder `ansi=false` |
| `test_f2_fail_value.py` float `/0` | NULL | BLAST: expect raise |
| `element_at` OOB / `[]` neg / `substr` bounds | NULL | **DEFER** — CAST/array/string arms CLOSED |
| `CAST('abc' AS INT)` | repark already raises | **DEFER** — CAST arm CLOSED; already matches Spark ANSI |
| `datetime.rs` “ansi=false our default” comment | stale docs | **DEFER** — file CLOSED |
| `F.expr("1/0")` under builder `ansi=false` | would raise | **RESIDUE** — `F.expr` parses standalone against the default TRUE carrier; session analyzer then sees the already-embedded UDF. spark.sql honors the knob. |
| Python `conf.get("spark.sql.ansi.enabled")` default | unset | **DEFER** — `_SQLCONF_DEFAULTS` CLOSED |
| Runtime `spark.conf.set` after build | store-only | **DEFER** — `RuntimeConfig` CLOSED |
| IllegalArgument class for `notabool` | Base exception | **DECLARE** §0.1 |

---

## 1. Engine change

- New `crates/repark-functions/src/ansi.rs` — carrier + parse + raise UDF.
- `SparkExprSemantics` reads `ConfigOptions`; `guard_zero_divisor` branches.
- `SparkExtension::configure` installs the carrier (default TRUE).
- `common::setup` → `setup_with_ansi(true)`; `setup_with_ansi(false)` for
  legacy twins. `setup_allow_local_fs_ddl` also ON.
- `check_lib_rs.py` `repark-functions` ceiling 166 → 175 (`pub mod ansi;`).

## 2. Tests

Owned: `decimal.rs`, `test_decimal128_parity.py` + record driver,
`test_sql_passthrough_parity.py`, `extension/tests.rs`, `ansi.rs` unit tests,
`analyzer.rs` unit tests.

Blast-forced pin flips: `cross_door.rs` (names kept), `ctas.rs`,
`test_ctas_division_writeback.py`, `test_f2_fail_value.py`.

Shared-raise shape: `spark_raises` + both tables `None`. G13 budget max 12.

## 3. JVM lock

| Event | Marker | pid | TS | Action |
|---|---|---|---|---|
| acquire | `s1-record` | 1862201 | 2026-08-13T19:47:30-04:00 | noclobber create `/tmp/grok-jvm-record.lock` |
| refresh | `s1-record` | 1864455 | 2026-08-13T19:47:4x | same lock, new pid; marker-verify `s1-` |
| record | `s1-record` | 1864455 | 2026-08-13 | both knob states; `PROBE_EC=0` |
| release | `s1-record` | 1864455 | trap EXIT | `rm` after marker-verify `pid=$$`; no foreign rm |

Live 4.1.2 (zulu-17, `local[2]`):

- ANSI ON: `/0` 38 + small raise `ArithmeticException` / `DIVIDE_BY_ZERO`; overflow raises `NUMERIC_VALUE_OUT_OF_RANGE`.
- ANSI OFF: `/0` 38 → `decimal128(38,6)` NULL; `/0` small → `decimal128(8,6)` NULL; overflow → `decimal128(38,0)` NULL.

No stale-rm of a foreign marker. No leftover SparkSubmit/pyspark.

## 4. Gates

`make verify` and `make preflight` in `/tmp/grok-s1`, exit 0.

## 5. Identity / hygiene

Per-command `user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer: `Authored-By: Grok (grok-4.5) <noreply@x.ai>`.
Two-pass hygiene count 0.

## 6. Paste-true registry handoff (S-5)

Do **not** land from this PR. S-5 owns `docs/spark-sql-iceberg-parity.md`.

### DEC-7 (divide-by-zero) — READY TO PASTE as FIXED

```
DEC-7 divide-by-zero — FIXED (S-1 / U5, 2026-08-14)
Spark door `spark.sql.ansi.enabled` defaults TRUE (Spark 4 / Q10=A).
`guard_zero_divisor` (rewrite_division AND rewrite_modulo) raises
`[DIVIDE_BY_ZERO]` / ArithmeticException when the knob is true, and
restores NULL via `nullif` when false.
Pins: `test_decimal128_parity.py::test_decimal128_row_matches_spark_or_still_diverges[div_by_zero_decimal38_raises_in_spark_null_in_repark]`
(shared-raise; name kept);
`[div_by_zero_decimal38_null_when_ansi_false]` (both NULL; type still
Arrow (38,4) vs Spark (38,6) — U4b);
same pair for `div_by_zero_small_decimal_*`;
`crates/repark-spark/src/tests/decimal.rs::pin_div_by_zero_decimal38_raises_under_default_ansi`
+ `pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false`;
`test_sql_passthrough_parity.py` `/0` and `% 0` both knob states.
ANSI door untouched (G11).
```

### DEC-6 (ANSI overflow) — READY TO PASTE as DECLARED

```
DEC-6 ANSI overflow — DECLARED (S-1 / U5, 2026-08-14)
No honest ≤unit hook on an allowed file. `(38,0)+(38,0)` has the same
Spark and Arrow result type `(38,0)`, so U4a CAST-after does not fire.
Arrow `decimal_op` wraps to `10^38` at declared `decimal128(38,0)`.
Evidence: `overflow_max_decimal38_plus_one_raises_in_spark` (spark_raises
ArithmeticException; repark `_dec_raw_i128(38, 0, 10**38)`);
`pin_overflow_max_decimal38_plus_one_wrong_value_i128` i128=`10^38`.
Closing needs an execution-time overflow check (U4b mold). Not a silent omit.
```

### DEC-9 (overflow-capable nullability) — READY TO PASTE as residue

```
DEC-9 overflow-capable nullability — NAMED RESIDUE (S-1 / U5, 2026-08-14)
Spark marks `9*9` / `9+9` / `999*999` nullable under BOTH ANSI modes
(DEC-DESIGN §2.6). CAST-after does not flip DataFusion nullability
(`pin_add_38_18` stays non-null). A CASE-NULL wrap would smash equality
add/mul non-null pins. Three G13 disclosures kept:
`mul_single_digit_nullability_differs`,
`add_single_digit_nullability_differs`,
`mul_three_digit_capacity_nullability_differs`.
```

### G11 cross-door `/0` — READY TO PASTE (note only)

```
G11 Spark-door `/0` halves flipped with U5 (names kept):
`cross_door_integer_div_by_zero_raises_on_ansi_null_on_spark` — both raise;
`cross_door_float_div_by_zero_is_infinity_on_ansi_null_on_spark` — ANSI Inf,
Spark DIVIDE_BY_ZERO;
`cross_door_decimal_div_by_zero_raises_on_ansi_null_on_spark` — both raise.
ANSI-door builder untouched.
```
