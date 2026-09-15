# Unit ledger — JAVA-DOUBLE-FD-1 · JDK 17 FloatingDecimal port for DOUBLE/FLOAT text

**Date:** 2026-09-15 · **Branch:** `feat/java-double-fd-1` · **Base:** `4bd43fc8`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Registry JAVA-DOUBLE-FD-1: the engine's Java-shaped formatter answers the
shortest round-trip digits where JDK 17 `Double.toString` / `Float.toString` answer
longhand (`8.41E21` → `8.409999999999999E21`, `1.0E23` → `9.999999999999999E22`,
`5.0E-324` → `4.9E-324`), and `format_string('%.2f', 0.125)` answers `0.12`
(half-even) where Java `Formatter` (HALF_UP) answers `0.13`. Owner ruling Q-15c-2
orders a port of JDK 17 `FloatingDecimal` digit generation. The spec is the recorded
corpus `/tmp/oc-worker/qc-oracle/fixtures-batch13-jdk17-tostring.json` (29,451 doubles,
9,976 floats, JDK 17.0.15) plus `fixtures-batch10.json` cells `J10-fd-8.41E21`,
`J10-fd-more`, `J10-format-string-f`, plus the run-16a suffix-cast oracle
`/tmp/oc-worker/qa-613-crit2/oracle/deg_inf_spark_oracle.json`.

**Rulings.**

| ID | Source | Ruling |
|---|---|---|
| Q-15c-2 | owner, 2026-09-15 | Port JDK 17 `FloatingDecimal` so DOUBLE/FLOAT text is byte-identical to Spark. |
| R-15c-11 | 15c | The BACKLOG row this unit closes is JAVA-DOUBLE-FD-1. |
| D-1 | actor, 2026-09-15 | The port is an independent Rust implementation written from the published algorithm structure (`dtoa` fast long path, `estimateDecExp`, `FDBigInteger` slow path, `insignificantDigitsForPow2`, `roundup`, `getChars` layout). No GPL source text is copied; the reference files stay in `/tmp/fd-ref`, outside the repo. |
| D-2 | actor, 2026-09-15 | The corpus test reads the fixture through `REPARK_JDK17_TOSTRING_CORPUS` and skips when unset; the in-tree table carries the 82 non-shortest doubles, the counted non-shortest floats and 200 random rows so CI holds the claim without the file. |
| D-3 | actor, 2026-09-15 | `format_string`/`printf` `%f`/`%e` HALF_UP lands as a narrow analyzer rewrite: a bare single-verb `%[.N]f`/`%[.N]e` literal over a float arg renders through a repark UDF with exact-decimal HALF_UP; mixed formats pre-round float args feeding `%f` verbs through a HALF_UP UDF so the downstream kernel's rounding is exact. `%g`/`%a`/flags/width stay on the upstream kernel and are a residue row. |
| D-4 | actor, 2026-09-15 | C-006 (STRING → DOUBLE suffixed casts) is a narrow fold fix: the pre-coercion seat strips a single trailing Java type suffix (`dDfF`) before parsing, after Arrow rejects the literal. Accepting shapes stay pinned. |

**Not in this round:** `STATUS.md`, any `Cargo.toml` / `Cargo.lock` / `pyproject.toml` /
`uv.lock` / `.github/` change, anything under `python/repark/src/repark/functions*.py`,
`python/repark/src/repark/spark/functions*.py`, `dataframe/**`, `column.py`, `catalog.py`,
`session/**` (runs 16a/16b own those; facade halves outside the fence are P2 hand-offs).
Pin inputs use `CAST(<text> AS DOUBLE|FLOAT)` or DataFrame columns, never a bare
exponent literal (FNP-4B).

## PROPOSITION LEDGER — JAVA-DOUBLE-FD-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Rust port of JDK 17 `FloatingDecimal` `dtoa` (fast long path, `estimateDecExp`, `FDBigInteger` slow path, `insignificantDigitsForPow2`, `roundup`) and `toJavaFormatString` layout for double and float behind the existing `java_double.rs` entry points; no `unsafe`, no new dependency; big-integer arithmetic ported, not replaced by a crate. | `cargo test -p repark-functions java_double` green with the corpus variable set, all 39,427 rows byte-equal; review confirms no verbatim copy. | **OPEN** | Port lands in step 2. |
| C-002 | Rust test loads the corpus file when present (env var, skipped otherwise) and asserts byte equality on all 39,427 rows; in-tree table holds at least the 82 non-shortest doubles, the non-shortest floats and 200 random rows. | Corpus test + in-tree table green in CI without the file; red-first output below. | **OPEN** | Red pins land in step 1. |
| C-003 | `format_string`/`printf` `%.Nf`/`%e` round HALF_UP as Java `Formatter` (J10-format-string-f); anything needing more than a narrow change is a residue row with a cell. | J10-format-string-f pins flip to equality; residue row filed if taken. | **OPEN** | Narrow rewrite lands in step 3. |
| C-004 | Perf keeps #612's bar (1.54x Arrow cast, 5M rows, best of 3, release native, both doors); before/after numbers pasted. | Both numbers in evidence. | **OPEN** | Before numbers in step 1; after in step 4. |
| C-005 | Registry JAVA-DOUBLE-FD-1 → FIXED 2026-09-15; pins `test_spark_door_jdk_longhand_backlog` and `test_spark_door_format_string_f_is_todays_answer` flip to equality; both doors pinned (SQL `CAST(d AS STRING)` and `F.col(d).cast("string")`) for the non-shortest cells. | Registry section + green pins. | **OPEN** | Step 5. |
| C-006 | STRING → DOUBLE/FLOAT casts of Java type-suffixed text (`1d`, `1f`, `1.5D`) answer `1.0` on both doors, ANSI on and off; every cast cell in the run-16a oracle pinned both doors for value AND type, accepting shapes included; Rust unit test beside the fix. | New pins green; registry FIXED row JAVA-DOUBLE-CAST-SUFFIX-1. | **OPEN** | Narrow fold fix lands in step 2. |

## Red-first evidence — 2026-09-15

TBD in step 1: corpus-test failing counts (expect 82 double mismatches plus the
counted float mismatches), Python pin failures, before-perf numbers.

## Coverage attestation

TBD when all clauses read PROVEN.
