# Unit ledger — Y-10 / H-2 gap G11: the ANSI door, correctness not parity

**Unit:** H-2 gap **G11** of the V2 Engine Hardening campaign · **Date:** 2026-08-12 ·
**Lane:** overnight conductor Y-10 · **Worktree:** `/tmp/grok-y10` · **Branch:**
`grok/y10-g11-ansi-door` · **Base freeze:** `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7`
(PR #65 / L-1 on `origin/main`; A11 — no mid-flight fetch/rebase)

**Charter:** workspace-side `planning/grok/BRIEF-y10-g11-ansi-door.md`
(bound by `BRIEF-overnight-conductor-4.md`, incl. A9). **SEPMO:** STANDARD, acc + C4.
**Executor:** Grok 4.5 (blind Actor).

**Ruling (owner, 2026-08-12, Option A):** Spark is **not** the ANSI door's oracle. The ANSI
door serves standard SQL; matching Spark is the Spark door's job. A door-vs-door divergence
that cannot be justified in writing is a FINDING (this ledger §5 + §6), never silently pinned
as intended.

**Out of scope (honored):** making either door match the other; fixing SUSPICIOUS findings;
Spark/JVM probes (the `/tmp/grok-jvm-record.lock` was **never taken**); registry/live pins;
`timestamp_cast_ansi_door.rs` / `session_timezone_ansi_door.rs` (Y-8 cites them);
`briefs/` / registry file.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Cross-door INTENDED rows (6) | [`crates/repark-sql/tests/cross_door.rs`](../crates/repark-sql/tests/cross_door.rs) | both doors' actual Arrow outputs, one-sentence reason in the row doc comment |
| ANSI-door value pins (6) | [`crates/repark-sql/tests/ansi_door_values.rs`](../crates/repark-sql/tests/ansi_door_values.rs) | native door, standard-SQL oracle |
| Maps | `crates/repark-sql/tests/map.md`, `crates/repark-sql/map.md`, `task/map.md` | lockstep |
| This ledger | `task/y10-ansi-door-ledger.md` | inventory transcript + §6 handoff |

**`tests/mod.rs`:** this crate's `tests/` tree is Cargo integration-test binaries (one `.rs`
file = one `--test` target), the same shape as `cross_door.rs` / `introspection.rs`. There is
no `tests/mod.rs` and creating one would add a broken extra binary. Wiring = the new sibling
file + map.md. A9's "new sibling module" is that file.

### 1.1 Budget

| Bucket | Budget | Landed |
|---|---|---|
| Cross-door INTENDED rows | 4–6 | **6** |
| ANSI-door value pins | 4–6 | **6** |
| Inventory surfaces | overflow, division, ID-1 cite, null order, string→number, reserved words | **all probed** |

### 1.2 Cross-door rows (6) — INTENDED only

Same SQL string through Session A (native `AnsiDialect`, no extension) and Session B
(`SparkDialect` + `SparkExtension`), independent memory catalogs. Arrow path, value AND type
AND nullability.

| Test | SQL | ANSI (actual) | Spark (actual) | Reason (one sentence) |
|---|---|---|---|---|
| `cross_door_integer_division_truncates_on_ansi_is_float_on_spark` | `CAST(5 AS INT) / CAST(2 AS INT)` | Int32 non-null `2` | Float64 **nullable** `2.5` | Standard SQL integer `/` truncates toward zero; Spark `/` is always floating-point (and nullable). |
| `cross_door_integer_div_by_zero_raises_on_ansi_null_on_spark` | `CAST(1 AS INT) / CAST(0 AS INT)` | exec error `Divide by zero` | Float64 nullable NULL | Standard SQL `/ 0` raises; Spark-family `/` promotes integers to float and yields NULL. |
| `cross_door_float_div_by_zero_is_infinity_on_ansi_null_on_spark` | `CAST(1.0 AS DOUBLE) / CAST(0.0 AS DOUBLE)` | Float64 non-null `+Inf` | Float64 nullable NULL | Stock DataFusion / IEEE-754 `/` yields `+Infinity`; the Spark door's `/` yields NULL. |
| `cross_door_decimal_div_by_zero_raises_on_ansi_null_on_spark` | `CAST(1 AS DECIMAL(10,0)) / CAST(0 AS DECIMAL(10,0))` | exec error `Divide by zero` | Decimal128(14,4) nullable NULL | Standard SQL decimal `/ 0` raises; the Spark door yields NULL at the decimal result type. |
| `cross_door_order_by_asc_default_nulls_last_on_ansi_first_on_spark` | `… ORDER BY n ASC` | `1, 2, NULL` | `NULL, 1, 2` | Trino/PostgreSQL-style nulls-sort-high (`ASC` → `NULLS LAST`) vs Spark/Hive nulls-sort-low (`ASC` → `NULLS FIRST`). |
| `cross_door_order_by_desc_default_nulls_first_on_ansi_last_on_spark` | `… ORDER BY n DESC` | `NULL, 2, 1` | `2, 1, NULL` | The same nulls-sort-high vs nulls-sort-low rule on `DESC`. |

Identifier case folding is **not** re-pinned — registry §3 row **ID-1**
(`cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted`) already holds both
doors' quoted-identifier refusal. Cited, not duplicated.

### 1.3 ANSI-door value pins (6) — standard SQL

Native session only. Derived values are the oracle.

| Test | SQL | Pin |
|---|---|---|
| `ansi_door_cast_overflow_int_to_tinyint_raises` | `CAST(200 AS TINYINT)` | raises; needle `Can't cast value 200 to type Int8` |
| `ansi_door_integer_division_truncates_toward_zero` | `CAST(5 AS INT) / CAST(2 AS INT)` | Int32 non-null `2` |
| `ansi_door_integer_division_by_zero_raises` | `CAST(1 AS INT) / CAST(0 AS INT)` | raises; needle `Divide by zero` |
| `ansi_door_sum_skips_nulls` | `SUM(n)` over `{1, NULL, 2}` | Int64 nullable `3` |
| `ansi_door_order_by_asc_defaults_to_nulls_last` | `ORDER BY n ASC` | `1, 2, NULL` |
| `ansi_door_implicit_string_plus_number_refuses` | `'1' + 1` | plan error `Cannot coerce arithmetic expression Utf8 + Int64` |

**Not pinned as a raise:** integer *arithmetic* overflow (`INT_MAX + 1` wraps). That is
FINDING **F-Y10-1**. The overflow pin is CAST overflow, which *does* raise.

---

## 2. Decisions

**D-Y10-1 — Option A is bound.** Spark is not the ANSI oracle. Cross-door rows document
INTENDED dialect splits; ANSI-door pins use standard SQL. No row silently retargets the ANSI
half at Spark.

**D-Y10-2 — A9 sibling file.** Value pins live in `tests/ansi_door_values.rs`, not in
`cross_door.rs`. `cross_door.rs` stays a comparison module. Y-10 is tonight's sole writer of
both files. Y-8's two ANSI-door files are untouched.

**D-Y10-3 — Pin CAST overflow, not arithmetic wrap.** The charter's "ANSI overflow raises"
cell is honest only where the door actually raises (CAST). Wrapping `INT_MAX + 1` is F-Y10-1.

**D-Y10-4 — Float `/ 0` is an INTENDED door split, with a residual.** IEEE `+Inf` vs Spark
NULL is a real dialect reason (D-Y10-1). Strict standard SQL would raise rather than return
Infinity; that residual is F-Y10-2, not a silent "inf is standard SQL" claim.

**D-Y10-5 — Decimal `/ 0` is INTENDED door-vs-door even though DEC-7 is a Spark-door-vs-Apache
Spark ANSI backlog.** The ANSI door's raise is standard-SQL correct. The Spark door's NULL is
the shipped Spark-family behavior (also G13 DEC-7). Justified in writing; not a silent pin of
DEC-7 as "done".

**D-Y10-6 — No `tests/mod.rs`.** Cargo integration-test layout. See §1.

**D-Y10-7 — Never-touch honored.** No registry, no `briefs/`, no JVM lock, no
`timestamp_cast_ansi_door.rs` / `session_timezone_ansi_door.rs`, no production engine change.

---

## 3. §0 inventory

**Method.** Temporary two-session probe (`tests/_y10_probe.rs`, **deleted before commit**) ran
the same SQL through Session A (native ANSI) and Session B (Spark-extended) over independent
`register_memory_catalog` warehouses. No JVM. Transcript below is the probe's verbatim
classification, compacted.

**Surfaces the charter named:** overflow, division (by zero, integer), identifier case
folding (cite ID-1), null ordering defaults, string→number coercion, reserved words.

### 3.1 Classification table

| Surface | SQL | ANSI | Spark | Same/Diff | Class |
|---|---|---|---|---|---|
| i32 add overflow | `CAST(2147483647 AS INT) + CAST(1 AS INT)` | Int32 `-2147483648` | same wrap | SAME | **F-Y10-1** (vs standard SQL raise). Not a door split. |
| i32 sub overflow | `CAST(-2147483648 AS INT) - CAST(1 AS INT)` | Int32 `2147483647` | same | SAME | F-Y10-1 family |
| i32 mul overflow | `CAST(2147483647 AS INT) * CAST(2 AS INT)` | Int32 `-2` | same | SAME | F-Y10-1 family |
| i64 add overflow | `CAST(9223372036854775807 AS BIGINT) + 1` | Int64 `i64::MIN` | same | SAME | F-Y10-1 family |
| i8 add overflow | `CAST(127 AS TINYINT) + 1` | Int8 `-128` | same | SAME | F-Y10-1 family |
| dec38 add overflow | `DECIMAL(38,0) max + 1` | corrupted Decimal128(38,0) | same | SAME | already **DEC-6 / G13** — cite, do not duplicate |
| CAST i32→tinyint | `CAST(200 AS TINYINT)` | raise `Can't cast value 200 to type Int8` | same | SAME | INTENDED-shared correctness; ANSI value pin |
| CAST dec narrow | `CAST(999 AS DECIMAL(2,0))` | raise (max 99) | same | SAME | INTENDED-shared; not separately pinned |
| untyped add | `2147483647 + 1` | Int64 `2147483648` | same | SAME | literals infer Int64; no overflow |
| int `/` 5/2 | `CAST(5 AS INT) / CAST(2 AS INT)` | Int32 non-null `2` | Float64 **nullable** `2.5` | DIFF | **INTENDED** — cross-door row 1. Probe printed collected-batch `null=false`; the pin uses `frame.schema()` (Arrow-path mold) which marks Spark `/` nullable. |
| int `/` 1/0 | `CAST(1 AS INT) / CAST(0 AS INT)` | raise `Divide by zero` | Float64 NULL | DIFF | **INTENDED** — cross-door row 2 |
| untyped `/` 5/2 | `5 / 2` | Int64 `2` | Float64 `2.5` | DIFF | same family as row 1 (not separately pinned) |
| untyped `/` 1/0 | `1 / 0` | raise | Float64 NULL | DIFF | same family as row 2 |
| float `/` 1/0 | `CAST(1.0 AS DOUBLE) / CAST(0.0 AS DOUBLE)` | Float64 `+Inf` | Float64 NULL | DIFF | **INTENDED** door split + **F-Y10-2** residual vs "must raise" |
| float `/` 5/2 | `5.0 / 2.0` as DOUBLE | Float64 `2.5` | same | SAME | control |
| dec `/` 1/0 | `DECIMAL(10,0) / 0` | raise `Divide by zero` | Decimal128(14,4) NULL | DIFF | **INTENDED** — cross-door row 4; Spark half is also DEC-7 |
| dec `/` 5/2 | `DECIMAL(10,0) 5/2` | Decimal128(14,4) `2.5000` | same | SAME | control |
| int `%` 1/0 | `CAST(1 AS INT) % 0` | raise | Int32 NULL | DIFF | same `/ 0` family; not a 7th row |
| unquoted mixed ident | `SELECT Id …` against column `n` | `No field named id` | same | SAME | not the ID-1 fixture |
| quoted wrong case | `SELECT "N"` against `n` | `No field named "N"` | same | SAME | **ID-1 family** — cite, do not duplicate |
| ORDER BY ASC default | `ORDER BY n ASC` | `1, 2, NULL` | `NULL, 1, 2` | DIFF | **INTENDED** — cross-door row 5 |
| ORDER BY DESC default | `ORDER BY n DESC` | `NULL, 2, 1` | `2, 1, NULL` | DIFF | **INTENDED** — cross-door row 6 |
| ORDER BY ASC NULLS FIRST | `… NULLS FIRST` | `NULL, 1, 2` | same | SAME | explicit clause agrees |
| ORDER BY ASC NULLS LAST | `… NULLS LAST` | `1, 2, NULL` | same | SAME | explicit clause agrees |
| ORDER BY n (bare) | `ORDER BY n` | `1, 2, NULL` | `NULL, 1, 2` | DIFF | same as ASC default |
| `'1' + 1` / `1 + '2'` / `'3' * 2` / `'10' / '2'` | implicit coerce | plan `Cannot coerce …` | same | SAME | INTENDED-shared standard SQL; ANSI value pin |
| `CAST('42' AS INT)` | explicit | Int32 `42` | same | SAME | control |
| `CAST('abc' AS INT)` | bad explicit | raise cannot cast `'abc'` | same | SAME | INTENDED-shared |
| `'a' \|\| 'b'` | concat | Utf8 `"ab"` | same | SAME | control |
| `'a' + 'b'` | plus strings | plan coerce fail | same | SAME | not Spark-style concat |
| reserved `AS select/order/from/table/user/date/where/group/limit/interval/end/window` | unquoted alias | accepted | same | SAME | INTENDED-shared sqlparser identifier context; not a door split |
| `AS "select"` / `` AS `select` `` | quoted alias | accepted | same | SAME | both quote styles parse |
| `FROM (SELECT 1 AS n) AS order/select/group` | reserved table alias | accepted | same | SAME | same parser context |
| `5 DIV 2` | Spark integer-div keyword | parse fail (no infix DIV) | **same parse fail** | SAME | observation: Spark door also lacks `DIV`; out of scope (no JVM / no Spark-parity fix) |
| `1 == 1` | Spark eq | Boolean true | same | SAME | both accept `==` |
| `CAST(1 AS STRING/VARCHAR/TEXT)` | type names | Utf8View | same | SAME | both accept Spark-ish and ANSI type names |
| `current_date` / `CURRENT_TIMESTAMP` | reserved fn | Date32 / Timestamp(ns) | same | SAME | both resolve as functions |
| `RLIKE` / `REGEXP` | Spark regex | unimplemented ast node | same | SAME | both refuse |
| `ILIKE` | | Boolean true | same | SAME | both accept |
| `TRY_DIVIDE` / `int_div` / `div` | missing fn | plan invalid function | same refuse, **different "Did you mean"** | DIFF (message only) | not semantic; not pinned |
| `SUM(n)` / `AVG(n)` / `COUNT(*)` / `COUNT(n)` | aggregates | 3 / 1.5 / 3 / 2 | same | SAME | ANSI `SUM` value pin |

### 3.2 Probe excerpt (verbatim, compacted)

```
PROBE div_int_5_2 [DIFF]
  ANSI: OK schema=[v:Int32:null=false] row[0]=[2]
  SPARK:OK schema=[v:Float64:null=false] row[0]=[2.5]
PROBE div_int_1_0 [DIFF]
  ANSI: EXEC_ERR … Arrow error: Divide by zero error
  SPARK:OK schema=[v:Float64:null=true] row[0]=[NULL]
PROBE div_float_1_0 [DIFF]
  ANSI: OK schema=[v:Float64:null=false] row[0]=[inf]
  SPARK:OK schema=[v:Float64:null=true] row[0]=[NULL]
PROBE div_dec_1_0 [DIFF]
  ANSI: EXEC_ERR … Arrow error: Divide by zero error
  SPARK:OK schema=[v:Decimal128(14, 4):null=true] row[0]=[NULL]
PROBE ord_asc_default [DIFF]
  ANSI: … row[0]=[1] row[1]=[2] row[2]=[NULL]
  SPARK:… row[0]=[NULL] row[1]=[1] row[2]=[2]
PROBE ord_desc_default [DIFF]
  ANSI: … row[0]=[NULL] row[1]=[2] row[2]=[1]
  SPARK:… row[0]=[2] row[1]=[1] row[2]=[NULL]
PROBE ovf_i32_add [SAME]
  ANSI: OK schema=[v:Int32:null=false] row[0]=[-2147483648]
  SPARK:OK schema=[v:Int32:null=false] row[0]=[-2147483648]
PROBE ovf_cast_i32_to_tinyint [SAME]
  ANSI: EXEC_ERR … Can't cast value 200 to type Int8
  SPARK:EXEC_ERR … Can't cast value 200 to type Int8
PROBE coer_plus [SAME]
  ANSI: PLAN_ERR: Cannot coerce arithmetic expression Utf8 + Int64 to valid types
  SPARK:PLAN_ERR: Cannot coerce arithmetic expression Utf8 + Int64 to valid types
```

The probe file was deleted after transcription. It is not in the tree.

---

## 4. Gate evidence

JVM lock: **never taken**. `/tmp/grok-jvm-record.lock` existed (MARKER=`y6-g10-boundary`,
Y-6) and was left untouched. The only SparkSubmit process visible was the standing
containerized thrift server, which the conductor says to ignore.

### 4.1 Targeted Rust

```
cargo test -p repark-sql --test ansi_door_values  → 6 passed
cargo test -p repark-sql --test cross_door        → 19 passed (13 prior + 6 G11)
```

### 4.2 `make verify` (JVM-free)

`/tmp/y10-verify.log` — **EXIT:0**. Includes rust-fmt-check, rust-clippy, rust-panic-ban,
check-crate-dag, check-lib-rs, check-rust-file-size, check-lib-py, check-manifest,
check-parity-live-dual-wire, rust-check, py-lint, py-format-check, py-lock-check,
toml-check, spell-check, and `cargo test --workspace --locked`. `cross_door` 19/19,
`ansi_door_values` 6/6.

### 4.3 `make preflight`

`/tmp/y10-preflight.log` — **EXIT:0**. verify + `py-test-facade` (`2822 passed, 71 skipped`)
+ cargo-deny/pip-audit + workflows-lint/zizmor.

File sizes: `cross_door.rs` 1209 ≪ 1500; `ansi_door_values.rs` 265 ≪ 1500. No EXCEPTIONS.

---

## 5. Findings (SUSPICIOUS / residuals — do not fix tonight)

### F-Y10-1 — integer arithmetic overflow wraps (both doors)

- **Observed:** `CAST(2147483647 AS INT) + CAST(1 AS INT)` yields Int32 `-2147483648` through
  **both** doors (two's complement wrap). Same for sub/mul, i64, i8.
- **Standard SQL:** overflow shall raise an exception.
- **Class:** SUSPICIOUS vs the ANSI door's standard-SQL oracle. **Not** a door-vs-door
  divergence (they agree). **Not** pinned as intended.
- **Handoff:** G13 (expression-level arithmetic overflow) already owns the decimal face
  (DEC-6). This is the integer analog. Morning: extend G13, do not launder wrap into "ANSI
  overflow is wrap".

### F-Y10-2 — ANSI float `/ 0` is IEEE `+Inf`, not a raise

- **Observed:** ANSI door returns non-null Float64 `+Infinity`; Spark door returns NULL.
- **Door-vs-door:** INTENDED (IEEE vs Spark-family null-on-div0) — pinned.
- **Vs strict standard SQL:** a raise would be more correct than Infinity. Residual only;
  do not "fix" the Spark door to Inf or the ANSI door to NULL overnight.

### Not findings

- **ID-1** quoted-identifier case folding — already declared; cited.
- **DEC-6 / DEC-7** — already in the registry; decimal `/ 0` door split is additionally an
  INTENDED G11 row (D-Y10-5).
- **Unquoted reserved aliases accepted** — shared sqlparser identifier context; not a door
  split; not a silent correctness claim that ANSI reserved-word enforcement is strict.
- **Spark `DIV` parse-fails on both doors** — Spark-parity observation; out of scope (no JVM).
- **`Did you mean` text** on missing functions — incidental registry difference; not pinned.

---

## 6. Handoff (paste-true; do not land from this unit)

`briefs/` and `docs/spark-sql-iceberg-parity.md` are never-touch for this lane.

### Registry ruling text

> **G11 closed: not parity — correctness; intended divergences pinned in
> `cross_door.rs`.** Spark is not the ANSI door's oracle (owner ruling 2026-08-12, Option A).
> The ANSI door serves standard SQL; matching Spark is the Spark door's job. Six INTENDED
> door-vs-door splits are pinned in
> `crates/repark-sql/tests/cross_door.rs` (`cross_door_integer_division_*`,
> `cross_door_*_div_by_zero_*`, `cross_door_order_by_*`). Six ANSI-door standard-SQL value
> pins live in `crates/repark-sql/tests/ansi_door_values.rs`. Identifier case folding remains
> registry row ID-1 (cited, not duplicated).

### Dated slate-amendment line (orchestrator)

> **2026-08-12 Y-10 / G11:** close G11 as a *parity* gap under Option A (correctness, not
> Spark-parity). Intended door-vs-door divergences are pinned in `cross_door.rs` (G11
> section). ANSI-door value coverage is `tests/ansi_door_values.rs`. Carry **F-Y10-1**
> (integer arithmetic overflow wraps on both doors) onto G13 as the integer analog of DEC-6;
> do not pin wrap as intended. Carry **F-Y10-2** as a residual (ANSI float `/ 0` is IEEE Inf
> rather than a standard-SQL raise). Do not edit `briefs/` from a lane.

---

## 7. Authorship

Per-command `-c user.name=TRO-Wolf -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer: `Authored-By: Grok (grok-4.5) <noreply@x.ai>`. After every commit
`git log -1 --format='%ae'` == `64240326+TRO-Wolf@users.noreply.github.com`. Two-pass hygiene
before stop. No push, no PR, no merge.
