# Unit ledger — A11 ANSI-door CREATE nanosecond-timestamp reject

**Unit:** A11 · TZ-4 remainder · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-m16` · **Branch:** `grok/a11-ansi-ns-reject` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`
(`docs(ta): truth-up the TYPPRICE oracle note and the stale pre-N2 plan claim (#114)`)

**Charter:** `planning/grok/BRIEF-stretch-q10-a11.md` A11 + conductor-13 **A13**.
**SEPMO:** STANDARD — acc + C4. Sequential hat-switch. Floor S1.

Does **not** edit `docs/spark-sql-iceberg-parity.md`, `STATUS.md`, lockfiles,
or `planning/hardening/unit-queue.md` (orchestrator / registry closed).

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Recon: the refuse at freeze is Iceberg v2 `Invalid type for {col}: timestamp_ns is not supported until v3` after `arrow_schema_to_schema_auto_assign_ids`, not a DDL needle. | PROVEN — red run of `ansi_column_def_nanosecond_timestamp_shapes_refuse` on the unfixed tree (verbatim: `Invalid schema for v2: Invalid type for ts: timestamp_ns is not supported until v3`). |
| C-002 | The seam is ANSI column-def `CREATE TABLE` in `repark-sql/src/create_table.rs` (`column_def_schema` via `CAST(NULL AS <type>)`). | PROVEN — `create_table.rs` + DataFusion `planner.rs:743-764`. |
| C-003 | HALT conditions do not fire: DDL refuse did **not** already exist; the fix is not write-path / fork. | PROVEN — C-001 + diff names (no `repark-iceberg`, no fork, no Spark door). |
| C-004 | y8 addendum / TZ4-DESIGN do **not** fold CTAS or Spark-door remapping into A11. Scope is CREATE column-def only. | PROVEN — BRIEF-stretch A11 + y8 Q1/Q2 (Spark mapping already landed #79); unit-queue remainder is "ANSI-door CREATE ns-reject". |
| C-005 | Named ns shapes refuse at DDL time naming column + precision 9 + `TIMESTAMP(6)`. | PROVEN — `ansi_column_def_nanosecond_timestamp_shapes_refuse` + unit helper pins. |
| C-006 | `TIMESTAMP(6)` / `TIMESTAMP(6) WITHOUT TIME ZONE` CREATE is unchanged (`timestamp[us]`). | PROVEN — `ansi_column_def_timestamp_6_create_is_unchanged` was green on the unfixed tree and stays green. |
| C-007 | Spark door stays `TIMESTAMP` → Iceberg `timestamptz` (documented, not changed). | PROVEN — no `crates/repark-spark` diff; cross-door note on the session_wiring test + this ledger. |
| C-008 | CTAS / ALTER / write-path / fork / registry / STATUS / lockfiles closed. | PROVEN — refuse is called only on the column-def arm (`query.is_none()`). Diff names. |
| C-009 | A refused CREATE leaves no table. | PROVEN — leftover `SELECT` after each named shape is `Err`. |
| C-010 | `map.md` / ledger land in the same change. Tests land with the code. | PROVEN — this file + listed maps. |

---

## 0. Recon (binding)

### 0.1 Why this is not a HALT

Freeze `cd0db4f` residual pin
`ansi_column_def_timestamp_still_rejects_ns_on_v2` asserted the **write-path**
Iceberg v2 check (`timestamp_ns` + `v3`). That is the TZ-4 remainder A11 is
chartered to replace, not evidence the unit is already done.

The derivation is local to this door:

1. `column_def_schema` plans `CAST(NULL AS <declared type>)`.
2. DataFusion 54.1 maps `TIMESTAMP` / `TIMESTAMP(9)` (any TZ spelling) →
   `TimeUnit::Nanosecond`; `TIMESTAMP(6)` → `Microsecond`
   (`datafusion-sql-54.1.0/src/planner.rs:743-764`).
3. Fork `arrow_schema_to_schema_auto_assign_ids` maps ns → `timestamp_ns` /
   `timestamptz_ns`; µs → `timestamp` / `timestamptz`.
4. Iceberg `Schema::check_compatibility` then refuses v2.

A11 inserts a Plan refuse between steps 1 and 3 on the **column-def arm only**.

### 0.2 Named shapes (DataFusion-legal ns)

| SQL spelling | Arrow at CAST | After A11 |
|---|---|---|
| `TIMESTAMP` | `timestamp[ns]` | DDL refuse |
| `TIMESTAMP(9)` | `timestamp[ns]` | DDL refuse |
| `TIMESTAMP WITH TIME ZONE` | `timestamp[ns]` (native session: tz `None`) | DDL refuse |
| `TIMESTAMP(9) WITH TIME ZONE` | same | DDL refuse |
| `TIMESTAMP WITHOUT TIME ZONE` | `timestamp[ns]` | DDL refuse |
| `TIMESTAMP(9) WITHOUT TIME ZONE` | same | DDL refuse |
| `TIMESTAMP(6)` | `timestamp[us]` | unchanged CREATE |
| `TIMESTAMP(6) WITHOUT TIME ZONE` | `timestamp[us]` | unchanged CREATE |

`TIMESTAMP(0)` / `TIMESTAMP(3)` stay DataFusion Second/Millisecond and still
fail later as unsupported Arrow types — **not** this unit (ns only).
`TIMESTAMP_NTZ` is Spark syntax; DataFusion `Unsupported SQL type`.

### 0.3 Out of scope (do not widen)

- **CTAS** — y8/TZ4-DESIGN do not put CTAS in A11. Instant-producer CTAS
  already stores `timestamptz` on the Spark door (#79). ANSI CTAS of a
  leftover ns producer still hits the write-path check.
- **ALTER ADD COLUMN / SET DATA TYPE** — shared `sql_type_to_iceberg` is
  untouched; still the late Iceberg refuse if someone adds an ns column.
- **Spark door** — already maps `TIMESTAMP` → `timestamptz` (live CREATE
  probe 2026-08-13). Documented, not edited.
- **Remapping ns → µs** — that would be a silent type lie on the ANSI door
  (DataFusion's TIMESTAMP **is** ns). A11 is refuse-loud.

### 0.4 Red-then-green

Unfixed tree (`refuse_nanosecond_timestamp_columns` not yet called):

```
must name column `ts`: DataInvalid => Invalid schema for v2:
- Invalid type for ts: timestamp_ns is not supported until v3
test ansi_column_def_nanosecond_timestamp_shapes_refuse ... FAILED
test ansi_column_def_timestamp_6_create_is_unchanged ... ok
```

After the refuse: named-shape pin green; µs positive still green; reverting
the call site re-reds the named-shape pin on the write-path needle.

---

## 1. Implementation

- `crates/repark-sql/src/create_table.rs` —
  `refuse_nanosecond_timestamp_columns` after `column_def_schema` on the
  `query.is_none()` arm. `DataFusionError::Plan`; names `` `{column}` ``,
  `nanosecond precision (9)`, `Supported precisions: 6 (microseconds)`,
  `Declare TIMESTAMP(6)`.
- No write-path, fork, Spark-door, lockfile, STATUS, or registry edits.

---

## 2. Tests

| Home | Tests |
|---|---|
| `create_table/tests.rs` | helper: ns refuse needles; zoned-ns refuse; µs (+ tz) pass |
| `tests/session_wiring.rs` | six named SQL shapes + mixed list; leftover SELECT is err; `TIMESTAMP(6)` / `TIMESTAMP(6) WITHOUT TIME ZONE` succeed at `timestamp[us]` |

Replaced residual `ansi_column_def_timestamp_still_rejects_ns_on_v2` (write-path
`timestamp_ns`+`v3`) with the DDL-time named-shape pin. Same file, same
reachability binary.

Maps: `crates/repark-sql/src/{map.md,create_table/map.md}`,
`crates/repark-sql/tests/map.md`, `task/map.md`, this ledger.

---

## 3. Critic (C4, sequential)

- **Scope:** refuse is column-def only. CTAS arm unchanged. Spark files
  untouched. ALTER's `sql_type_to_iceberg` untouched.
- **Needles:** column in backticks (Iceberg said `for ts:` without
  backticks — that is how the red pin distinguished the two messages).
- **Positive control** was already green before the fix — the refuse does
  not fire on `TimeUnit::Microsecond`.
- **Silent remap risk:** none. We do not rewrite ns → µs.
- **tests.rs ceiling:** e2e lives in `session_wiring.rs` because
  `src/tests.rs` is at 1556/1600.
- **FIFO fences:** no functions*, ta*, merge commit region, occ_tests,
  position_delete.

---

## 4. Gates

| Gate | Exit | Evidence |
|---|---|---|
| red-then-green | RED then GREEN | Unfixed: `ansi_column_def_nanosecond_timestamp_shapes_refuse` FAILED on Iceberg `Invalid type for ts: timestamp_ns is not supported until v3`. Fixed: that pin + `TIMESTAMP(6)` positive + 3 helper tests green. |
| `cargo test -p repark-sql` | 0 | full crate suite, including session_wiring 4/4 |
| `make verify` | 0 | fmt, clippy -D warnings, rust-test workspace, maps |
| `make preflight` | 0 | verify + facade `3119 passed, 71 skipped` + audit + zizmor |

Clippy remediations in this unit: `single_match_else` (column-def arm is `if let`), `redundant_closure_for_method_calls` (`RecordBatch::num_rows`), `doc_markdown` (`` `timestamptz_ns` ``).
