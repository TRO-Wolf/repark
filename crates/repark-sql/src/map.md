# map — repark-sql/src

## Purpose

Source for the ANSI SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. All NEW code — there is no port census here, so every behavior (including every
refusal) carries its own test in the same change.

The router's ORDER is the design's, and each position is load-bearing: text guards run first
(multi-statement before anything else), then metadata passthrough, then a stock parse, then the
statement match, then delegation with the SEC-02 plan guard between planning and execution. The
wrong-door sniff runs on the ERROR path only.

## Contents

- `lib.rs` — manifest: module list, `pub use dialect::AnsiDialect`, `pub use router::execute`.
- `router.rs` — the statement router (guards → metadata passthrough → parse → match →
  delegate) and the delegation path that carries the SEC-02 guard.
  Tests: [router/map.md](router/map.md).
- `dialect.rs` — `AnsiDialect: repark_core::SqlDialect` (the frozen seam adapter; a one-liner
  onto the router, deliberately). In-module tests.
- `guards.rs` — the guard set: multi-statement refuse (quote-aware, FIRST), P11 read-only
  catalog DML (generic message), write-to-branch, and the SEC-02 local-filesystem plan gate.
  The last two are RE-IMPLEMENTED from the Spark door's contract (not shared): both live behind
  private modules in `repark-spark`, and `repark-sql` must not take a door→door edge, nor the
  `repark-functions` edge the Spark gate uses to read its conf. Same conf key, same grandfather
  rule, same refusal class — read via `ConfigOptions::entries()`.
  Tests: [guards/map.md](guards/map.md).
- `sniff.rs` — the error-path wrong-door sniff (Q10/G3): on parse/plan FAILURE, name the token,
  the native equivalent, and the Spark door. Tests: [sniff/map.md](sniff/map.md).
- `scan.rs` — ANSI-quoting-aware SQL text scanning: the one place the door reads raw text.
  Blanks string-literal / quoted-identifier / comment CONTENT so the guards and the sniff cannot
  false-positive. Backticks are deliberately NOT treated as quoting (they are the Spark-ism the
  sniff reports). In-module tests.
- `create_table.rs` — CTAS + column-def `CREATE TABLE`: Q15 target routing (registered Iceberg
  catalog or LOUD refuse — never a silent `MemTable`), clause refusals, the three-way
  `LocationPolicy` resolution, staged create/replace, and the service-managed create-first path.
  Tests: [create_table/map.md](create_table/map.md).
- `properties.rs` — the curated `WITH (…)` vocabulary (Q1/G4/G9): `format`, `format_version`,
  `location`, `partitioning`, the `extra_properties = MAP(ARRAY[…], ARRAY[…])` raw-key hatch,
  and the reserved refusals (`sorted_by`, ORC/AVRO) that name their triggers.
  Tests: [properties/map.md](properties/map.md).
- `partitioning.rs` — partition-transform parsing (a small pure function, per Q2 — deliberately
  NOT shared with the Spark door's `PARTITIONED BY` validator) and Iceberg spec building with
  Java-parity field names. Tests: [partitioning/map.md](partitioning/map.md).
- `schema_ddl.rs` — `CREATE SCHEMA … WITH (location = …)`, `DROP SCHEMA`, `DROP TABLE`, plus the
  shared catalog-handle / name-parts / identifier-hygiene helpers.
  Tests: [schema_ddl/map.md](schema_ddl/map.md).
- `matrix.rs` (`#[cfg(test)]`) — this door's disposition of every `repark_common::surfaces` ID,
  with the compile-run audit that fails on an unmapped surface (Q13/G2).
- `tests.rs` (`#[cfg(test)]`) — the end-to-end door battery on a NATIVE session (no extension),
  asserted on the Arrow path, value AND type.

## I want to...

| ...do this | go to |
|---|---|
| Change routing order | `router.rs` (the order is the design's — read the module doc first) |
| Add a curated table property | `properties.rs` + a row in `properties/tests.rs` + an e2e row in `tests.rs` |
| Add a partition transform | `partitioning.rs` + `partitioning/tests.rs` |
| Add a guard | `guards.rs` + `guards/tests.rs` + a `surfaces` ID if it is a claimed surface |
| Record a surface this door will not have | `matrix.rs` (`DeliberatelyAbsent` with reason + ADR) |

## Pointers

- Up: [../map.md](../map.md). Design: `../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A guard fired on text inside a string literal | It cannot — the guards read `scan::blank_out_quoted_and_comments` output; check the scrubber's tests |
| A statement was delegated that should have been intercepted | `router.rs` match arms, and whether `references_metadata_table` diverted it |
| The matrix audit RED after adding a surface ID | Add a `Tested` or `DeliberatelyAbsent` row in `matrix.rs`; the failure names the ID |
| `m1_ships_the_briefed_scope` RED | A surface changed disposition — update the pin AND the ledger, in the same change |

First checks: `cargo test -p repark-sql --lib`. Escalate to: [../map.md#debug](../map.md).
