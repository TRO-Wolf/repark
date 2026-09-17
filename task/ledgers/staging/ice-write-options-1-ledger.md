# Charter ledger — ICE-WRITE-OPTIONS-1 · DataFrame write options on Iceberg writes

**Date:** 2026-09-17 · **Branch:** `feat/ice-write-options-1` · **Base:** `origin/main`
`79e328f2` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Rating row V2-29 (2026-09-16): `writeTo(t).option("snapshot-property.run_id",
"abc-123").append()` commits but the snapshot summary carries no `run_id`;
`.option("write-format", "orc")` writes PARQUET; `target-file-size-bytes`,
`isolation-level` and unknown options are accepted with no effect, disclosed only by a
process-once `UserWarning`. Pipelines use `snapshot-property.*` as idempotency markers, so
the drop is a silent wrong answer.

**Not in this unit:** `overwrite(condition)` engine support (refusal stays; options never
reach a commit there); ORC/Avro Iceberg writers (DECLARED refusal, no writer built);
`STATUS.md` (never edited); fork pin moves (rule: pin moves only via its own PR).

## Rulings recorded at open

- Q-17a-2 (owner, 2026-09-16): Rust first. Option parsing, validation and every
  raise/cast/coerce/branch decision land in Rust. Python binds names only.
- Run 19c shape rule: remediate against Spark 4.1.2 + the Iceberg runtime in
  `python/repark/tests/_oracle_pins.py`, live-oracle pins over a RECORDED FIXTURE, or a
  dated DECLARED refusal with a registry row. Never a silent wrong answer.
- R-19c-1 (this lane, 2026-09-17): the options map crosses the PyO3 boundary rendered
  into the facade-generated SQL as an `OPTIONS(...)` clause (mechanical rendering, no
  key branching in Python). Rust pre-parse recognizer extracts, validates and honors or
  refuses. Raw-SQL users never see the clause; it is a facade-internal channel.
- R-19c-2 (this lane, 2026-09-17): plain `INSERT INTO` commits inside the fork's
  DataFusion provider, which mints only `engine.operation-id`. An option-carrying append
  therefore executes on the RePark-owned stage-then-commit path (source SELECT via the
  session, files via `repark-iceberg` writers, `commit_append` with the merged summary),
  mirroring `insert_overwrite.rs` stage-then-swap. Option-free appends keep the fork
  passthrough byte-identical.
- R-19c-3 (this lane, 2026-09-17): the pinned fork's `RollingFileWriterBuilder` exposes
  no per-statement target-size setter (`new_with_default_file_size` uses the 512MB
  constant; even the table property is not consulted). Per-statement
  `target-file-size-bytes` is a fork ask (F-TARGET-FILE-SIZE-1,
  TRO-Wolf/iceberg-rust#288), not a local patch. Affected pins refuse typed.

## PROPOSITION LEDGER — ICE-WRITE-OPTIONS-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `snapshot-property.<k>` lands in the snapshot summary of the commit on every DataFrame write path that commits: `writeTo(t).append()`, `.overwritePartitions()`, `.create()/.replace()/.createOrReplace()`, `df.write.format("iceberg").option(...)` + `saveAsTable`/`insertInto`/`save` where supported; exactly the key/value Spark writes (prefix stripped iff Spark strips it). | `test_ice_write_options_1.py` SNAP cells vs `ice_write_options_1_spark_oracle.json`. | OPEN | Oracle cells recorded 2026-09-17 (see §2). |
| C-002 | `write-format=parquet` honoured; `orc`/`avro` refuse typed naming the registry row; never a silent parquet file. | Same pin file, FORMAT cells. | OPEN | RePark has no ORC/Avro Iceberg writer: dated DECLARED 2026-09-17. |
| C-003 | Each of `target-file-size-bytes`, `compression-codec`, `compression-level`, `distribution-mode`, `fanout-enabled`, `isolation-level`, `check-nullability`, `check-ordering`: Spark's behavior measured, then honoured in Rust or refused typed with a row. Nothing accepted-but-unhonoured stays silent. | Same pin file, OPT cells; registry rows for refusals. | OPEN | `target-file-size-bytes` needs F-TARGET-FILE-SIZE-1 (see R-19c-3). |
| C-004 | Unknown option keys answer exactly as Spark does (measured: Spark ignores unknown DataSource options). | Same pin file, UNKNOWN cells. | OPEN | To be measured in §2. |
| C-005 | The process-once `UserWarning` goes away when C-001..C-004 hold; kept only for anything still not honoured, naming the keys. | Warning-text grep + pin file. | OPEN | Old text at `spark/dataframe/core.py:191-203`. |
| C-006 | SQL door: measured whether Spark 4.1.2 + Iceberg 1.11 exposes snapshot properties on SQL (session conf, `SET`, table options on INSERT). Same answer on RePark's SQL door if it does; measurement in ledger if not. | SQL cells in fixture + ledger §2. | OPEN | To be measured in §2. |
| C-007 | Registry rows in `docs/spark-sql-iceberg-parity.md`: `ICE-WRITE-OPTIONS-1` dispositions, the ORC/Avro DECLARED row, any fork ask. | Registry diff. | OPEN | Written in §5. |

## 1. Red-first record (base `79e328f2`, release native, 2026-09-17)

TODO: paste base-tree red output of `test_ice_write_options_1.py`.

## 2. Spark oracle recording (PySpark 4.1.2, Iceberg 1.11.0, 2026-09-17)

TODO: generator run output, fixture path, per-cell observations.

## 3. Implementation (Rust)

TODO: files touched, design notes, per-piece commits.

## 4. Gates

TODO: paste gate outputs.

## 5. Registry

TODO: rows written.

## 6. Handoff

TODO: disk checks, cleanup, kept artifacts.
