# Unit ledger — MB-3 · the Session-owned micro-batch driver (round 1)

**Date:** 2026-10-07 · **Branch:** `feat/mb-3-driver` · **Base:** `origin/main` `b162ca4a` ·
**Model:** claude-opus-5-5 (opus-worker build lane) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Scope.** The [MB-3 order](../../wo/microbatch/mb-3-driver.md) under the
[design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §2 Q2, Q3, Q7, Q9, §3.5, §3.6, §4,
§6 (the `mb-3` correction row) and §8, the
[North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) §2, §4, §5 and §8,
the [MB-2a ledger](mb-2a-ledger.md) D-10 and OQ-2a-2, the [MB-2c ledger](mb-2c-ledger.md) C-006,
and the orchestrator's rulings of 2026-10-07 carried in the brief (OQ-2a-2 acted on its lean; the
`toTable` door built on today's claim and epoch check with no racing-driver exactly-once claim;
the processing-time look-ahead cell measured first).

## Halt checks — 2026-10-07

- **Order halt rule 1** does not fire: the sketch's Q7 answer is present, and MB0-T2 and T3 agree
  with it.
- **Order halt rule 2** does not fire: MB-2c part 1 is merged (`8d456f38`); its closing slice
  (C-006) waits on `F-APPEND-PIN-BASE-1`, which the brief rules is not a halt for this round.
- **Order halt rule 3** does not fire: `crates/repark-core/src/session.rs` is not edited; the
  driver reaches the session through `Session::context()` (sketch §3.5).
- **Order halt rule 4** does not fire: no recorded MB-0 cell changes its answer. The 28 earlier
  entries and the preamble stay byte-identical beside the new MB0b-R18 (C-001).

## PROPOSITION LEDGER — MB-3 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | MB-1 fold-2 Q1, measured before the capped trigger: MB0b-R18 runs R17's shape (two one-file appends, then an `overwrite`, and the `delete` twin) under `trigger(processingTime="0 seconds")` with `streaming-max-files-per-micro-batch=1`. Spark delivers `a` only (batch 0, offset `(a, 0)`) and then fails `STREAM_FAILED` / `XXKST` with `Cannot process overwrite snapshot: <id>` (and the delete twin). Spark does not deliver-first, so the dated DECLARED row is added beside D-5. | The live cell on Spark 4.1.2 + Iceberg 1.11.0, the merged recording with every earlier entry byte-identical, the SHA-256 rewritten, the registry row and the D-5a row. | **PROVEN** | `MB0_CELLS=MB0b-R18` under `systemd-run --user --scope --slice=repark.slice`: `r18` and `r18_delete` each record `batches: [{batch 0, rows [[1, "k1"]]}]`, `offsets: [{batch 0, snapshot_ordinal 1, position 0}]` and `STREAM_FAILED`; the preamble and 28 earlier entries compare equal after parsing; `sha256sum -c` OK. Registry row `MB-3-LOOKAHEAD-1` (`docs/spark-sql-iceberg-parity.md`), MB-1 ledger D-5a. pins: mb-3/C-001 |
| C-002 | The `toTable` door is exactly-once against a racing driver on the same sink: a second driver's stamped append that loaded the sink before the first committed fails at its pinned base instead of re-basing. | `F-APPEND-PIN-BASE-1` merged and repinned, MB-2c C-006 closed (crash pins 2 and 3 green), then a driver-level race pin on the `toTable` door. | **OPEN** | Not claimed this round (orchestrator ruling, 2026-10-07). The door is built on today's claim and epoch check (MB-2c C-003), which refuse a stale re-delivery that reads the stamped head and a second claim in one scope, but the append still re-bases past a racer's commit. The crash gate keeps pins 2 and 3 ignored under their existing reason. The closing question is MB-2c C-002's: has the card's race pin merged at a pin RePark has taken? |
| C-003 | Slice 1, the skeleton and the Session seam (sketch §3.5). (a) `StreamingQueryManager::of` installs one manager per session as a DataFusion config extension through `Session::context()`, returns the same `Arc` on every call for that session and a different one for another session, and `session.rs` is not edited. (b) `register` resolves the sink through the session's catalogs, derives `QueryId::derive(sink uuid, queryName)`, mints a fresh `RunId` per handle, and leaves the query `Registered` and out of `active()` (CC-1 rule 1). (c) An unknown table, an unregistered catalog and a two-part name refuse at `register` naming the cause. (d) `RecordedLocation` never renders its text, alone or inside a `StreamSpec`. | The slice-1 pins in `microbatch/driver_tests.rs`. | **PROVEN** | 5 pins green: `the_manager_rides_the_session_config_once_per_session` (a), `registering_derives_the_query_id_from_the_sink_and_does_not_start` (b), `registering_refuses_an_unknown_or_malformed_sink` (c), `a_recorded_location_never_renders` (d), `query_states_split_active_from_terminal` (NS-16's six states; `isActive` is `Running` or `Draining`). `git diff origin/main -- crates/repark-core/src/session.rs` is empty. pins: mb-3/C-003 |

## Decisions

- **D-1 (2026-10-07). `register` is async and takes the session.** The sketch's
  `StreamingQueryManager::register(&self, spec)` is synchronous and `QueryHandle::id()` returns a
  `QueryId`, but the id is `QueryId::derive(sink uuid, queryName)` (sketch §3.1), and the uuid
  lives in the sink's metadata, which only an async catalog load reads. `register` therefore
  loads the sink, and takes `&Session` because the manager rides inside the session's config and
  must not hold the session (that would be a reference cycle through the config extension).
  Registering still does not start the query (CC-1 rule 1); MB-4's `toTable` / `start` call
  both, as Spark's do.
