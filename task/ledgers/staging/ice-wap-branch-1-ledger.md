# Unit ledger — ICE-WAP-BRANCH-1 · `spark.wap.branch` redirects writes and reads (round 1)

**Date:** 2026-09-19 · **Branch:** `fix/ice-wap-branch-1` · **Base:** `4049164d` (`origin/main`)
**Model:** Claude Opus 5 (high) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18 is 1:1 parity with Spark's Iceberg integration. On main
the session conf `spark.wap.branch` was IGNORED: with `write.wap.enabled=true` on the table, a
write meant for an audit branch published straight into the table and the session's reads stayed
on `main`. That is a silent wrong answer on the write path — the failure mode registry row REF-3
exists to keep impossible — and it was reachable from a one-line conf set.

**What it is.** One resolver in Rust decides the effective write branch and the effective read
reference per statement: an explicit `t.branch_b` selector outranks the session conf, the session
conf applies only when the target table carries `write.wap.enabled=true`, and `main` is the
default. Python forwards the conf string and decides nothing.

**Not in this unit:** the `spark.wap.id` staged-snapshot flow and `publish_changes` (fork ask
F-STAGE-ONLY-1 — `SnapshotProducer::with_stage_only` is the fork's, and RePark never patches
Iceberg table-format semantics locally); `spark.conf.isModifiable` (registry CONF-WAP-1's
remaining half); `STATUS.md`; `Cargo.toml` / `Cargo.lock`; size ceilings.

**Writable paths:** `crates/repark-spark/src/` (the resolver, the carrier, the tests),
`crates/repark-python/src/session_runtime.rs` (the conf seam),
`python/repark/src/repark/spark/session/` (conf forwarding), `python/repark/tests/`,
`docs/spark-sql-iceberg-parity.md`, this ledger, touched `map.md` files.

## Measured

Oracle: live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0, 19 `QW-*` cells, recorded
2026-09-19 by `python/repark/tests/_record_ice_wap_branch_1_oracle.py` into
`python/repark/tests/ice_wap_branch_1_spark_oracle.json` (SHA-256
`0fb43d0a55f33106d4987dbfd38f7e6f95aadc95eabf3d40d9372f9d8cd8d4f4`). The recording reproduces the
orchestrator's run-25c measurement (`spark-qc5.json`, cells `QW-*`) **observation for
observation**: 19 cells, zero differences over status, error type, error text and every `obs` key
(cross-check script run 2026-09-19).

Replay of the same 19 cells through the harness on RePark:

| Head | Cells equal to Spark |
|---|---|
| `4049164d` (main, before) | 5 / 19 |
| this branch (after) | 19 / 19 |

The 5 that already matched are the negative controls: no `write.wap.enabled`, read-only, an
explicit `t.branch_other` write, an explicit `VERSION AS OF 'main'` read, and a CTAS into a new
table.

## The resolver's decision table

Per statement, for one Iceberg target or relation:

| Statement carries | Table property `write.wap.enabled` | `spark.wap.id` also set | Effective write ref | Effective read ref |
|---|---|---|---|---|
| `t.branch_b` | any | any | `b` (refuses when `b` does not exist) | `b` |
| `t.tag_v` | any | any | refuses (`Cannot write to table with time travel`) | `v` |
| `VERSION AS OF 'x'` | any | any | n/a (read) | `x` |
| nothing, `spark.wap.branch=a` | `true` | no | `a` (created from `main` when absent) | `a`, falling back to `main` when the ref does not exist |
| nothing, `spark.wap.branch=a` | `true` | yes | refuses with Java's text before any write | — |
| nothing, `spark.wap.branch=a` | absent / not `true` | any | `main` | `main` |
| nothing, no `spark.wap.branch` | any | any | `main` | `main` |

Not a relation for either half: a metadata-table path (`t.snapshots`, including the `table$suffix`
word the metadata rewrite emits), the statement's own write target on the read scan, a table
outside the catalog registry, and a CTAS into a new table (no target to load).

## PROPOSITION LEDGER — ICE-WAP-BRANCH-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The committed oracle is the measurement: 19 recorded Spark cells, reproduced by a generator, and RePark answers every observation of every cell (`refs`, branch rows, main rows, the session read while the conf is set, the plain read after it is unset). | One parametrized pin per cell in `test_ice_wap_branch_1.py`. | PROVEN | 23 passed + 1 skipped on the release native; red-first 17 failed / 6 passed on `9256a491`. pins: ice-wap-branch-1/C-001 |
| C-002 | With `write.wap.enabled=true` and `spark.wap.branch` set, every write shape commits on the branch and `main` is unchanged: SQL INSERT, `writeTo().append()`, `saveAsTable(append)`, CoW DELETE, MoR DELETE, UPDATE, MERGE, INSERT OVERWRITE, two stacked appends. | The nine write cells + `wap_branch_redirects_the_write_and_the_session_read`. | PROVEN | Every cell's `main` observation is the seed pair and its `branch` observation carries the write. pins: ice-wap-branch-1/C-002 |
| C-003 | The session's plain reads of that table follow the branch while the conf is set and answer `main` again once it is unset, with the table's Arrow types unchanged. | The `session-read` / `plain-read-after-unset` observations on all 18 ok cells + `test_wap_branch_read_keeps_the_arrow_types`. | PROVEN | The Arrow rider compares the branch read's full schema to main's. pins: ice-wap-branch-1/C-003 |
| C-004 | Without `write.wap.enabled=true` the conf is ignored entirely — the write lands on `main` and reads stay on `main`. It is a table property, not a session conf. | `QW-INSERT-NOT-ENABLED`; `wap_branch_without_the_table_property_writes_main`; `test_wap_branch_leaves_a_table_without_the_property_on_main`. | PROVEN | Green on all three doors. pins: ice-wap-branch-1/C-004 |
| C-005 | Precedence: an explicit `t.branch_other` write target and an explicit `VERSION AS OF 'main'` read both outrank the conf; a wap branch that does not exist is created by the write from `main`; metadata tables and a CTAS into a new table are unaffected. | `QW-EXPLICIT-BRANCH`, `QW-VERSION-AS-OF-MAIN`, `QW-INSERT-NO-BRANCH`, `QW-METADATA-TABLE`, `QW-CTAS-OTHER`; the five matching Rust pins. | PROVEN | The metadata pin caught a real defect: the metadata rewrite emits one `table$suffix` Word that re-tokenizes as a base name plus a `$`-placeholder, so the read pass had to skip a name glued to a placeholder. pins: ice-wap-branch-1/C-005 |
| C-006 | `spark.wap.branch` with `spark.wap.id` refuses `IllegalArgumentException: Cannot set both WAP ID and branch, but got ID [w1] and branch [audit]` at the write, before anything is written. | `test_wap_branch_cell_refuses_like_spark[QW-WITH-WAP-ID]`; `wap_branch_with_wap_id_refuses_with_javas_text`. | PROVEN | Class and message asserted byte-exact against the fixture; the Rust pin also asserts the refused statement wrote nothing. pins: ice-wap-branch-1/C-006 |
| C-007 | The conf reaches the Rust carrier through both spellings — `spark.conf.set` and SQL `SET spark.wap.branch = audit` (which answers Spark's `(key, value)` row) — and `unset` / `RESET` clears it. Python forwards the string; the parse, the validation and the resolution are Rust's. | `test_sql_set_wap_branch_answers_the_pair_row`, `QW-SET-SQL`, `test_wap_session_conf_stores_through_the_sql_set_door`, `test_sql_set_wap_branch_answers_the_pair_row` (registry 16b), `the_carrier_round_trips_through_the_config_map`. | PROVEN | The facade's `unset` forwards the empty string and the Rust carrier owns what that means. pins: ice-wap-branch-1/C-007 |
| C-008 | The ANSI door carries no `spark.wap.*` conf, so no native-door write can be silently redirected: `repark.sql("SET spark.wap.branch = …")` refuses naming the rejected namespace. | `test_native_door_refuses_the_spark_wap_conf`. | PROVEN | The two-doors row. The native session does not share the facade's catalog registry, so the stronger "the ANSI write lands on main" shape is not reachable from a test; the refusal is the checkable half. pins: ice-wap-branch-1/C-008 |
| C-009 | The generator re-derives the committed fixture on live Spark (`check` mode exits non-zero naming the first mismatch). | `test_live_oracle_fixture_reproduces` under `REPARK_PARITY_LIVE=1`. | PROVEN | Ran green on the live leg 2026-09-19 (see Gates). pins: ice-wap-branch-1/C-009 |
| C-010 | `spark.wap.id` on its own is unchanged by this unit: the write still lands on `main` where Spark stages it, and this residue is written down in REF-3 as fork ask F-STAGE-ONLY-1. | `test_wap_id_alone_still_lands_on_main`; the REF-3 row. | PROVEN | Stated plainly rather than half-built: the brief's boundary is "leave `spark.wap.id` as it is". The residue is now reachable through SQL `SET` as well as `conf.set`, because both spellings route to one carrier — named in REF-3 and in the hand-back. pins: ice-wap-branch-1/C-010 |

## Measured observations (not clauses)

- The `before` replay also failed two cells for reasons downstream of the ignored conf:
  `SET spark.wap.branch` raised `Could not find config namespace "spark"` (the engine's SET door),
  and `fast_forward('main','audit')` refused `main is not an ancestor of audit` because the write
  had moved `main` instead of the branch. Both are green after the fix with no separate change.
- `crates/repark-spark/src/lib.rs` sits exactly on its 150-line ceiling. `pub mod wap;` was paid
  for net-zero by folding the two `catalog_ops` re-exports into one `pub use` (sanctioned out (1)
  in `scripts/check_lib_rs.py`); no ceiling moved.
- An explicit write to a branch that does not exist still refuses Java's
  `Cannot use branch (does not exist)`. Only the wap path skips that pre-check, because the
  commit creates the ref — the fork's `latest_snapshot` falls back to `main` exactly as Java's
  `SnapshotUtil.latestSnapshot` does.

## Honest residues

- **`spark.wap.id` alone** (C-010): write lands on `main`, Spark stages. Fork ask
  F-STAGE-ONLY-1; recorded in REF-3.
- **A wap-branch DELETE/UPDATE/MERGE onto a branch that does not exist yet** is unmeasured.
  The plain-INSERT path creates the ref; the row-level paths scan through the fork's
  `resolve_scan_snapshot_id`, which refuses a missing ref loudly (`snapshot ref 'a' not found`)
  rather than falling back to `main`. Loud, never a wrong answer; no oracle cell covers it.
- **`spark.conf.isModifiable`** still answers `True` (CONF-WAP-1, unchanged by this unit).

## Gates

On `a5cb3d94`, `CARGO_BUILD_JOBS=6 RUST_TEST_THREADS=6`:

- `cargo test -p repark-spark --lib` — 1252 passed, 0 failed, 5 ignored.
- `cargo test -p repark-spark --lib tests::wap_branch` — 9 passed.
- `.venv/bin/python -m pytest python/repark/tests/test_ice_wap_branch_1.py -q -n 4` —
  23 passed, 1 skipped.
- `python3 /tmp/oc-worker/_lib/comment_ban.py <clone> origin/main` — `comment-ban hits=0`.
- `cargo fmt --all --check` — clean. `.venv/bin/ruff check` / `format --check` — clean.
- `scripts/check_lib_rs.py`, `check_rust_file_size.py`, `check_lib_py.py`,
  `check_crate_dag.py`, `sync_map_md.py --check`, `check_docs_links.py` — clean.

## Attestation

```
COVERAGE_ATTESTATION:
  pr_unit: ice-wap-branch-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The Spark-visible change is pinned cell for cell against a recorded live oracle that reproduces the orchestrator's independent measurement observation for observation; 5 of 19 cells matched before, 19 of 19 after.
      artifacts: [python/repark/tests/test_ice_wap_branch_1.py, python/repark/tests/ice_wap_branch_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first on the unfixed tree — 17 failed, 6 passed, 1 skipped. Every pin names an input where the branch changes the output; the metadata-table pin redded on a real defect (the table$suffix re-tokenization) and is green only because the fix landed.
      artifacts: [crates/repark-spark/src/tests/wap_branch.rs, python/repark/tests/test_ice_wap_branch_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every write door Spark offers is a cell (SQL INSERT, writeTo append, saveAsTable, CoW and MoR DELETE, UPDATE, MERGE, INSERT OVERWRITE, stacked appends), plus the five negative controls, the both-keys refusal, the SQL SET spelling and the native ANSI door.
      artifacts: [python/repark/tests/test_ice_wap_branch_1.py, python/repark/tests/test_ref_branch_tag_wap.py]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state and no concurrency. The carrier is a per-session DataFusion ConfigExtension written under the session state lock, the same seam the ANSI, zone, case-sensitivity and overwrite-mode knobs already use.
    - id: AT-5
      status: ATTACKED
      evidence: The change is fail-safe in the direction that matters — a table without write.wap.enabled is untouched, an explicit selector always outranks the conf, and the both-keys combination refuses before any file is written (pinned: the refused statement wrote nothing).
      artifacts: [crates/repark-spark/src/tests/wap_branch.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The residues are written down rather than smoothed over — spark.wap.id alone still lands on main (now reachable through SQL SET too), a row-level DML onto a not-yet-created wap branch refuses loudly, and isModifiable still diverges. All three are in the ledger and in the registry rows.
      artifacts: [task/ledgers/staging/ice-wap-branch-1-ledger.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: The before/after numbers are this unit's own harness replay of the same 19 cells on the same box — 5/19 on 4049164d, 19/19 on the fix.
      artifacts: [task/ledgers/staging/ice-wap-branch-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, no Cargo feature, no Cargo.toml edit; no size ceiling moved (lib.rs stayed at 150 by folding two re-exports into one). The fork is untouched — the staged-snapshot half is filed as an ask, not patched locally.
      artifacts: [crates/repark-spark/src/lib.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Five maps carry the change with pins citations — the Spark source map, the Spark tests map, the binding map, the facade session map and the Python tests map — and the two registry rows are corrected to describe only what is still true.
      artifacts: [crates/repark-spark/src/map.md, crates/repark-spark/src/tests/map.md, python/repark/tests/map.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The whole repark-spark lib suite (1252 tests), the new file offline and live, and every file mentioning wap under python/repark/tests ran green at the unit's head; CI runs the rest.
      artifacts: [crates/repark-spark/src/tests/wap_branch.rs, python/repark/tests/test_ice_wap_branch_1.py]
  complete: true
```
