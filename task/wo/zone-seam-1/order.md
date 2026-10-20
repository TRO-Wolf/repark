# ZONE-SEAM-1 — one session-zone conversion seam for every zoned-timestamp conversion     grade: B   engine band: Claude Opus 5.5 build, one hostile Opus 5.5 verifier   release: 1.5.6

**Status:** order, **adopted by the owner on 2026-10-20** ("Do it, proceed with recommendations
and implementation"), from the stability pathway the orchestrator recommended the same evening.
Dispatches after EXPLAIN-DML-WRITE-1 merges, on the one Opus 5.5 lane; its step 0 sketch
returns to the orchestrator before step 1. Written by Claude (claude-fable-5-1), orchestrator,
from the measured tables of card
[CONTAINER-CAST-ZONE-1](../../roadmap/mid-term/container-cast-zone-1-card-2026-10-20.md) and from
reading main `754fb6e5`; nothing was run.

## 0. Why

Every timestamp unit since v1.5.3 (R-007, R-015, R-011, ENC-1, the container cast) is one defect
seen from a different door. RePark converts a zoned timestamp on two paths. The leaf cast
`CAST(x AS TIMESTAMP_NTZ)` is lowered at the SQL AST (`keyword_lower/ntz_cast_lower.rs`) to the
UDF `__repark_cast_timestamp_ntz__` (`crates/repark-functions/src/timestamp_ntz_cast.rs`), which
reads the session zone and matches Spark. Everything else, a `CAST`/`TRY_CAST` whose target is a
struct, array or map holding a naive timestamp, string or date at a position where the source
holds a zoned timestamp, and `to_json` of a zoned leaf, goes through DataFusion's native
`CastExpr` and renders in UTC. The card's tables show the wrong wall in a plain `SELECT`, in every
write and in the six table-creating writers, on main. The write-side gates of R-015 and R-011
sit downstream of that split and cannot see a value that passed through another type first
(PR #1124's second verification, S1-1 and S2-1).

The seam fixes the cast. Then a plain `SELECT` is right, and every door inherits it because a
write stores what `SELECT` yields.

## 1. Rulings and standing guidance in force

- Owner 2026-10-20: the stability pathway adopted: fix the root, prove parity at the expression
  level, refuse only what Spark refuses, retire gates that only covered the defect, no panic
  reaches a user.
- Rust-first (2026-09-14): the seam is Rust; Python is the facade.
- Coercion rulings 2026-10-12: cost gate ≤1.05x like-for-like medians; out-of-range raises in
  both ANSI modes; a fallback is never silent.
- NS create-door ruling 2026-10-13 and R-015 are untouched: a `timestamp_ns` leaf keeps its
  refusal; this unit is microsecond and string/date rendering.
- Ruling 76(a): no hint or refusal names a data-deleting action.
- Ruling 70: a statement main answers that the head refuses, or answers differently, outside
  the card's table is a halt.
- One Opus agent at a time; the sketch comes to the orchestrator before step 1; a verifier FAIL
  parks and the owner hears first (rulings 55, 73).

## 2. Files (read on main `754fb6e5`; the sketch confirms or corrects each)

**The existing session-aware leaf path, to reuse, not duplicate:**

| What | Where |
|---|---|
| the NTZ cast UDF and its `try` twin, the NTZ literal | `crates/repark-functions/src/timestamp_ntz_cast.rs` (490 lines; `rewrite_ntz_target_cast` already rewrites a plan-level `Expr::Cast` to `Timestamp(µs, None)`) |
| the AST lowering of `CAST(x AS TIMESTAMP_NTZ)` and `TIMESTAMP_NTZ '…'` | `crates/repark-spark/src/keyword_lower/ntz_cast_lower.rs` (86 lines), called from `keyword_lower.rs` |
| the session-aware zoned→string rendering | `format_spark_timestamp_string` (`repark_common::spark_timestamp_text`), used by `crates/repark-functions/src/timestamp_cast.rs` |
| the map cast UDF | `crates/repark-functions/src/cast_map.rs` (`__repark_cast_map__`, 449 lines) |
| the session zone | `crates/repark-core/src/session_time_zone.rs` (383 lines; `spark.sql.session.timeZone`, default UTC) |
| `to_json` | `crates/repark-functions/src/json/to_json.rs` |
| the nested-NTZ store and update casts | `crates/repark-iceberg/src/write/ntz_store.rs`, `update_cast.rs` (name the same UDFs) |
| the write-side gates the seam will make partly redundant | `crates/repark-iceberg/src/write/nested_ns_gate.rs`, `crates/repark-spark/src/router/nested_ns.rs` (after #1124) |

**What the ZONE-ORACLE-1 corpus (PR #1128) adds to the scope, 2026-10-20 later:** the container
cast is format-wrong as well as zone-wrong (to `STRING` it renders ISO `T`/`Z` text, to `BIGINT`
microseconds where Spark gives seconds, in UTC too), so the seam routes every container leaf
through the leaf UDFs rather than correcting the zone of a native cast; `to_json` of an instant
leaf is already EQUAL and the `to_json` step is the `TIMESTAMP_NTZ` rendering (trailing `Z`); the
SQL leaf `TRY_CAST(x AS TIMESTAMP_NTZ)` is not session-aware; `to_utc_timestamp` and
`from_utc_timestamp` of a wall literal return `TIMESTAMP_NTZ` where Spark returns `TIMESTAMP`.
The sketch names each of these as a (source, target) pair or a function site, and the corpus
cells it closes.

**The seam (proposed; the sketch decides the exact placement):** one plan-level rewrite in
`repark-functions`, run where `rewrite_ntz_target_cast` runs today, that replaces any
`Expr::Cast`/`Expr::TryCast` whose target type holds, at any depth, a naive timestamp, a string
or a date at a position where the source type holds a zoned timestamp, with per-leaf
session-aware conversions: a struct becomes a `named_struct` of converted members, an array a
per-element conversion, a map the `__repark_cast_map__` path with converted values; every other
member keeps its native cast. The leaf conversions are the UDFs that exist. `to_json` renders a
zoned leaf through the same text function. No new conversion semantics are invented: every leaf
answers what the leaf cast answers today.

**Ceilings** (`scripts/check_rust_file_size.py`, default 1000): `timestamp_ntz_cast.rs` at 490
and `cast_map.rs` at 449 take the rewrite if it fits; a new file under `repark-functions` is
fine; no ceiling rises.

## 3. Steps

0. **Sketch** (`task/wo/zone-seam-1/sketch.md`, docs only, one commit): the rewrite's placement
   and signature; the exact list of (source leaf type, target leaf type) pairs it converts; how
   `TRY_CAST` fails per leaf; the `to_json` site; which R-011 and R-015 gate arms become
   unreachable once the seam lands (name them; do not remove them here); the cells of the card's
   tables each step closes; the expression corpus of ZONE-ORACLE-1 as the pin. To the
   orchestrator before step 1.
1. **Red first:** the ZONE-ORACLE-1 corpus (PR #1128; 400 DIFFERENT, 20 REFUSED, 70 ERROR of
   1,640 on main `754fb6e5`) replayed, the cells this unit will close listed in the ledger by id; Rust pins for each (source, target) pair red on main.
2. **The seam** (one or two commits): the rewrite and its unit tests in `repark-functions`.
3. **`to_json`** (one commit).
4. **Pins green**, on `repark.sql()`, the Spark SQL door and the DataFrame door: the card's plain
   `SELECT` table, the "through another type" table, the six table-creating writers, the cached
   frame; every control (leaf casts, UTC session, a wall source, `timestamp_ns` refusal of R-015)
   unchanged.
5. **Mutants and revert check:** each (source, target) arm removed alone reds a pin; the seam
   removed reds the corpus.
6. **Never worse:** the ZONE-ORACLE-1 corpus and the U-8 door matrix
   (`task/wo/ice-ntz-nested-refuse-1/evidence/probe.py`) on main and head; every changed cell is
   DIFFERENT→EQUAL or a listed refusal→EQUAL; any EQUAL→other is a halt. Cost: the U-8
   verifier's six statements, 10-run medians, ≤1.05x.
7. **Records:** the card, parity rows R-011 and R-015 (dated status lines naming the seam and
   the arms now unreachable), the v1.5.6 notes line, `map.md`s, the unit ledger under
   `task/ledgers/staging/`.

GATE-RETIRE-1 (removing the gate arms the sketch names as unreachable) is a separate small
round after the verifier's PASS, not this unit.

## 4. Gates

`make verify`, `make preflight`, `cargo test -p repark-functions`, `make rust-panic-ban`,
`make rust-clippy`, `python3 scripts/check_rust_file_size.py`, the ZONE-ORACLE-1 replay with
zero DIFFERENT on the timestamp corpus, the facade suite, the live leg alone and co-collected,
`make check-docs-links check-docs-compaction check-ledger-grammar`,
`python3 scripts/sync_map_md.py --check`, `python3 scripts/check_model_ban.py`,
`make check-comment-density`. CI green on the branch before the verifier. Then one fresh hostile
Opus 5.5 verifier: the corpus from its own builds of main and head, the card's tables, the six
writers, the mutants, cost, and an attack on container shapes the corpus does not hold.

## 5. Halts

A (source, target) pair whose Spark answer the corpus does not hold (record it, do not guess);
a leaf whose session-aware answer would differ from the existing leaf cast's; a change needed in
`repark-core` or the Iceberg write path beyond the records of step 7; a ceiling that would rise;
any EQUAL cell that moves; a cost over the gate.

## 6. Hand-back

The §7 JSON of [TEMPLATE.md](../TEMPLATE.md) plus: the corpus counts on main and head
(EQUAL / DIFFERENT / REFUSED per door and zone), the card's tables re-measured, the arms named
unreachable, the cost table.

Standing rules from [TEMPLATE.md](../TEMPLATE.md): no code comments from Anthropic models and
relocated code sheds its comments; `map.md` lockstep; the scratch clone, never the live worktree;
the TRO-Wolf identity and exactly one trailer, `Authored-By: Claude (claude-opus-5-5)
<noreply@anthropic.com>`.
