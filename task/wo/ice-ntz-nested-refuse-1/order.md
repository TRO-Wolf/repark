# ICE-NTZ-NESTED-REFUSE-1 (U-8, R-011, ruling T-3): every door refuses a mixed-wall nested microsecond NTZ store with one hint     grade: A   engine: Claude Opus 5.5 (`claude-opus-5-5`)   release: 1.5.5

**Status:** order of record, dispatched 2026-10-20 as step 2 of the one Opus 5.5 lane of the
2026-10-20 Iceberg re-prioritisation (order table row U-8), adopted by the owner the same day.
Branch `feat/ice-ntz-nested-refuse-1` from main `c516c54a`. A scoped Opus verifier follows on a
fresh agent. The step-0 answer is [sketch.md](sketch.md). The text below is the dispatch brief,
with the local clone and hand-back locations removed.

## Standing repository rules (verbatim, in force)

1. Anthropic models write NO comments in code (Rust, Python, TOML, YAML, Makefile, shell). Never
   edit an existing comment line. Doc comments (`///`, docstrings) are not comments. Explanatory
   prose goes in the directory's `map.md` with a `pins: <unit>/C-NNN` citation.
2. Commit trailer exactly `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`; never
   a co-author line, a session line or a chat link, in commits or the PR body.
3. No absolute home paths and no session scratch paths in committed lines.
4. `map.md` lockstep per commit for every directory whose files change.
5. No retries, raised bounds, skips, or loosened pins. A pin that this unit makes red is
   rewritten to the new ruled answer with a dated note, never deleted or skipped.
6. Rust first: the decision lives in Rust; the Python facade only reaches it.
7. Ruling 76(a), 2026-10-15: **a hint never names an action that deletes data.** No hint may
   suggest dropping, truncating, overwriting or recreating anything.
8. Pinned formatters `uvx ruff@0.15.22`, `uvx taplo@0.9.3`, `uvx typos@1.47.2`; `cargo fmt`.
9. The verification gate is `make verify` (Rust) plus the facade suite and
   `make rust-panic-ban`; `make ci` holds the comment-density ceiling.

## The ruling you build (T-3, adopted 2026-10-14; plan row U-8, 2026-10-20)

Parity row ICE-TSNS-SQL-1-R-011 (`docs/spark-sql-iceberg-parity.md`, "a nested microsecond
`TIMESTAMP_NTZ` field stores two walls"): in a session whose zone is not UTC, an instant written
into a nested microsecond `TIMESTAMP_NTZ` leaf stores the **session** wall by field assignment
(28 cells, the value Spark stores on every door) and the **UTC** wall by every other door (106
cells, wrong). A wall source and a UTC session agree everywhere (379 cells). Two fold attempts
(#1037, #1038) to store the right wall are parked; the lift is a 1.5.7 unit. T-3: **R-011 refuses
with a hint now, as R-015 does.** The plan row: every door refuses a mixed-wall nested NTZ store
with one hint; the hint names no deleting action; #1037's Spark cells are re-pinned as refusals.
This is the last silent-value row on the v2 label list.

## Read first, in this order

- `AGENTS.md` once, then `.agents/skills/engineering-method/SKILL.md`.
- The R-011 row and the R-015 row in `docs/spark-sql-iceberg-parity.md` (search the headings).
  R-015 is the model: its refusal text, its "decided on the table before the write is routed"
  rule, its list of what stays allowed, and its treatment of `EXPLAIN ANALYZE`, `PREPARE` and
  CTAS-of-INSERT.
- The R-015 seam: `crates/repark-iceberg/src/write/nested_ns_gate.rs` (`nested_ns_leaf`,
  `nested_ns_refusal`, `refuse_nested_ns_supply`, the `NestedWrite` supply model) and its single
  caller `crates/repark-spark/src/router/nested_ns.rs` (`refuse_at`), plus the `map.md` prose for
  both ("Why refuse and not convert", "Why a timestamptz_ns or microsecond leaf is not matched").
- `task/roadmap/mid-term/ice-ntz-nested-wall-1-card-2026-10-10.md` and the ledger clause C-017 and
  question Q4 in `task/ledgers/staging/ice-tsns-merge-wall-1-ledger.md` (§9.2 has the measured cells).
- `task/wo/ice-ntz-nested-wall-1/fable-q3/answer.md`: the parked design. Its §2 lists every door
  that ends at a nested NTZ store today (the UPDATE and MERGE folds, the MERGE star, the
  predicate-DML UPDATE projection, INSERT OVERWRITE and BY NAME, the INSERT hook, the DataFrame
  writers). That list is your door census.
- The current pin that holds the split: `crates/repark-spark/tests/timestamp_ns_wall_doors.rs`
  `a_nested_microsecond_ntz_field_keeps_the_split_main_has`. It goes red under this unit by
  design and is rewritten.
- PR #1037's measured Spark cells: `git fetch origin fix/ice-ntz-nested-wall-1` and read
  `task/wo/ice-ntz-nested-wall-1/evidence/` on that branch (`spark-4.1.2.jsonl`, `summary.md`,
  `python/repark/tests/ice_ntz_nested_wall_1_helpers.py`). Reuse the cell shapes; do not merge
  or cherry-pick any product code from #1037 or #1038.
- The existing refusal grammar and SQLSTATE: how R-015 and the struct-pairing refusal
  (`crates/repark-spark/src/insert_by_name/nested.rs`) phrase theirs, so this one reads the same.

## Step 0: the sketch, before any product code

Write `task/wo/ice-ntz-nested-refuse-1/sketch.md` (and `order.md` beside it, plus the directory's
`map.md` and the `task/wo/map.md` row) stating the refusal rule, the door census with the seam
each door reaches, the cell accounting, the hint text, and the pins. The lean, which you confirm
or correct by reading the code:

- **Rule.** A write is refused, before any file is written, when ALL of: the session zone is not
  UTC; the target column's type holds a microsecond naive timestamp leaf below the top level
  (struct field, array element, map key or value, any depth; the `nested_ns_leaf` walk with the
  unit changed); the statement supplies that column (the same `Supply` reading as R-015: a
  listed column, a by-name source carrying it, an assignment to it or into it, a MERGE clause
  that sets or inserts it); and the value reaching that leaf is zoned or its type cannot be
  read at the seam. A naive (wall) source whose type the seam can read is NOT refused: those
  are the 379 agreeing cells, and refusing them would regress EQUAL cells. If reading the source
  type at a door needs a new seam, that door refuses every supply (fail closed) and the sketch
  says which doors and how many cells that is. A UTC session refuses nothing.
- **Decision point.** On the table before the write is routed, in the router, as R-015. The
  `timestamptz_ns` leaf stays a control (R-008); the nanosecond naive leaf keeps R-015's text.
- **Text.** One refusal, R-015's class and SQLSTATE
  (`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … SQLSTATE: KD000`), naming the table,
  column and leaf path, and one hint. The hint tells the user how to write the row today: set
  `spark.sql.session.timeZone` to `UTC` for the statement, or write the leaf from a
  `TIMESTAMP_NTZ` wall (a literal or a `CAST(… AS TIMESTAMP_NTZ)` at the top level). The hint
  names no deleting or overwriting action (ruling 76(a)).
- **Cells.** Count, per door and zone, the cells that move from "stores a value" to "refuses":
  the 106 wrong cells must all refuse; the 28 field-assignment cells refuse too (one rule, one
  text; say so); the 379 wall or UTC cells keep their value. State the counts in the sketch
  from your own measurement, not from the row.
- **Doors in scope.** Every SQL door in the fable-q3 census plus the DataFrame writers
  (`append`, `overwrite`, `writeTo`), `EXPLAIN ANALYZE` of a DML, `PREPARE … AS` a write run by
  `EXECUTE`, CTAS of an INSERT, with and without a branch reference, exactly as R-015 lists them.
  `CALL system.add_files`, CTAS from a SELECT, compaction, reads, DELETE and schema evolution are
  not refused.

Put the sketch in your first commit. If, while reading, you find the lean cannot hold (for
example the session zone is not readable where the router decides, or a door cannot be reached
before it writes), stop at the sketch, write the hand-back with the measured reason, and do not
build. The orchestrator takes it to the owner.

## Build

- Generalise the R-015 gate so one walk and one refusal serve both leaf units, parameterised by
  unit and by the zone condition; no second copy of the supply model.
- Rewrite `a_nested_microsecond_ntz_field_keeps_the_split_main_has` into refusal pins: every door
  in three zones refuses with the one text, and in UTC stores the wall it stored before (the
  same values the test holds today). Add a facade pin on the DataFrame writers and a live Spark
  cell set re-pinning #1037's measured shapes as refusals (Spark stores the session wall; RePark
  refuses; the registry row says why they differ). Each pin cites `ice-ntz-nested-refuse-1/C-NNN`
  in the test docstring's `pins:` line and in the `map.md` row.
- The new ledger `task/ledgers/staging/ice-ntz-nested-refuse-1-ledger.md` follows the grammar
  of `task/ledgers/staging/ice-tsns-merge-wall-1-ledger.md` (the ledger-grammar gate reads it).
- The parity row R-011: a dated status 2026-10-20 "REFUSED (U-8, ICE-NTZ-NESTED-REFUSE-1)"
  with the rule, the text, the cell counts and the lift unit named, keeping the measured
  history above it. Update the card's status and the v1.5.5 release notes under
  `task/roadmap/mid-term/` with one line each. Do not touch `task/ledgers/staging/ice-tsns-merge-wall-1-ledger.md`
  beyond a dated pointer on C-017 if its grammar allows an addendum; else leave it.

## Gates, commits, PR, hand-back

`cargo fmt`, `make verify`, `make rust-panic-ban`, the facade suite
(`PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv taskset -c 0-25 uv run --no-project python -m pytest python/repark/tests -q -n 16 --dist loadgroup --maxschedchunk=8 -p no:cacheprovider`
after `uv sync --locked --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark`
and `maturin develop --profile ci` in `python/repark`), `make ci`, `python3 scripts/sync_map_md.py`.
The live Spark cells run with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 REPARK_PARITY_LIVE=1`
after `uv sync --locked --extra record …` (the `parity-live` recipe in the Makefile), on the
new module alone and co-collected after `python/repark/tests/parity`.

Commits: the sketch first, then the build in as few commits as the story needs, each with the
trailer. Subject form `feat(ice-ntz-nested-refuse-1): …`. Push and open the PR against `main`
with `gh pr create`: title = the final subject; body = the rule, the hint text, the cell counts
before and after per door, the gates with their numbers, "Verifier: pending", "Owner merges". No
attribution footer. Do NOT merge.

Write the hand-back outside the clone: the rule as built, every door and its seam, the cell counts, every
gate command with its wall time and load, the PR number, what a hostile verifier should attack
first, and anything you left undone or read differently from this brief.
