# ATTR-ID-1 S3a: move the select family to `resolve` (fresh Muse session, guided; lane /tmp/xattr, branch feat/attr-id-1, head eeffdc43)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1, §3.4–§3.7, §4 "S3", §5, and **§9, §9a, §9b and §9c** (all S1, S2, S2b and S1b outcomes and rulings);
- the ledger `task/ledgers/staging/attr-id-1-ledger.md`;
- `crates/repark-core/src/session/df_guards/attr_id.rs` (`resolve`), the pyfunction `resolve_display_name` in `crates/repark-python/src/dataframe_names.rs`, and `Column._attr_id` with the bind sites from S2;
- on main's facade: `core.py` `select` (about 1244), `__getattr__` (about 1658), `_rebind_stable_name_column` (about 2032), `__getitem__` (about 2083) and `_column_of` (about 3462); and its callers `udf_projection.py:88`/`:250` and `joins_columns.py:247`.

## Task: the S3a family only
Move `select`, `__getitem__`, `__getattr__`, `_column_of` and `_rebind_stable_name_column` onto the single rule of §1:
- **hits** are the positions whose display name matches under the session's **live** `spark.sql.caseSensitive` (§3.7), and whose qualifier matches when one is written;
- **distinct** is the set of ids over those hits;
- 0 hits → UNRESOLVED_COLUMN; exactly 1 distinct id → bound; more than 1 → AMBIGUOUS_REFERENCE, with Spark's message shape.

Parent Columns carrying `_attr_id` bind by id against the child frame (§3.6): no matching position gives the existing missing-attribute refusal.

- **Delete** the helpers that only this family used, in the same commit. List every helper you delete, and show with a grep that it has no other caller.
- Helpers still used by the S3b–S3e families stay until their round.
- **§9, Q1: a user's `alias()` mints a fresh id** through the alias's own metadata, so `df.select(df.v.alias('v'), df.v)` holds two attributes, as in Spark. Pin it.
- Never add an "unknown identity" branch. A missing id on a frame the facade holds is a bug: raise an internal error in tests, and never treat it as a wildcard or as distinct (halt rule 3).

## First: timing (the §9c Q2 ruling)
- Before any change, re-run the S0 replay 3 times on eeffdc43 when the box is quieter. Record the load average with each run.
- Command: `make develop` in the slice, then `/tmp/xattr/.venv/bin/python /tmp/oc-worker/direct/wo/attr-id-1/replay/replay.py <engine name S0 used for main> /tmp/oc-worker/direct/wo/attr-id-1/s3a/pre-N/`, then `compare.py` against `main.json`.
- If the median is still above 597 s, first apply the executor's cut as its own commit: `DataFrame.__init__` returns the same handle when the frame is already stamped. Then re-measure.

## Gate for this round (the replay, cell by cell against `main.json`)
- **0 cells move away from Spark** against `main.json`, meaning cells EQUAL on main must stay EQUAL.
- Every changed cell in the select family either becomes EQUAL to Spark (a gain; list them) or is a named residue with a reason.
- The four §9c residue cells may change further, but may not become worse. Moving from an error to wrong rows would be worse.
- Timing: median of 3 at or under 597 s.
- The neighbour sweep that S2b ran: 44 files, including `test_casesens_1*.py` and every `test_*join*`, `test_*csv*`, `test_*json*`, `test_*write*`, `test_*iceberg*` and `test_*cache*`, with `-n 8`. All must pass, or fail identically on main; measure main with `/tmp/oc-worker/direct/wo/attr-id-1` main runs or a main worktree.
- `bash /tmp/xattr/gate.sh` prints `GATE GREEN`. It includes `make rust-panic-ban`.

## Proof
- Pins, under both case rules:
  - `select` and `__getitem__` of a twin pair of one attribute (bound);
  - two attributes with the same display name (AMBIGUOUS_REFERENCE);
  - a parent Column after a filter or drop;
  - `alias()` minting a fresh id;
  - a qualified select on a join (`L.v`), and a qualified miss;
  - a session case rule changed after the frame was created (§3.7).
- Mutation: make `resolve` group by display name only, ignoring ids. The twin and ambiguity pins go red. Revert, and show `git status` clean.

## Halt when
- A cell EQUAL on main moves away from Spark.
- The round needs an unknown-identity branch.
- The timing stays above 597 s after the cut.
- A deleted helper turns out to have a caller outside this family.

## Commit (commit first, and keep `handback.json` provisional)
- Commit: `feat(attr-id-1): select, getitem and getattr bind by attribute id through resolve; the family's origin helpers are deleted (S3a)`, plus the optional timing-cut commit before it.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source file, docstrings on new functions and `noqa` prose included. Update `map.md` in lockstep. Extend the ledger with S3a clauses.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling.
- Hand-back: `/tmp/xattr/handback.json`, with the replay counts, the gains, the residues, the timings with load, and the product-line and test-line counts.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3a/`. Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
