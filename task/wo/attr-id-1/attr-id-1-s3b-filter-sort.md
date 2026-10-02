# ATTR-ID-1 S3b: move filter(str) and orderBy/sort to `resolve` (fresh Muse session, guided; lane /tmp/xattr, branch feat/attr-id-1, head 846b447e)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1, §3.4–§3.7, §4 "S3", §5 and §9–§9d;
- the S3a brief `/tmp/oc-worker/direct/wo/attr-id-1-s3a-select.md` and S3a's hand-back `/tmp/oc-worker/direct/wo/attr-id-1/s3a-final-handback.json`. S3a's resolve rule, its live case rule, its parent-id rebind, its alias fresh id, `chain_id` and the SubqueryAlias overlay are the base you build on;
- the ledger `task/ledgers/staging/attr-id-1-ledger.md`;
- main's facade: `core.py` `filter` (about 1195; its SQL-string predicate path and the case-collision refusal in its docstring) and `order_by` (about 2570; `orderBy`/`sort` are aliases), plus every helper they call to bind names.

## Task: the S3b family only
Move **`filter(str)`/`where(str)`** (every column name a SQL-string predicate mentions) and **`orderBy`/`sort`** (string names and parent Columns) onto the single resolve rule that S3a established:
- hits by display name under the **live** case rule, with qualifiers;
- distinct ids over those hits;
- 0 hits → UNRESOLVED_COLUMN; one id → bound; more than one → AMBIGUOUS_REFERENCE.

Rules for the moves:
- Keep the predicate **parsing** where it is. Only the binding of each name moves to `resolve`.
- **Delete** the helpers that only this family used, in the same commit. Show by grep that each one has no other caller, and list the deletions. Helpers still used by S3c–S3e stay.
- Never add an "unknown identity" branch (halt rule 3).

## The §9c residue cells this round must close
- `r2.{F,T}_oj_twinjoin_filt`: Spark gives AMBIGUOUS_REFERENCE; main and head give a schema error.
- `r3.{F,T}_ob_agg_max`: Spark gives rows; main and head give a planning error ("Projections require unique expression names").

Each must equal Spark after S3b, or be explained as a named residue with its precise cause and the slice that owns it.

## Gate (the replay, cell by cell against `main.json`)
- Run with `make develop` in the slice, then `replay.py` with S0's main engine name, into `/tmp/oc-worker/direct/wo/attr-id-1/s3b/`, then `compare.py`.
- **0 cells move away from Spark** against `main.json`.
- Every changed cell in the S3b family either becomes EQUAL (a gain; list them) or is a named residue with a reason.
- S3a's gains must not regress: compare with S3a's results too.
- **Timing (§9d):** like for like over the cells whose outcome class is unchanged, at most 1.2x main, median of 3. Report the time on FIXED cells separately.
- The 44-file neighbour sweep with `-n 8`.
- `bash /tmp/xattr/gate.sh` prints GATE GREEN.

## Proof
- Pins under both case rules:
  - `filter("v > 1")` on a twin pair of one attribute (bound);
  - `filter` on two attributes with one display name (AMBIGUOUS_REFERENCE);
  - a qualified predicate on a join;
  - `orderBy` by a string and by a parent Column on a twin frame (this is RC5-7, card SORT-PARENT-COLUMN-1: say whether S3b closes it);
  - `orderBy` over an aggregate (the `r3_ob_agg_max` shape);
  - the case rule changed between building the frame and filtering it.
- Mutation: make `resolve` ignore ids in the filter path. The twin and ambiguity pins go red. Revert, and show `git status` clean.

## Halt when
- A cell EQUAL on main moves away from Spark.
- The round needs an unknown-identity branch.
- The like-for-like timing is above 1.2x.
- A deleted helper has a caller outside the family.

## Commit (commit first, and keep `handback.json` provisional)
- Commit: `feat(attr-id-1): filter predicates and orderBy/sort bind by attribute id through resolve; the family's helpers are deleted (S3b)`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source file, docstrings on new functions and `noqa` prose included. Update `map.md` in lockstep. Extend the ledger.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling.
- Hand-back: `/tmp/xattr/handback.json`.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3b/`. Never touch `~/CodeRepos`. Never use AWS credentials. Do not push. Run every replay child under `ulimit -v 67108864`.
