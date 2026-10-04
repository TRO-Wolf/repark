# ATTR-ID-1 S3d: move drop / dropDuplicates / fillna / dropna to `resolve` (fresh Muse session, guided; lane /tmp/xattr, branch feat/attr-id-1, head b02dd8bf)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1, §3.4–§3.7, §4 "S3", §5, §9–§9e and **§9f** (rename mints a fresh id per position);
- the S3c brief `attr-id-1-s3c-withcolumn.md` and S3c's final hand-back `/tmp/muse-worker/xattr/20261001T133834Z/handback.json`. S3a–S3c's resolve rule, the live case rule, the shared unique-engine-field guard, the parent-id rebind, the fresh-id alias, Java fold semantics, `chain_id` and the SubqueryAlias overlay are the base you build on;
- the ledger `task/ledgers/staging/attr-id-1-ledger.md`;
- the facade's `core.py` `drop` (about 2437), `drop_duplicates` (about 3045), `fillna` (about 3272), `dropna` (about 3280), and `actions_export.py` `DataFrameNaFunctions.fill` (about 31) and `.drop` (about 248), plus every helper they call to bind or match names.

## Task: the S3d family only
Move the name and Column binding of **`drop(*cols)`**, **`dropDuplicates(subset)`** / `drop_duplicates`, **`fillna` / `na.fill`** (`subset`, and the dict form's keys) and **`dropna` / `na.drop`** (`subset`) onto the single resolve rule. Design §4 says the fan-out is "every position in `Bound`". Measure Spark's semantics and pin them; do not assume. The cases to measure, each under both case rules:
- `drop("v")` where `v` names twins of one attribute, two different attributes with one display, a case variant (`V`), and a miss (expected no-op).
- `drop(F.col("v"))`, and `drop(parent.v)` with a parent Column against a child frame: does it drop by id, by display, or refuse when the display is ambiguous?
- `dropDuplicates(["v"])` on twins, on a two-attribute display and on a miss.
- `fillna(0, subset=["v"])`, `fillna({"v": 0})` and `dropna(subset=["v"])` on twins, on a two-attribute display and on a miss.

If Spark refuses a shape (for example AMBIGUOUS_REFERENCE or UNRESOLVED_COLUMN), RePark refuses with the same error class. If Spark fans out, RePark fans out to every hit position.

Rules for the moves:
- Only the name binding moves to `resolve` and the shared guard. Projection, aggregate and fill building stay where they are.
- **Delete** the helpers only this family used, in the same commit. Show by grep that each has no other caller, and list the deletions. Helpers still used by S3e or S4 stay.
- Never add an "unknown identity" branch (halt rule 3).
- S3c named two `fillna`/`dropna` cells as S3d-owned residues. Close them, or name why not.

## Gate (the replay, cell by cell against `main.json`)
- `make develop` in the slice. Then run `replay.py` with S0's main engine name into `/tmp/oc-worker/direct/wo/attr-id-1/s3d/t-head-N`, then `compare.py`. Reuse S3c's harness (`s3c/replay_timed.py`, `s3c/like_for_like_s3c.py`); copy it into `s3d/`, never edit it in place.
- **Run every replay in the foreground**, with `timeout` and `ulimit -v 67108864`. **Never detach.** A Muse round is a systemd unit, and its detached children die with it.
- **0 cells move away from Spark** against `main.json`.
- No earlier gains are lost: 8,558 FIXED at b02dd8bf, and none of them may regress.
- Every changed cell in the S3d family either becomes EQUAL (a gain; list them) or is a named residue with a reason and the slice that owns it.
- **Timing (§9d):** like for like over the cells whose outcome class is unchanged, at most 1.2x main, median of 3. Report the time on FIXED cells separately.
- The 46-file neighbour sweep with `-n 8`.
- `bash /tmp/xattr/gate.sh` prints GATE GREEN. If a CAP-1 row or the script baseline must ratchet **down** because `core.py` shrank, ratchet both in lockstep.

## Proof
- Pins under both case rules for every measured case above, each measured against Spark with `/tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python` (JAVA_HOME=/usr/lib/jvm/zulu-17-amd64). Batch the Spark cells into one probe. Add one pin each:
  - `drop(parent.v)` after a self-join, which must drop only the parent side's position;
  - the case rule changed between building the frame and calling `drop`.
- Mutations, each turning its pins red, then reverted with `git status` clean:
  - `resolve` ignores ids in this family;
  - `drop(Column)` binds by display instead of id;
  - the subset fan-out stops at the first hit.

## Halt when
- A cell EQUAL on main moves away from Spark.
- The round needs an unknown-identity branch.
- The like-for-like timing is above 1.2x.
- A deleted helper has a caller outside the family.
- Spark's measured semantics contradict the design's "fan-out = every position in `Bound`" for a shape. Halt with the measurement; do not choose.

## Commit (commit first, and keep `handback.json` provisional)
- Commit: `feat(attr-id-1): drop, dropDuplicates, fillna and dropna bind by attribute id through resolve; the family's helpers are deleted (S3d)`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source file, including docstrings on new functions and `noqa` prose. Moved code sheds its comments. Update `map.md` in lockstep. Extend the ledger.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling.
- Hand-back: `/tmp/xattr/handback.json`. Start a fresh one; do not append to S3c's.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3d/`. Never touch `~/CodeRepos`. Never use AWS credentials. No `.github` changes. Do not push.
