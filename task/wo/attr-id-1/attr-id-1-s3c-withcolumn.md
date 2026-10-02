# ATTR-ID-1 S3c: move withColumn(s) and withColumn(s)Renamed to `resolve` (fresh Muse session, guided; lane /tmp/xattr, branch feat/attr-id-1, head eec4765f)

Read first:
- AGENTS.md, from this clone;
- the work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1, §3.4–§3.7, §4 "S3", §5, §9–§9d and **§9e** (the S3b rulings: unique-engine-field guard, lambda scope, dotted tokens);
- the S3b briefs `attr-id-1-s3b-filter-sort.md` and `attr-id-1-s3b-fix.md`, and S3b's final hand-back `/tmp/muse-worker/xattr/20261001T120119Z/handback.json`. S3a's and S3b's resolve rule, the live case rule, the shared uniqueness guard, the parent-id rebind, the alias fresh id, `chain_id` and the SubqueryAlias overlay are the base you build on;
- the ledger `task/ledgers/staging/attr-id-1-ledger.md`;
- the facade's `core.py` `with_column` (about 1012), `with_columns` (about 1043), `with_column_renamed` (about 3100) and `with_columns_renamed` (about 3134), plus every helper they call to bind or replace names. Find where folded-duplicate keys are refused today: the design calls it `refuse_folded_duplicate_keys`, but grep for the live name.

## Task: the S3c family only
Move **`withColumn` / `withColumns`** (which existing position a written name replaces, versus appending a new one) and **`withColumnRenamed` / `withColumnsRenamed`** (which positions a written existing name renames) onto the single resolve rule. Spark's semantics, which you must measure and pin, not assume:
- `withColumn(name, c)`: if `name` hits existing outputs under the live case rule, **every** hit position is replaced. Measure what Spark does with twins of one attribute and with two different attributes sharing one display. Otherwise the column is appended.
- `withColumnRenamed(existing, new)`: renames **every** position whose display matches `existing` under the live rule. A miss is a no-op, not an error. Measure the twin and two-attribute cases.
- **Folded-duplicate keys in one call** (`withColumns({"a": …, "A": …})` and `withColumnsRenamed`) stay on today's refusal, unchanged (design §4).
- The new column of a `withColumn` gets a **fresh id**, exactly as an alias does in S3a. A renamed column **keeps** its id.

Rules for the moves:
- Only the name binding moves to `resolve` and the S3a/S3b guard. Projection building stays where it is.
- **Delete** the helpers only this family used, in the same commit. Show by grep that each has no other caller, and list the deletions. Helpers still used by S3d or S3e stay.
- Never add an "unknown identity" branch (halt rule 3).

## Gate (the replay, cell by cell against `main.json`)
- `make develop` in the slice. Then `replay.py` with S0's main engine name into `/tmp/oc-worker/direct/wo/attr-id-1/s3c/t-head-N`, then `compare.py`.
- **Run every replay in the foreground**, with `timeout` and `ulimit -v 67108864`. **Never detach**: a Muse round is a systemd unit, and its detached children die with it.
- **0 cells move away from Spark** against `main.json`.
- S3a's and S3b's gains must not regress: 7,672 FIXED at eec4765f, and none of them lost.
- Every changed cell in the S3c family either becomes EQUAL (a gain; list them) or is a named residue with a reason and the slice that owns it.
- **Timing (§9d):** like for like over the cells whose outcome class is unchanged, at most 1.2x main, median of 3. Report the time on FIXED cells separately.
- The 44-file neighbour sweep with `-n 8`.
- `bash /tmp/xattr/gate.sh` prints GATE GREEN.

## Proof
- Pins under both case rules, each measured against Spark with `/tmp/oc-worker/_lib/jvm-lock.sh /tmp/sparkenv/bin/python` (JAVA_HOME=/usr/lib/jvm/zulu-17-amd64):
  - `withColumn` replacing a twin pair of one attribute;
  - `withColumn` on two attributes with one display;
  - `withColumn` appending under a case-variant name (`V` against `v`) in each rule;
  - `withColumnRenamed` on a twin pair, on a two-attribute display and on a miss;
  - `withColumnsRenamed` with a folded-duplicate key (the refusal is unchanged);
  - the id after `withColumn` (fresh) and after `withColumnRenamed` (kept), each followed by a self-join and `filter` that would go AMBIGUOUS if the id were wrong;
  - the case rule changed between building the frame and calling `withColumn`.
- Mutations, each turning its pins red, then reverted with `git status` clean:
  - `resolve` ignores ids in this family;
  - `withColumn` keeps the old id;
  - rename mints a fresh id.

## Halt when
- A cell EQUAL on main moves away from Spark.
- The round needs an unknown-identity branch.
- The like-for-like timing is above 1.2x.
- A deleted helper has a caller outside the family.
- Spark's measured semantics contradict a bullet above. Halt with the measurement; do not choose.

## Commit (commit first, and keep `handback.json` provisional)
- Commit: `feat(attr-id-1): withColumn(s) and withColumn(s)Renamed bind by attribute id through resolve; the family's helpers are deleted (S3c)`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- **No code comments** in any source file, docstrings on new functions and `noqa` prose included. Moved code sheds its comments. Update `map.md` in lockstep. Extend the ledger.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling.
- Hand-back: `/tmp/xattr/handback.json`.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3c/`. Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
