# Ruling on S3c Q1 (same session; lane /tmp/xattr, head eec4765f, tree clean)

**Your lean is adopted.** The orchestrator re-measured the same shapes live with its own probe, and Spark gives AMBIGUOUS_REFERENCE after both `withColumnRenamed` and `withColumnsRenamed` on twins, under both case rules. The ruling is recorded in the design as §9f.

- A rename mints **one fresh id per renamed position**, exactly like an alias in S3a.
- The singular rename keeps its `Column.alias` route. Move the plural rename's native plain alias (`core.py` about 3185) to the fresh-id alias.
- Replace the brief's pin "renamed keeps its id" with: renamed twins, then `filter`, `select(str)` and `select(F.col)`, all giving AMBIGUOUS_REFERENCE under both rules.
- Replace the mutation "rename mints a fresh id" with **"rename keeps the id"**, which must turn those pins red.
- Every other S3c bullet stands as you measured it. **Save your Spark probe outputs** as `.out` files next to the scripts in `attr-id-1/s3c/`. They were missing this round.

Then do the whole S3c brief: the moves, the deletions, the pins, the mutations, the replay gate (foreground only, never detached), timing, the sweep, gate.sh, the commit and `handback.json`.
