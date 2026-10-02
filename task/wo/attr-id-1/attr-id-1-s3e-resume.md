# ATTR-ID-1 S3e resume: commit now, then finish the gate (resumed session; lane /tmp/xattr, head 496bbced, 16 files staged and uncommitted)

Your last turn ended `IN_PROGRESS` with nothing committed. **Commit before anything else.** One step budget ran out with the whole change uncommitted, and the next must not.

## 1. Make the staged change committable, then commit it
- **No squeezing.** `case_bind.rs` sits at exactly 1,000 lines. If a file reached the cap because lines were joined or blank lines removed, undo that and split the file instead. Move a cohesive group into a new sibling module under `df_guards/`, or move its `#[cfg(test)] mod tests` into a file under `crates/repark-core/src/session/tests/`, as `attr_id_s3e.rs` already does. Every Rust file should then sit at or below about 950 lines. AGENTS.md bans `#[path]`; use a plain `mod`.
- Run `cargo fmt` (diffs are reported in `session/tests/attr_id.rs`) and check with `cargo fmt -- --check`.
- Bring `map.md` into lockstep for every directory touched, including `python/repark/src/repark/spark/dataframe/` and `scripts/`, and any new Rust module's directory.
- Run the comment ban `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xattr origin/main HEAD` after committing. It must report 0 hits.
- Commit: `feat(attr-id-1): qualified names on join children and alias frames bind by attribute id through resolve (S3e)`, with the TRO-Wolf identity and exactly one trailer, `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`. Never use `--no-verify`. If the hook fails, fix the cause.

## 2. Then finish what the brief still owes, committing as each piece lands
The brief is `/tmp/oc-worker/direct/wo/attr-id-1-s3e-qualified.md`.
1. The pins test file and the three mutations, each red and then reverted with `git status` clean. Commit the pins.
2. Replay ×3 into `/tmp/oc-worker/direct/wo/attr-id-1/s3e/`, in the foreground, with `timeout` and `ulimit -v 67108864`. **Never detach.** Compare against both `main.json` and 496bbced.
3. The like-for-like timing, the 47-file sweep with `-n 8`, and `bash /tmp/xattr/gate.sh` printing GATE GREEN.
4. The ledger and the final hand-back. List the helpers you deleted. Tell S4 whether `__REPARK_QCOL_` and `_origin_plan_id`/`_origin_field` still have readers. Name your four out-of-scope observations as residues, each with an owner.

Keep `handback.json` `IN_PROGRESS` until the gate is green. If steps run low, commit what you have and list exactly what is still owed.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3e/`. Never touch `~/CodeRepos`. Never use AWS credentials. No `.github` changes. Do not push.
