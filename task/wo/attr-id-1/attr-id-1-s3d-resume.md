# ATTR-ID-1 S3d resume: finish the gate, and move the Java folds to Rust first (resumed session; lane /tmp/xattr, head 7bc788dd)

Your last turn ended with `IN_PROGRESS`. You still owe the map.md and ledger updates, the mutation re-run, replay t-head-2 and t-head-3, the timing, the 46-file sweep and gate.sh. Before any of that, one orchestrator ruling applies.

## Ruling R-S3d-1: the Java case folds live in Rust (the owner's Rust-first rule)
7bc788dd put Java case-folding in Python: the `_JAVA_UPPER_SINGLE` and `_U13_*_IDENTITY` tables, `_java_upper_char`, `_java_lower_char`, `_fold_a_equal` and `_fold_b_equal` in `python/repark/src/repark/spark/subset_resolve.py`. S3c added a third, approximate copy, `_java_case_equal` in `python/repark/src/repark/spark/column_fields.py`. The owner's standing rule is that everything that can be Rust is Rust, and the Python facade stays thin. Three fold implementations will drift.

Do this as its own commit:
1. Move the tables and both comparisons into Rust, in a new module `crates/repark-common/src/java_case.rs`. It exposes `fold_a_equal(&str, &str) -> bool` (codepoint `toLowerCase`) and `fold_b_equal(&str, &str) -> bool` (codepoint `equalsIgnoreCase`). Port your full-codepoint fuzz check as a Rust unit test against the same dump; keep the dump fixture out of the source tree if it is large.
2. Expose one native function in `crates/repark-python/src/dataframe_names.rs`, for example `java_fold_hits(written: &str, displays: Vec<String>, mode: &str) -> Vec<usize>` with mode `"a"` or `"b"`. It returns the hit positions whose display differs from `written` but folds equal to it. Exact hits stay where they are today.
3. `subset_resolve.py` calls the native function and deletes its tables and fold helpers. S3c's `_java_case_equal` also routes through the native fold B and is deleted. Measure first: if any S3c pin changes, halt with the cell.
4. **Do not change `NameRule::matches` or `NameRule::lookup`** in `crates/repark-common/src/names.rs`. They are ASCII-only and serve S3a/S3b and many other callers. Converging them on Java folds belongs to the unicode-case card (RC3-5), not to this round. Name that gap as a residue in the ledger.
5. Every Rust file stays at or below 1,000 lines. No code comments, no docstrings on new functions and no `noqa` prose. Update `map.md` in lockstep in `crates/repark-common/src/`, `crates/repark-python/src/` and `python/repark/src/repark/spark/`. If the new module needs a `lib.rs` line, run `bash scripts/check_lib_rs.sh`.
6. Commit: `refactor(attr-id-1): Java case folds move to repark-common; the facade calls one native fold (S3d R-S3d-1)`, with the TRO-Wolf identity and exactly one trailer, `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.

## Then finish the S3d gate as the original brief says
The brief is `/tmp/oc-worker/direct/wo/attr-id-1-s3d-drop-na.md`. Run it on the head after R-S3d-1:
- `make develop`, then replay t-head-2 and t-head-3 into `/tmp/oc-worker/direct/wo/attr-id-1/s3d/`, in the foreground, with `timeout` and `ulimit -v 67108864`. **Never detach.**
- Compare against `main.json`. Expect 0 cells moved away from Spark, and none of the 8,558 FIXED at b02dd8bf lost. Your note that `main.json` predates S3a–S3c is correct. A cell counts as a gain only if it is EQUAL to Spark; a cell that is "stale-baseline" must still be EQUAL to Spark on head.
- Run the like-for-like timing, median of 3, at most 1.2x. Run the 46-file sweep with `-n 8`. `bash /tmp/xattr/gate.sh` must print GATE GREEN.
- Re-run the three brief mutations after R-S3d-1, each going red and then reverted, with `git status` clean.
- Name the two residues you found: join-above-union single-bind is unprobed, and flipTF/T-caseF build-time resolution is S3a/S3b territory. Give each one its owning slice.
- Bring map.md and the ledger up to date. Commit them as their own commit before the final hand-back.

## Commit-first and the hand-back
Commit each step as it lands, and keep `/tmp/xattr/handback.json` provisional (`IN_PROGRESS`) until the gate is green. Then write `CONCLUDED`, listing commits, gates, deletions, residues and the R-S3d-1 change. If you run low on steps, commit what you have and say exactly what is still owed.

## Rules
Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s3d/`. Never touch `~/CodeRepos`. Never use AWS credentials. No `.github` changes. Do not push.
