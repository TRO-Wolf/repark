# ATTR-ID-1 S1: the Rust core (Opus 5.5 executor at high; lane /tmp/xattr, branch feat/attr-id-1 at origin/main db3a1f37)

You are the S1 executor for ATTR-ID-1. RePark is a pure-Rust Spark/Iceberg engine with a PySpark-compatible facade. The owner adopted the work order on 2026-09-30, and S1 is yours: the design-heavy round.

## Read first
1. `/tmp/xattr/AGENTS.md` (the repo contract), then the `map.md` of every directory you touch.
2. The work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: all of it, especially §1–§3, §4 "S1" and §5. It is authoritative for this round.
3. The S0 result, `/tmp/oc-worker/direct/wo/attr-id-1/replay/summary.txt`:
   - 43,996 cells; main ≠ Spark on 27,648; #881 ≠ Spark on 18,914;
   - 807 cells EQUAL on main that moved away on #881, 799 of them mapped to RC findings;
   - replay time 498 s on main and 543 s on #881.
   Its driver is `replay/replay.py`, and its comparator is `replay/compare.py`. You do not run the replay in S1: there is no facade change. It is here so you know what S2–S3 will judge.
4. DataFusion 54.1.0 in the cargo registry (`~/.cargo/registry/src/*/datafusion-expr-54.1.0`, `datafusion-common-54.1.0`), for the §2 facts.
5. On main: `crates/repark-core/src/session/df_guards/case_bind.rs` (including `requalify_join_sides`) and `crates/repark-python/src/dataframe_names.rs`. `sort_names.rs` and `written_names.rs` exist only on #881's branch (`/tmp/xs-cs1`, read-only for you). S1 does not depend on them.

## Deliver exactly §4 S1
1. **The propagation pins first**, one per row of §3.5. Each asserts the ids by position after the node:
   - Column and Alias(Column) inherit;
   - Cast and arithmetic are fresh;
   - Filter, Limit, Sort, Distinct and SubqueryAlias keep the same ids;
   - Join: the left side keeps its ids, and a colliding right-side id is re-minted;
   - Union: first measure what DataFusion does, then pin what RePark guarantees;
   - Aggregate keys keep their ids;
   - a temp-view round trip keeps them.
   Commit the pins that pass on DataFusion alone first. A §2 "to verify" row whose test contradicts §2 is a **HALT** with the measured behaviour. Never change the rule silently.
2. **`crates/repark-core/src/session/df_guards/attr_id.rs`:** `AttrId`, `stamp`, `attribute_ids`, `resolve`, and the join re-mint helper, as specified in §3.1–§3.4. Re-export them through `frame_names`, with at most 3 `pub use` lines in `case_bind.rs`; `case_bind.rs` must not grow beyond that.
3. **`crates/repark-python/src/dataframe_names.rs`:** three pyfunctions: `stamp_attribute_ids`, `attribute_ids` and `resolve_display_name`, with the signatures given in §4 S1. `requalify_join_sides` gains the re-mint.
4. **`resolve` pins** over `(id, Data, s)` under both rules: one hit; two hits with one id; two hits with two ids; a qualified hit; a qualified miss; folded-duplicate keys.
5. **Mutations M1–M4** as §4 S1 lists them. Run each, show that its pins go red, and revert it; show `git status` clean after each. Record them in a new staging ledger, `task/ledgers/staging/attr-id-1-ledger.md`, following the grammar of neighbouring ledgers, with clauses and residues.
6. **No facade change** and no Python edits in S1.

## Rules for the code
- **No code comments of any kind** in any source file, doc comments (`///`, `//!`) included: you are an Anthropic model, and the owner's ruling bans them. Reasons and design notes go in `map.md` and the ledger. The comment ban is gated (`python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xattr origin/main HEAD`).
- Every Rust file stays at or below 1,000 lines. Never raise a ceiling or relax any guard. Update `map.md` in lockstep in every commit that adds a file.
- `AttrId` is never derived from a name, a position or a plan id (§3.1). A missing id after `stamp` is a bug, never a wildcard (§3.3, halt rule 3).

## Builds
- Run every cargo command inside the repark slice, e.g. `CARGO_BUILD_JOBS=6 systemd-run --user --scope --slice=repark.slice cargo test -p repark-core --lib attr_id`.
- One cargo process at a time. Long runs go in the background, and you wait on them with the Bash tool's background mode. Never use `sleep` polling loops, `pgrep -f` or `tail -F`.
- The full gate is `bash /tmp/xattr/gate.sh`. It covers the comment ban, the full repark-core lib tests, the repark-python lib tests, clippy with `-D warnings`, fmt, check_lib_rs, rust file size, ruff, ledger grammar, docs links and map.md. Run it at the end; it must print `GATE GREEN`. It writes its results into `/tmp/xattr/handback.json`, which is git-excluded.

## Commits (one round; you may split pins, core and pyfunctions into up to 3 commits)
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com commit …`.
- Exactly one trailer line: `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`. **No the co-author trailer (banned, see the commit-attribution rule), no session trailer, and no session links.** The pre-push hook rejects them.
- Message prefix: `feat(attr-id-1): …`.
- Do not push, and do not open a PR.

## Halt when (the work order's §5, plus these)
- A §2 "to verify" test contradicts the expected propagation. Report the measured behaviour; do not adapt the rule.
- Any path would need to treat a missing id as anything but a bug.
- `case_bind.rs` or any file would pass 1,000 lines.
- The design is ambiguous on a point that changes behaviour. Stop and ask, stating your lean; do not invent.

## Hand-back
Update `/tmp/xattr/handback.json`: add `status` (CONCLUDED or HALT), `summary`, `commits`, `questions` (each with a `lean`), the propagation table as measured (node → inherit / fresh / same, with the test name), the mutation results, and the product-line and test-line counts. Then reply in 15 lines or fewer.

## Rules
- Work only in `/tmp/xattr`. Read `/tmp/xs-cs1` and `/tmp/oc-worker/direct/wo/` without writing to them.
- Never touch `~/CodeRepos`. Never use AWS credentials. Never push.
- Never delete anything outside `/tmp/xattr`.
- If a permission is denied, report it. Never ask the orchestrator to run it for you.
- Run `test -e` on every path you cite.
