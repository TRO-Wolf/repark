# ATTR-ID-1 S2b + S1b (Opus 5.5 executor at high; lane /tmp/xattr, branch feat/attr-id-1, head fe931985)

You are the executor for two consecutive fixes on ATTR-ID-1 in RePark, a pure-Rust Spark/Iceberg engine with a PySpark-compatible facade. Do **Part A first**, commit it, then Part B.

## Read first
1. `/tmp/xattr/AGENTS.md`, and the `map.md` of every directory you touch.
2. The work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1–§3, §5 and §9.
3. The S2 brief `/tmp/oc-worker/direct/wo/attr-id-1-s2-seam.md` and S2's hand-back `/tmp/muse-worker/xattr/20260930T153112Z/handback.json`, which ended IN_PROGRESS/BLOCKED. Read S2's commits too: `git log --stat ce7fe8e2..fe931985`, including 8bc9ddff, c87542b2, 1d6bf45e, 4250c2c8, 0739db1c and fe931985. S2's probes are in `/tmp/oc-worker/direct/wo/attr-id-1/s2/`.
4. The S1b brief `/tmp/oc-worker/direct/wo/attr-id-1-s1b-fix.md` (VA1-1..VA1-6), which is Part B below, and the verifier hand-back `/tmp/oc-worker/direct/wo/verify-attr-s1-opus-handback.json` with its failing tests `/tmp/oc-worker/direct/wo/verify-attr-s1/attr_id_verify.rs`.
5. DataFusion 54.1.0 `src/physical_planner.rs:1020-1075` in the cargo registry. There the Aggregate planner compares the physical input schema to the logical input schema **including field metadata**, and fails with "Physical input schema should be the same as the one converted from logical input schema".

## Part A: S2b, unblocking the seam
S2 stamps every facade frame at construction (`DataFrame.__init__`). It strips `repark.attr` at the sinks and exports, and it re-mints on USING joins. Its own pins are green (13 of 13) and `gate.sh` is green. But **23 neighbour tests fail**:
- g4b_semi_join, 18 tests;
- casesens_1, 3 tests;
- csv/json round trip, 2 tests.

They fail with the physical-planner error above, citing `repark.attr` on the logical side. The isolated trigger is `spark.read.csv(..., inferSchema=True).to_arrow()`; header-only reads pass.

1. **Diagnose before fixing.** Find, for each failing family, where the logical field carries `repark.attr` and the physical field does not, or the reverse. Candidates:
   - an optimizer rule that removes the stamp's pass-through Projection while the logical schema above keeps its metadata;
   - a physical `ProjectionExec`, or a scan, that does not propagate alias metadata;
   - the S2 "born-clean MemTables" or sink stripping creating a schema that differs from the logical plan;
   - the `inferSchema` path re-planning.
   Write a minimal Rust repro test first.
2. **Fix it by construction**, so that the logical and physical schemas agree on metadata at every node DataFusion checks, without weakening DataFusion's check. Acceptable directions include:
   - making sure every node that carries ids in the logical plan also produces them physically;
   - or keeping ids **out of the optimized and physical plans**: stamp or resolve on the unoptimized logical plan only, and strip `repark.attr` in an analyzer or optimizer step before physical planning, since ids are only needed for name resolution in the facade.
   Choose the one that keeps §1's rule intact, and record the choice and the evidence in the design's §9.
   - The second direction is acceptable only if resolution never needs ids after optimization. Check that against §3.4 and §3.6: parent Columns bind against a child frame's unoptimized plan.
3. **Gate for Part A.**
   - All 23 neighbour tests pass.
   - The full neighbour sweep: `test_casesens_1*.py`, every `test_*join*`, `test_*csv*`, `test_*json*`, `test_*write*`, `test_*iceberg*` and `test_*cache*`, with `-n 8`.
   - S2's pins stay green.
   - **The S0 replay is byte-identical to `main.json`** on every judged cell: `make develop` in the slice, then run `/tmp/oc-worker/direct/wo/attr-id-1/replay/replay.py` with the engine name S0 used for main, into `/tmp/oc-worker/direct/wo/attr-id-1/s2b/`, then `compare.py`. Timing: median of 3 at or under 597 s.
   - Commit Part A: `fix(attr-id-1): <what> so logical and physical schemas agree on repark.attr at every checked node (S2 neighbour regressions)`.

## Part B: S1b
Do everything in `/tmp/oc-worker/direct/wo/attr-id-1-s1b-fix.md`, R1 and R2 plus VA1-2..VA1-6, with its proof, mutations M5–M7, its halt rules and its commit, **on top of Part A**.
- If Part A's chosen direction changes where ids live, for example unoptimized plans only, adapt S1b's R1 to that. Structural freshness for Window and Aggregate outputs still holds, and so does idempotence.
- Re-run the S0 replay and the neighbour sweep after Part B. They must stay byte-identical and green.

## Rules for the code
- **No code comments of any kind** in any source file, doc comments and Python docstrings on new functions included. You are an Anthropic model, and the owner's ruling bans them. The comment ban is gated.
- Every Rust file stays at or below 1,000 lines, and every Python file stays within its CAP-1 row. Never raise a ceiling or relax a guard. Keep `map.md` in lockstep. Extend the ledger `task/ledgers/staging/attr-id-1-ledger.md`.
- Build inside the slice: `CARGO_BUILD_JOBS=6 systemd-run --user --scope --slice=repark.slice …`. Run one cargo process at a time. Long runs go in the background with the Bash tool's background mode. Never poll with sleep loops, `pgrep -f` or `tail -F`.
- The gate is `bash /tmp/xattr/gate.sh`, which includes `make rust-panic-ban`. It must print `GATE GREEN` after each part.
- Commits:
  - Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com commit …`.
  - Exactly one trailer: `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`. No the co-author trailer (banned, see the commit-attribution rule), no session trailer, and no session links.
- Do not push.

## Halt when
- Part A cannot make the schemas agree without changing DataFusion.
- The replay changes a judged cell.
- Any path needs a missing id to act as a wildcard.
- S1b's halt rules trigger.

## Hand-back
Write `/tmp/xattr/handback.json` fresh: status, summary, commits, questions (each with a `lean`), Part A's diagnosis, the neighbour sweep results, the replay counts and timing, S1b's pins and mutations, and the line counts. Then reply in 15 lines or fewer.

## Rules
- Work only in `/tmp/xattr` and `/tmp/oc-worker/direct/wo/attr-id-1/s2b/`.
- Never touch `~/CodeRepos`. Never use AWS credentials. Never push. Never delete anything outside those paths.
- If a permission is denied, report it; never ask the orchestrator to run it for you.
- Run `test -e` on every path you cite.
