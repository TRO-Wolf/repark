# ATTR-ID-1 S1b: fresh ids for computed Window and Aggregate outputs (Opus 5.5 executor at high; lane /tmp/xattr, branch feat/attr-id-1, on top of the S2 commits)

You are the S1b executor for ATTR-ID-1, in RePark, a pure-Rust Spark/Iceberg engine with a PySpark-compatible facade. S1, the Rust core (ee3ce129, 4570d134, ce7fe8e2), was verified. The verifier found one S1, and this round fixes it together with the verifier's small items. S2 (the facade seam, sink stripping and the USING re-mint) has landed on the same branch before you start. Read its commits, but do not change its scope.

## Read first
1. `/tmp/xattr/AGENTS.md`, and the `map.md` of every directory you touch.
2. The work order `/tmp/oc-worker/direct/wo/attr-id-1-design.md`: §1–§3, §5 and §9.
3. The verifier hand-back `/tmp/oc-worker/direct/wo/verify-attr-s1-opus-handback.json` and its failing tests `/tmp/oc-worker/direct/wo/verify-attr-s1/attr_id_verify.rs`, with their run log beside them.
4. `crates/repark-core/src/session/df_guards/attr_id.rs` (`stamp` at about 60–75, and the Projection inheritance at about 127–129).
5. DataFusion 54.1.0's `expr_schema.rs` in the cargo registry: which aggregate and window functions copy their argument's field metadata.

## VA1-1 (S1): stamp keeps ids that DataFusion copies onto new values
DataFusion 54.1 copies the argument field's metadata onto the outputs of `first_value`/`last_value`, `lag`/`lead`/`nth_value`, and onto cast or negated group keys. `stamp` then keeps those ids:
- an Aggregate or Window root only mints where an id is missing;
- a Projection column inherits a same-op Window or Aggregate output.

So `select(id, lag(id) AS id-twin)` resolves as Bound([0, 1]), where Spark raises AMBIGUOUS_REFERENCE. The facade builds these plans today: `GroupedData.agg`, and first/last lowered to `first_value_udaf`.

## Ruling (the orchestrator's; do not redesign further)
- **R1: identity is decided by structure, never by copied metadata.**
  - For an **Aggregate**, a group expression that is a bare `Column` (possibly under an Alias) inherits its input id. Every other group expression and **every aggregate value** is fresh.
  - For a **Window**, the input pass-through fields inherit, and **every window-expression output** is fresh.
  - For a **Projection**, a `Column` expression inherits only when the field it references is an inherited field of its input. A reference to a computed output of a **same-op** Window or Aggregate directly below it is fresh. The facade built that node in the same operation, so it was never a stamped frame root.
  - "Same-op" means the node directly below the Projection is a Window or Aggregate that is not itself the root of an already-stamped frame. Decide this structurally, for example by position against the node's input width, and not from metadata. If you cannot decide it without metadata, stop and state your lean (see "Halt when").
- **R2: `stamp` stays idempotent.**
  - The fresh ids are set through the explicit `Alias::with_metadata` on the pass-through or root Projection that `stamp` already adds.
  - A second `stamp` must recognise an Alias carrying `repark.attr` metadata as already stamped and keep it.
  - Pin idempotence over every new shape.
- **§3.2's "never rewrite below the root" is amended** to "never rewrite a node that was the root of a stamped frame". A same-op Window or Aggregate may be read below the root to classify its outputs, but it is **not rewritten**; the ids are set at the Projection above it. Record this amendment in the design's §9 and in the ledger.

Also fold these:
- **VA1-2 (S2):** widen the "values carry no id" pins to every aggregate and window function the facade can emit. Cover at least first/last/first_value/last_value, lag/lead/nth_value, min/max/sum/avg/count, collect_list/collect_set, any_value, and the cast/negate/arithmetic group keys. Each pin asserts a fresh id distinct from its argument's.
- **VA1-3 (S3):** `lateral_join` re-mints colliding right-side ids like `requalify_join_sides`, with a pin.
- **VA1-4 (S3):** the join no-op pin asserts that the ids are present and equal, not only equal.
- **VA1-5 (S3):** a `repark.attr` key already present on a source schema is never trusted. `stamp` of a source (TableScan, Values, or any root that is not a Projection) re-mints over foreign ids once. Record the rule, and pin it with a parquet file whose footer carries a `repark.attr` key; S2's sink stripping should make such files rare. Check that this does not break idempotence: a frame stamped by RePark must keep its ids across a re-stamp. That needs a way to tell RePark-minted ids from foreign ones, such as a per-process prefix or a session nonce in the id. Choose the minimal design, and record it.
- **VA1-6 (S3):** remove the `/tmp` path from `df_guards/map.md` and repair the garbled clause.
- The verifier's 7 tests in `attr_id_verify.rs` must pass once they are ported into the crate's test module; they are the regression pins.

## Proof
- All pins green, the verifier's tests included.
- Mutations: M5 lets Window outputs inherit copied ids (the lag or lead pins go red); M6 lets Aggregate values inherit (the first_value pins go red); M7 does not recognise stamped aliases (the idempotence pins go red). Revert each, and show `git status` clean. Record them in `task/ledgers/staging/attr-id-1-ledger.md`.
- Rerun S2's facade pins and the S0 replay on this build (`make develop` in the slice, then `replay.py` as S2 ran it, and `compare.py` against `main.json`). The target is still **0 changed judged cells**, because nothing reads ids yet. Timing: at or under 597 s.

## Rules for the code
- **No code comments of any kind** in any source file, doc comments included. You are an Anthropic model, and the owner's ruling bans them. The comment ban is gated.
- Every Rust file stays at or below 1,000 lines. Never raise a ceiling or relax a guard. Keep `map.md` in lockstep.
- Build inside the slice: `CARGO_BUILD_JOBS=6 systemd-run --user --scope --slice=repark.slice cargo …`. Run one cargo process at a time. Long runs go in the background, and you wait on them with the Bash tool's background mode. Never poll with sleep loops, `pgrep -f` or `tail -F`.
- The gate is `bash /tmp/xattr/gate.sh`, and it must print `GATE GREEN`.
- Commits:
  - Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com commit …`.
  - Exactly one trailer: `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`. No the co-author trailer (banned, see the commit-attribution rule), no session trailer, and no session links.
  - Prefix `fix(attr-id-1): …`, with at most 2 commits.
- Do not push.

## Halt when
- R1 cannot tell a same-op node from a stamped frame root without relying on metadata. State your lean.
- The replay changes a judged cell.
- A DataFusion function's output field cannot be overridden from a Projection above it.

## Hand-back
Update `/tmp/xattr/handback.json`: status, summary, commits, questions (each with a `lean`), the pins, the mutations, the replay counts and timing, and the line counts. Then reply in 12 lines or fewer.

## Rules
- Work only in `/tmp/xattr`.
- Never touch `~/CodeRepos`. Never use AWS credentials. Never push. Never delete anything outside `/tmp/xattr`.
- If a permission is denied, report it; never ask the orchestrator to run it for you.
- Run `test -e` on every path you cite.
