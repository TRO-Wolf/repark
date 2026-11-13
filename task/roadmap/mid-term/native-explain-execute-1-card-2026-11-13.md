# Card NATIVE-EXPLAIN-EXECUTE-1: native `EXPLAIN EXECUTE` is unsupported

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SIGNED-ZERO-NORMALIZE-1 part B
part 1 verifier 1, 2026-11-13 (finding F7); verifier 2, 2026-11-13 (check 4).

**Status:** filed 2026-11-13, not dispatched. Severity **S4**: a missing native-door diagnostic; `EXECUTE` itself
answers.

## Measured

- Native `EXPLAIN EXECUTE <name>` answers `Unsupported logical plan: Statement(Execute)` (verifier 1); verifier 2
  records that DataFusion renders it as `Execute: …` and shows no plan.
- The `EXECUTE` answers are right (part B cells `n-prepare`, `n-prepare-join`).
- **Consequence:** a plan property of a prepared statement (part B's one-wrapper check) can be proved only by a Rust
  pin (`prepare_then_execute_wraps_each_key_once`), not from the native door.

**Builds:** part B's head `b84adbc7` and its base, main `276dc503`.

## Fix direction

None given by the sources.

## Close

Native `EXPLAIN EXECUTE` shows the bound plan, or refuses with a stated message, pinned.
