# Unit ledger — REVIEW-FIX-2 · CFG-1's own pins stop reading the developer's HOME

**Unit:** REVIEW-FIX-2 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-1-2` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The REVIEW-1 sweep's Q-1 finding, confirmed and re-run by the orchestrator:
two pins in `config_file::tests` read ambient process state, so anyone following
`docs/guide/repark-toml.md` and writing a home file cannot run the suite. One round,
tier M, Rust tests only.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Not in this step:** REVIEW-FIX-1 (done, previous commit, untouched), REVIEW-FIX-8's
probe (a later card owns `scripts/profiles1_probe.py`), any production code, any
dependency file, `STATUS.md`, `briefs/next-sequence.md`.

**Design note.** D-2's parenthetical (`REPARK_CONFIG=""` or the environment parameter)
cannot be spelled literally in-process: mutating the process environment is the unit's
own D-8 violation, and under edition 2024 `std::env::set_var` is `unsafe`, which
`unsafe_code = "forbid"` refuses — no pin in the crate mutates the environment today.
The conforming spelling of "discovery disabled" is the forced path: `load_file_config`
with `Some(path)` never calls `discover()`, so the C-023 control session builds from a
forced empty staged file through the existing `staged_file` helper. No new mechanism,
no new helper name. The PROFILES-1 probe's `REPARK_CONFIG=""` is the same rule on its
side (subprocess environments are settable); the shared spelling is "the control sees
no discovered file", stated here for that card's reuse.

## PROPOSITION LEDGER — REVIEW-FIX-2 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: no pin calls the public `load()` without a stub environment — the empty-config pin drives `discover` / `load_file_config` with `home: None` and an explicit stub environment, per D-8. | `load_without_a_file_is_the_empty_config` (re-homed, same name) | **PROVEN** | Red first: with `HOME` pointed at a scratch dir containing `.config/repark/repark.toml` (`[default.conf]` `probe_from_home = "1"`), `cargo test -p repark-core config_file` fails 2 of 63, this pin panicking at `tests/mod.rs:43` with `left: ConfigFile { profiles: {"default": Profile { conf: Some({"probe_from_home": String("1")}) ... }}}` vs `right: ConfigFile { profiles: {} }`. Green after: `63 passed; 0 failed` under both the real and the stub `HOME`. |
| C-002 | D-2: the C-023 control session builds with discovery disabled, so the byte-identical-to-`.config()` dump compares the same two things on any machine. | `file_built_session_registers_the_same_catalogs_as_config_calls` (control arm re-homed onto a forced empty file) | **PROVEN** | Red first: same stub-`HOME` run, this pin panicking at `tests/wiring.rs:315` on the `paired(from_file.conf_dump()) == paired(from_calls.conf_dump())` equality (the ambient `probe_from_home` pair reaches only the undiscovered control side). Green after: the control builds from `staged_file("")`, `63 passed; 0 failed` under both `HOME`s. The FIX-7 discovered-versus-named warning pins are untouched and green in the same runs. |

VERDICT: 2 clauses, 2 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each card decision maps to one clause with its named pin; the stub-HOME run is the pin that holds both decisions at once.
      artifacts: [task/ledgers/staging/review-fix-2-ledger.md, crates/repark-core/src/config_file/tests/mod.rs, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The adversarial input is a real developer HOME (visible conf key through the home discovery path); the boundary is the suite green under both the real and the stub HOME.
      artifacts: [task/ledgers/staging/review-fix-2-ledger.md]
    - id: AT-3
      status: ATTACKED
      evidence: No refusal behavior changed: the re-homed pins assert the same outcomes (empty default, equal dumps); every refusal pin in the battery stays green under both HOME settings.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-4
      status: N/A
      justification: No concurrency, locks, or shared state; the fix removes the only ambient reads instead of guarding them.
    - id: AT-5
      status: ATTACKED
      evidence: The REVIEW-FIX-7 discovered-versus-named warning coverage is not weakened: all five warning pins are untouched and green under both HOME settings in the same runs.
      artifacts: [crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-6
      status: ATTACKED
      evidence: A forced empty file behaves exactly like no file for the control (empty pairs, empty dump, default knobs); the C-023 equality and all round-trip pins stay green.
      artifacts: [crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-7
      status: N/A
      justification: No hot loop, growth, or resource touch; one bounded discovery call per pin over tempdirs.
    - id: AT-8
      status: ATTACKED
      evidence: Existing seams only (discover, load_file_config, staged_file, from_config_file); no new mechanism, no environment mutation, no unsafe (set_var would need it under edition 2024).
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Failure and refusal messages are unchanged; the re-homed asserts keep their debug dumps ({:?} on provenance and pairs).
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Every added branch has a naming input: discover-None plus default load on the empty pin, forced-empty control on the C-023 pin, both exercised by the stub-HOME run.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs, crates/repark-core/src/config_file/tests/wiring.rs]
  complete: true
```