# map — .config/

## Purpose

Repository-level configuration for tools that look for a `.config/` directory. Today that is
cargo-nextest only.

## Contents

- `nextest.toml` — the cargo-nextest `default` profile that `make rust-test` runs
  (CI-SPEED-B, 2026-10-18). The file carries no comment; the reasons live here:
  - `fail-fast = false` — one run reports every failing test across every test binary. Plain
    `cargo test` stopped at the first failing binary, so a red run hid the crates after it (the
    2026-10-11 production audit recorded eight crates "not reached").
  - `slow-timeout = { period = "60s" }` — a test that runs past 60 s is named as `SLOW` at
    each period. There is no `terminate-after`, so nextest never kills a slow test; a timeout
    stays the test's own finite bound.
  - No retries (nextest's default of zero is kept). A flaky test fails the run.

The nextest version is pinned once, as `NEXTEST_VERSION` in the [Makefile](../Makefile), and the
same string is the `tool: cargo-nextest@…` pin in `ci.yml`'s `rust-test` job. Bump both in one
change.

## I want to...

| I want to... | go to |
|---|---|
| Change how the Rust workspace suite runs | `nextest.toml` here, and the `rust-test` target in [../Makefile](../Makefile) |
| Bump the nextest version | `NEXTEST_VERSION` in [../Makefile](../Makefile) AND the install step in [../.github/workflows/ci.yml](../.github/workflows/ci.yml) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../DEVELOPMENT.md](../DEVELOPMENT.md) (the `make test` row and the test-command
  discipline), [../.github/workflows/map.md](../.github/workflows/map.md) (the `rust-test` job).

## Debug

| Symptom | First check |
|---|---|
| A test passes under `cargo test` but fails under `make rust-test` | nextest runs each test in its own process, concurrently with tests from every other binary. Look for state a test expected another test in the same binary to set up, or a fixed path, port or service shared across binaries. |
| `SLOW` lines in the output | Information only: the test is still running and nothing is killed. |
| Doctests did not run | nextest does not run doctests. `make rust-test` runs them in a second step, `cargo test --locked --workspace --doc`. |

First checks: `make rust-test`. Escalate to: [../map.md](../map.md).
