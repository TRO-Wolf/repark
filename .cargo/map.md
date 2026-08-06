# map — .cargo/

## Purpose

Workspace-wide cargo configuration.

## Contents

- `config.toml` — sets `PYO3_PYTHON = "python3"` so the PyO3 build script finds an interpreter on
  systems that ship only `python3` (not `/usr/bin/python`). Inert until `repark-python` lands
  (phase 3); maturin overrides it when building wheels.
- `audit.toml` — `cargo audit` ignore list (the `make rust-audit` gate). Kept in sync with
  `deny.toml` `[advisories].ignore`; both empty at phase 0.

## I want to...

| I want to... | go to |
|---|---|
| Ignore a RustSec advisory | `audit.toml` AND `deny.toml` `[advisories].ignore` (both, with reason + revisit trigger) |
| Change cargo env configuration | `config.toml` |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../deny.toml](../deny.toml), [../Makefile](../Makefile) (`rust-audit` / `rust-deny`).

## Debug

| Symptom | First check |
|---|---|
| PyO3 build: "failed to run the Python interpreter" (phase 3+) | Ensure `python3` is on PATH; `config.toml` points PyO3 at it |
| cargo-audit and cargo-deny disagree on an advisory | The two ignore lists drifted — re-sync `audit.toml` and `deny.toml` |

First checks: `cargo audit`, `cargo deny check all`. Escalate to: [../map.md](../map.md).
