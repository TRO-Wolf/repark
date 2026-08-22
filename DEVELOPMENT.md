# DEVELOPMENT.md — build, test, verify

The contributor how-to: local setup, the `make` targets that matter, formatting, the CI surface, and
troubleshooting. For the rules that govern a change read [AGENTS.md](AGENTS.md); for the testing
contract read [docs/testing.md](docs/testing.md) (this file does not restate it); for current state
read [STATUS.md](STATUS.md).

## Prerequisites

- **Rust** — the exact toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml); with
  `rustup` installed it is provisioned automatically on first `cargo` invocation (channel +
  `rustfmt` + `clippy`). Do not override the channel; bump it only in lockstep with the
  DataFusion/iceberg pin.
- **Python ≥ 3.12** — the version floor is in [`.python-version`](.python-version). The Python
  side is a [uv](https://docs.astral.sh/uv/) workspace; install `uv` and it manages the virtualenv
  and the locked dependencies (`uv.lock` is checked in and validated, never rewritten, by
  `uv lock --locked`).
- **maturin** — builds the PyO3 wheel / editable native module. It is invoked through `uvx` at a
  pinned version by the Makefile, so no separate install is required for the `make` targets.
- Linters/formatters (ruff, taplo, typos, zizmor) and the security tools (cargo-deny, cargo-audit,
  pip-audit) are all run at pinned versions by the Makefile via `uvx` / install-action — they
  never silently skip locally, and their pins match the workflow pins exactly.

No JVM is needed for the normal build/test/verify loop. A Java 17 home is needed **only** for
`make parity-live` (re-deriving Spark goldens from real Spark).
The Spark door parses floating-point SQL literals (e.g. `1.5`) as DECIMAL, matching Spark (`datafusion.sql_parser.parse_float_as_decimal=true`); the ANSI door is unchanged.

## The commands that matter

`make help` lists every target. The ones you will use:

| Command | What it does |
|---|---|
| `make ci` | **The canonical fast gate.** fmt-check + clippy + panic/async bans + crate dependency policy + lib.rs thinness + rust file-size (`crates/**/*.rs` ceilings; SSOT `scripts/check_rust_file_size.py`) + lib.py thinness + structural manifest + `cargo check` + ruff lint/format + uv-lock check + toml + spell. Mirrors the CI `ci.yml` job. |
| `make test` | The **Rust workspace** suite (`cargo test --locked --workspace`) — and that is deliberately all of it. The Python suites need something `cargo test` cannot give them (see below). |
| `make verify` | `ci` + `test` — full local verification. **A change is not done until `make verify` is green** and the touched directories' `map.md` files are current. |
| `make py-test-facade` | The **facade** suite (`python/repark/tests`) against the real native module: provisions the four declared extras (`numpy`, `pandas`, `polars`, `ml-ext`) from `uv.lock`, runs `maturin develop`, then pytest. Run it when you touch the facade — `make verify` does not. |
| `make py-test` | The **parity** harness (`python/repark-parity/tests`) — pyarrow + pydantic (PYC-4), no native build, no JVM. Mirrors the CI step. |
| `make preflight` | The pre-PR gate: `verify` + the facade suite (`make py-test-facade`) + the security/workflow gates CI also runs. Roster: [AGENTS.md](AGENTS.md) "Verify before done". |
| `make format` | Autoformat Rust + Python (`cargo fmt`, `ruff format`). |
| `make lint` | Clippy `-D warnings` + ruff (autofix Python). |
| `make develop` | Build + install the native module editable into the root `.venv` (`maturin develop`), for exercising the Python facade against real compiled code. |
| `make build-wheel` | Build the release wheel with maturin. |
| `make install-hooks` | Wire the pre-commit hook (map.md lockstep + map.md link validity + crate dependency policy + thinness guards + rust file-size + structural manifest + fmt/taplo/typos). Do this once per clone. |
| `make check-map-sync` | The map.md **content** guard: every relative link in every tracked `map.md` resolves. Measured n=5 median **0.08 s** over 143 maps, so it is on pre-commit. The coverage rule (every mappable file mentioned by its directory's map) is behind `--strict` and **not** armed — 24 pre-existing findings at 2026-08-22, a floor rather than an exact debt; run `python3 scripts/sync_map_md.py --check --strict` by hand. |
| `make check-python-conventions` | The Python conventions guard on its own: no `def` nested inside a `def`, no `dataclasses`/`attrs` import. Re-measured at PYC-5: n=5 median **0.996 s** (max 1.011 s) over 164 files — at the sub-second hook budget line, with the max already over it, so it is **not** on pre-commit; dual-wired `make ci` + CI. |

### Test-command discipline (hard)

Run tests as **`cargo test --workspace`** (what `make test` does). **Never `--all-features`:** the
all-features flag turns on `repark-python`'s `extension-module`, which tells PyO3 not to link
libpython and breaks a standalone test binary. The Makefile, CI, and this rule all agree. See
[docs/testing.md](docs/testing.md) and [AGENTS.md](AGENTS.md) "PyO3 build notes". The cdylib is
validated separately via the maturin wheel + an import smoke test, not via `cargo test`.

**Where each suite runs.** `make test` (and so `make verify`) is JVM-free and native-build-free on
purpose, which is why it covers the **Rust workspace only** — not because the Python side is
unfinished. The **facade** suite needs the compiled native module, so it lives behind a build
step: locally `make py-test-facade`, and in CI the `wheels.yml` **smoke** job, which runs that same
suite against the *packaged wheel* with the four extras installed (a facade regression must not
pass CI on an import smoke alone). The **parity** harness runs in `ci.yml` (and `make py-test`).
The **live-Spark oracle** tier needs a JVM: `make parity-live` / `parity-live.yml` only.

The testing **contract** (tests land in the same commit as the code; test-per-change; the
entry-point matrix; divergence-class claims) is in [docs/testing.md](docs/testing.md) — read it
before any code change.

## Formatting + house style

- **Rust:** rustfmt `max_width = 100`, edition 2024 (`rustfmt.toml`); clippy `all` + `pedantic`,
  `-D warnings`; `unsafe_code = "forbid"` everywhere except `repark-python`. Section-function doc
  banners are hand-authored (see AGENTS.md "Rust house style" / the per-crate maps).
- **Python:** ruff lint + format, `line-length = 100`; type hints on every signature; Pydantic v2
  for structured config; `pathlib`; `logging` not `print`; never a bare `except`.
- **TOML:** taplo format + lint. **Spelling:** typos (domain vocabulary is allow-listed in
  `.typos.toml`).

## The CI surface

CI mirrors the local gates at identical tool pins. Tier-1 (runs on every PR, no cloud creds):

- `ci.yml` — the `make ci` chain (fmt, clippy, panic/async bans, crate dependency policy,
  thinness, structural manifest, `cargo check`, ruff, uv-lock, toml, spell) plus the workspace
  tests and the Python parity step.
- `taplo.yml`, `typos.yml`, `zizmor.yml` (workflow linter, blocking), `cargo-deny.yml`, `audit.yml`,
  `pip-audit.yml` — the format/lint/security panels `make preflight` also runs.

Tier-2 (live AWS / real Spark) **never runs against unmerged code** — nightly on `main` and manual
`workflow_dispatch` only, via OIDC role assumption, no self-hosted runners: `aws-acceptance.yml`,
`parity-live.yml`, and `wheels.yml` (release wheels). See [.github/map.md](.github/map.md).

## Troubleshooting

| Symptom | First check |
|---|---|
| `cargo test --all-features` fails to link (libpython) | Never use `--all-features`; use `cargo test --workspace`. See the discipline note above. |
| Pre-commit hook rejects a commit | Run `bash scripts/check_map_md.sh` — the touched directory's `map.md` must be staged in the same commit. `make install-hooks` if the hook is not wired. |
| `map-sync: … dead link` | A `map.md` points at a path that moved or was deleted. Repoint the link, or `python3 scripts/sync_map_md.py --fix` when the whole list row should go (mechanical only — it never writes a description). |
| `python-conventions: FAIL` | A `def` inside a `def`, or a `dataclasses`/`attrs` import. Lift the function to module or class level, or add an inline `# nested-def: <reason>` pragma if it is one of the three sanctioned cases; use a Pydantic `BaseModel` instead of a `dataclass`. The EXCEPTIONS tables in `scripts/check_python_conventions.py` ratchet **down** only. Reasoning: [.agents/skills/code-quality/SKILL.md](.agents/skills/code-quality/SKILL.md). |
| `crate-dag: layering inversion` | A new dependency points up a tier — see `scripts/check_crate_dag.py` (the SSOT) and [crates/map.md](crates/map.md). |
| `undeclared dependency edge` / `dependency kind not permitted` (the crate-DAG guard) | Every internal edge is declared with its kind (`normal`/`optional`/`dev`/`build`) in `scripts/check_crate_dag.py` `ALLOWED_EDGES`; add the row with a reason, or drop the dependency. |
| `manifest: FAIL` | `repo-manifest.toml` disagrees with reality — a Cargo member is undeclared, a declared doc or `make` target is gone, STATUS.md moved the milestone, or a crate map lags. `make check-manifest` names the field. |
| `unsafe` lint fires | Only `repark-python` may use `unsafe`; keep FFI there. |
| `uv lock --locked` is RED | `uv.lock` lags a `pyproject.toml` floor bump — run `uv lock` and commit the result. |
| The facade suite is green but a polars / ML path was never exercised | The `.venv` is missing an extra, and `importorskip` turns a missing extra into a silent **skip**. `make py-test-facade` provisions all four (`numpy`, `pandas`, `polars`, `ml-ext`) from `uv.lock`; a bare `uv sync` provisions only the root `dev` group. Compare the polars/ML skip *deltas* against the recorded full-extras cohort ([docs/port/census.md](docs/port/census.md) §4) — a developer `.venv` also carries the `dev` group's duckdb, which §4 requires absent, so the absolute counts will differ by those sites; the extras delta is the load-bearing number. |
| A lint passes locally but fails in CI (or vice-versa) | Tool pins drifted — Makefile pins must equal the workflow pins; bump both in one change. |
| PyO3 build can't find Python | `.cargo/config.toml` sets `PYO3_PYTHON`; confirm a `python3` with `libpython3.x` is on PATH. |

Escalate through the touched directory's `map.md` "Debug" section, then [AGENTS.md](AGENTS.md).
