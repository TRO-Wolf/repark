# task/wo/ — work orders, by grade

The orders an engine builds from. [TEMPLATE.md](TEMPLATE.md) defines the three grades (A self-directed,
B guided, C clerk) and the skeleton every order follows; the engine band table that says which grade an
engine may run is owner-gated and lives outside the repo. Orders used to live under a `/tmp` work
directory that the box wipes at boot; from 2026-10-01 they are committed here so a reboot cannot take
them.

- [TEMPLATE.md](TEMPLATE.md) — the three grades, the test of a grade-C order, the skeleton, the standing rules every order inherits.
- [t-1-spark-tests-it.md](t-1-spark-tests-it.md) — **T-1, grade C:** `repark-spark`'s 166-entry `src/tests/` into one `tests/it/main.rs` binary; pure move, identical test count, the private-item halt (CL-4, CL-6).
- [c-0-postgres-harness.md](c-0-postgres-harness.md) — **C-0, grade C:** the disposable Postgres container script, the `pg_live` fixture with unique names and cleanup, the five cdc S0 scenario pins, the live-database rules paragraph (CC-6); the Compose-declared bounded container, `make pg-up`/`pg-down`/`pg-url`, four at once, `reap` (owner, 2026-10-02); no product code.
- [ci-1-wheel-smoke-time.md](ci-1-wheel-smoke-time.md) — **CI-1, grade C:** the wheel smoke check from 45–50 minutes to under 25 without weakening it: a docs-only short-circuit, one build fanned out to three facade shards under an aggregate job that keeps the required-check name, the `ci` cargo profile at an opt-level chosen by measurement with a `main`-only cache, and a 30-run proof of the depth-guard flake (owner, 2026-10-02); no edit to `crates/`.
- [ci-2-slow-test-inputs.md](ci-2-slow-test-inputs.md) — **CI-2, grade C:** the ten slowest facade tests on CI-1 run 37121982216 sum to 393.69 s, more than a quarter of the slowest shard (289 s; a quarter is 72.25 s); reuse each immutable input once per pytest-xdist worker, keep every assertion and parameter, move no test between languages; `test_perf_ice_catalog_io_1.py` is a memory pin and stays put (owner, 2026-10-03); no edit under `crates/`.
- [ta-chain-1-leading-prefix.md](ta-chain-1-leading-prefix.md) — **TA-CHAIN-1, grade B (v1.5.2):** a leading NaN/NULL run on a `ta_*` input is skipped before the kernel as polars_talib does, so a chained indicator (`ema` of `trange`) stops answering all-NaN; wrapper-only, kernels and the 158 goldens untouched, prefix goldens recorded through polars_talib, 1.02x cap; Opus 5.5 at high effort for the executor and the verifier (owner, 2026-10-03).
- [attr-id-1/](attr-id-1/) — the ATTR-ID-1 orders (design sketch, S0…S4, the resumes and fixes), moved out of `/tmp` on 2026-10-01 with local paths and session links scrubbed; the design order is the grade-B template.

Up: [../map.md](../map.md).
