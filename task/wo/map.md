# task/wo/ — work orders, by grade

The orders an engine builds from. [TEMPLATE.md](TEMPLATE.md) defines the three grades (A self-directed,
B guided, C clerk) and the skeleton every order follows; the engine band table that says which grade an
engine may run is owner-gated and lives outside the repo. Orders used to live under a `/tmp` work
directory that the box wipes at boot; from 2026-10-01 they are committed here so a reboot cannot take
them.

- [TEMPLATE.md](TEMPLATE.md) — the three grades, the test of a grade-C order, the skeleton, the standing rules every order inherits.
- [t-1-spark-tests-it.md](t-1-spark-tests-it.md) — **T-1, grade C:** `repark-spark`'s 166-entry `src/tests/` into one `tests/it/main.rs` binary; pure move, identical test count, the private-item halt (CL-4, CL-6).
- [c-0-postgres-harness.md](c-0-postgres-harness.md) — **C-0, grade C:** the disposable Postgres container script, the `pg_live` fixture with unique names and cleanup, the five cdc S0 scenario pins, the live-database rules paragraph (CC-6); bounded containers under rootless Docker in `repark.slice`, four at once, `reap` (owner, 2026-10-02); no product code.
- [attr-id-1/](attr-id-1/) — the ATTR-ID-1 orders (design sketch, S0…S4, the resumes and fixes), moved out of `/tmp` on 2026-10-01 with local paths and session links scrubbed; the design order is the grade-B template.

Up: [../map.md](../map.md).
