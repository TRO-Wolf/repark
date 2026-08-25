# lrs — STATUS record

## Cut from STATUS.md — closed 2026-08-21 by #191

- **Low-risk sweep (LRS)** (chartered 2026-08-20, delivered; branch `fix/low-risk-sweep`,
  eleven commits, **merged as [#191](https://github.com/TRO-Wolf/repark/pull/191)** / `8c660f6`).
  Chartered off `feat/spark-function-parity` @ `8a28057`; rebased onto `main` on 2026-08-21 when
  that campaign squash-merged as `65bacdf`, tree-identical both before and after. Works the
  sub-floor remainder the two Critic rounds forwarded. Design: [docs/design/low-risk-sweep.md](../../design/low-risk-sweep.md);
  slate: [briefs/low-risk-sweep.md](../../../briefs/low-risk-sweep.md); approval gate (10/10 `PROVEN`):
  [task/lrs-0-charter-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-21-lrs-0-charter-ledger.md).
  - **Delivered:** LRS-5 (canonical Rust module layout — all six `#[path]` sites gone), LRS-1
    (four facade paths refuse a higher-order column instead of leaking a DataFusion internal),
    LRS-2 (argument contracts matched to Spark), LRS-7 (a window with no `ORDER BY` frames the
    whole partition), LRS-3 (registry rows `RAND-1` / `BL-8` landed with pins; `randstr` batch
    bound; the SQL door learned `approx_count_distinct`), LRS-6 and LRS-4 (measurement +
    registration).
  - **The campaign's invariant held:** no query that worked before returns a different value.
    Every change turns a failure into a better failure, or registers something already decided.
  - **A live PySpark 4.1.2 + JVM oracle** is installed on this machine and was used to scope every
    unit. It refuted **three** of the Critic round's suggested fixes and one of my own — see
    design §7. It is not a build dependency and CI cannot reach it; every answer it gave is
    transcribed into the ledger that used it.
  - **Two silently wrong answers found and registered, not fixed** (each changes what a working
    query returns, which the charter forbids): `RE-1` — `regexp_extract_all(str, regexp)` returns
    capture group 0 where Spark returns group 1, on both doors; `LOG-1` — `SELECT log(x)` through
    the SQL door returns DataFusion's base-10 answer where Spark returns the natural log. Both are
    ordinary calls on common functions. **Both went to the owner, who ruled on 2026-08-21:**
    `RE-1` closes (SEM-1, below), `LOG-1` is **tabled** and keeps its row.
