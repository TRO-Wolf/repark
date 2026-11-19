# task/wo/avg-decimal-grouped-run-variance-1/evidence/verdicts/ — per-cell verdicts and Spark's answers

Written by [../classify.py](../classify.py). A verdict row is one (query, ANSI mode); its code strings give one code
per configuration (`E`, `W`, `X` = WRONG not exact, `R`, `C` = both raise another condition, `V<n>`).

- [verdicts-pkg-main-4c365814.tsv](verdicts-pkg-main-4c365814.tsv) — main, 2,360 rows × 11 configurations.
- [verdicts-pkg-sum-845766fd.tsv](verdicts-pkg-sum-845766fd.tsv) — #1360's head: the `sum` twins (five
  configurations) and `avg` on `c38_0`, `big`, `t4` and the over-precision files (two).
- [verdicts-pkg-sumx-898e15e6.tsv](verdicts-pkg-sumx-898e15e6.tsv),
  [verdicts-pkg-sumx-5469a59a.tsv](verdicts-pkg-sumx-5469a59a.tsv) — the sizing's prototypes, two configurations.
- [spark-vs-model.tsv](spark-vs-model.tsv) — Spark at both masters beside the exact answer, per (query, mode).
- [spark.jsonl](spark.jsonl) — Spark 4.1.2's raw answers (4,720 records).
- [narrow-main.jsonl](narrow-main.jsonl) — the `Decimal32` / `Decimal64` probe on main (error answers cut to their condition).

Up: [../map.md](../map.md).
