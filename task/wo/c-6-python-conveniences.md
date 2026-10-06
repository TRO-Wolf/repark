# C-6 — the Python conveniences and the CC-4 registry rows · grade-B skeleton · standard tier · 1.6

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why, and what is out

C-6 closes 1.6 for Python: `read_database` / `write_database` conveniences only
(the SQL door stays primary: one namespace, no per-source function — the 2026-08-29
federated-SQL ruling), plus the CC-4 registry rows for the RePark-owned conditions
(card 1.6). Out: engine behavior changes (all reads/writes already work through
SQL by C-5).

## 1. Rulings already made

- R-1 (engine): the **standard tier** with one verifier per stack and DIFF-PROBE
  (card 1.6).
- R-2 (surface): `read_database` / `write_database` conveniences only, in
  `python/repark/src/repark/io/` (card 1.6). **PENDING on C-2/C-4**: the exact
  signatures follow the provider and sink surfaces those units land, including the
  C-4 flag surface (R-4 there).
- R-3 (errors): connector errors carry structured operational meaning first;
  Spark wording only where measured, else RePark-owned conditions on the
  `CommitStateUnknown` precedent, each with a dated registry row (CC-4). The row
  list is **PENDING on C-2…C-5**: it enumerates the conditions those units
  introduce (authentication failure, retryable disconnect, invalid specification,
  and whatever the build finds).
- R-4 (auth rows exist): the ES-1 IAM-token and Kerberos rows land in C-1 (R-6
  there); C-6 adds the rest, it does not re-decide them.
- R-5 (ledger): cites ConnectorX and ADBC per the 1.6 row's standing instruction
  (owner, 2026-09-13).

## 2. Files

`python/repark/src/repark/io/` (the conveniences);
`docs/spark-sql-iceberg-parity.md` (the R-3 rows); Python pins beside the facade
suite; `map.md` lockstep. **PENDING on C-2/C-4**: the convenience signatures and
their pin files.

## 4. Steps

1. The conveniences on the landed C-2/C-4 surfaces; one commit per slice.
2. The R-3 registry rows for every RePark-owned condition C-2…C-5 introduced.
3. Facade-suite green; ledger with the citations (R-5).

## 5. Gates and the line that means green

Facade suite (`make py-test-facade`); `bash scripts/check_map_md.sh
--base origin/main`; `ruff check` / `ruff format --check` on touched Python;
DIFF-PROBE (R-1). Pin per convenience: same answers as the SQL-door equivalent.
**PENDING**: the R-3 row list (grows with C-2…C-5).

## 6. Halt rules

- **H-SIG** A convenience needs an engine surface C-2/C-4 did not land: halt; do
  not extend the engine inside this unit.
- **H-COND** A RePark-owned condition has no dated row: halt the close-out; CC-4
  allows no blanket carve-out.

## 7. Hand-back

```json
{"unit":"C-6","tests":"","rows":[],"halt":null}
```

Plus the ledger (R-5).
