# C-5 — SQL Server: TDS reads and writes behind the pip opt-in · grade-B skeleton · standard tier · 1.6

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why, and what is out

C-5 adds the second backend: TDS via `tiberius`, paged/partitioned SELECT reads,
bulk insert with row fallback, shipped as a pip opt-in (card 1.6; CC-5). Out: new
providers (it reuses C-2…C-4's shapes), conveniences (C-6).

## 1. Rulings already made

- R-1 (engine): the **standard tier** with one verifier per stack and DIFF-PROBE
  (card 1.6).
- R-2 (protocol): TDS via `tiberius`; read is paged/partitioned SELECT, write is
  bulk insert with row fallback (card 1.6). No JVM, no ODBC (card 1.6).
- R-3 (packaging): SQL Server ships through pip as an opt-in (CC-5; owner,
  2026-10-01: "ship it as an option in pip"). The mechanism — compiled into the
  standard wheel with the extra carrying only Python dependencies, or a sibling
  `repark-mssql` wheel — is decided by the C-0 measurement (CC-5): build time,
  wheel size, linking, platforms. **PENDING**: that measurement is not on `main`
  (verified 2026-10-05: no mssql/tiberius commit; only roadmap mentions, and the
  C-0 order scopes the SQL Server container as "a separate measurement"). C-5 does
  not start until the measurement rules the mechanism.
- R-4 (types): `types/mssql.rs`, one table per backend (card 1.6); the R-7 rule of
  C-1 applies unchanged (round-trip pin per mapped type; declared, never
  approximated).
- R-5 (ledger): cites ConnectorX and ADBC per the 1.6 row's standing instruction
  (owner, 2026-09-13).

## 2. Files

`crates/repark-connect/src/{types/mssql.rs,read/mssql.rs,write/{mssql_bulk,row}.rs}`;
the packaging files per the R-3 ruling; `tests/it/` pins; `map.md` lockstep.
**PENDING on C-2/C-3/C-4**: the provider, partitioning and sink shapes this
backend reuses. **PENDING on R-3**: every packaging file.

## 4. Steps

1. Confirm the R-3 measurement rules the mechanism; halt per H-MECH if not.
2. The TDS read path on the C-2/C-3 shapes; the write path on C-4's; one commit
   per slice.
3. Live cells against the SQL Server container (card 1.6: "SQL Server via its
   docker image"); the federated statement gains its × SQL Server leg with an
   `EXPLAIN` boundary pin (card 1.6 pins). Ledger with the citations (R-5).

## 5. Gates and the line that means green

`cargo test -p` on touched crates; live SQL Server cells; DIFF-PROBE (R-1);
`bash scripts/check_map_md.sh --base origin/main`; clippy/fmt clean. Pins: the
mssql type-map round-trip table, bulk vs row parity, the federated × SQL Server
`EXPLAIN` pin (card 1.6 pins). **PENDING**: packaging gates per the R-3 ruling.

## 6. Halt rules

- **H-MECH** The C-0 CC-5 measurement is missing or rules nothing: halt; do not
  pick a mechanism.
- **H-SHAPE** A reused C-2/C-3/C-4 shape fits Postgres only: halt with the gap;
  do not fork the shape silently.

## 7. Hand-back

```json
{"unit":"C-5","tests":"","live":"","mechanism":"","halt":null}
```

Plus the ledger (R-5).
