# C-4 — writes: COPY FROM STDIN default, row fallback, both-door routing · grade-B skeleton · standard tier · 1.6

## 0. Why, and what is out

C-4 is the Postgres write path: `COPY … FROM STDIN (FORMAT BINARY)` by default
with a row `INSERT` fallback per the `bulk | row` flag, and
`INSERT INTO <source>.<schema>.<table> SELECT …` routed to the sink in both doors
(card 1.6). Out: SQL Server writes (C-5), conveniences (C-6).

## 1. Rulings already made

- R-1 (engine): the **standard tier** with one verifier per stack and DIFF-PROBE
  (card 1.6).
- R-2 (paths): bulk default (`COPY … FROM STDIN (FORMAT BINARY)`), row fallback
  (card 1.6; the 1.6 release row: "bulk path default with a per-call `bulk` / `row`
  flag"). Row mode is the fallback for types bulk cannot carry (the 1.10 row states
  the same rule).
- R-3 (routing): `INSERT INTO <source>.<schema>.<table> SELECT …` routes to the
  sink in `repark-sql` and `repark-spark` (card 1.6).
- R-4 (flag surface): **PENDING** — the per-call flag's surface (SQL option,
  Python argument, or both) depends on C-2's provider shape and C-6's convenience
  signatures; no source rules it.
- R-5 (ledger): cites ConnectorX and ADBC per the 1.6 row's standing instruction
  (owner, 2026-09-13); ADBC's bulk-ingest contract is the bar here.

## 2. Files

`crates/repark-connect/src/write/{postgres_copy,row}.rs`; routing in
`repark-sql` and `repark-spark`; `tests/it/` pins; `map.md` lockstep.
**PENDING on C-2**: the provider surface the sink plugs into.
**PENDING on R-4**: the flag's surface files.

## 4. Steps

1. The bulk path with bulk-vs-row parity pins; one commit per slice.
2. The both-door routing (R-3); the flag surface once R-4 is ruled.
3. Live cells on the C-0 container; ledger with the citations (R-5).

## 5. Gates and the line that means green

`cargo test -p` on touched crates; live cells on the C-0 container; DIFF-PROBE
(R-1); `bash scripts/check_map_md.sh --base origin/main`; clippy/fmt clean. Pins:
bulk vs row parity, one test row per door for the routing (the two-doors rule:
new SQL surface lands with both spellings). **PENDING**: flag-surface pins (R-4).

## 6. Halt rules

- **H-FLAG** R-4 still open when the flag slice starts: halt; do not invent the
  surface.
- **H-PARITY** A bulk/row divergence: halt with the diverging rows; the paths must
  agree.

## 7. Hand-back

```json
{"unit":"C-4","tests":"","live":"","halt":null}
```

Plus the ledger (R-5).
