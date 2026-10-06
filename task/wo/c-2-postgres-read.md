# C-2 — the Postgres read path: COPY BINARY decode, pushdown, mounted providers · grade-B skeleton · Opus design sketch first · 1.6

**Design sketch:** [c-2-design.md](c-2-design.md) (2026-10-06, Claude Opus 5.5) closes the PENDING items of
R-5, §2, §4 step 1 and §5, and splits the build into slices C-2a…C-2d.

## 0. Why, and what is out

C-2 is the first live connector: Postgres sources declared under CFG-2 become
queryable. `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` decode feeds Arrow,
`TableProvider::scan` pushes the predicate subset Postgres takes, `EXPLAIN` shows
the per-source boundary with residuals, the providers mount, and both C-1 stubs
retire (card 1.6). Out: partitioned reads (C-3), writes (C-4), SQL Server (C-5),
the Python conveniences (C-6).

## 1. Rulings already made

- R-1 (engine): an **Opus design sketch first**, per card 1.6 ("C-1 and C-2 are
  design-heavy (an Opus executor with a design sketch)"). The sketch lands before
  the build slices open.
- R-2 (read protocol): `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` decode → Arrow
  (card 1.6). Driver: `tokio-postgres` (`sqlx` only if needed, card 1.6).
- R-3 (pushdown): `supports_filters_pushdown → Exact` for the predicate subset
  Postgres takes (card 1.6). `EXPLAIN` prints what the statement pushed per source,
  residual filters included (CC-1; card 1.6). Pushdown that would change semantics
  (collation, NULL ordering) is declared, never approximated (card 1.6 hand-back
  rule).
- R-4 (mounting): `catalog_state.rs` mounts the providers through
  `SessionExtension::register` (card 1.6; CC-1). `refuse_source_ddl` retires on use
  (card 1.6); the deferred `read_postgres` refusal in
  `crates/repark-python/src/session.rs` retires (card 1.6, "both stubs retired").
- R-5 (settings): C-2 is the first consumer of endpoint keys, so it defines them
  (CC-3: no key defined until its unit lands; C-1 carries props through). **PENDING
  on C-1**: the key list must fit C-1's `settings.rs` shape and the H-AUTH ruling.
- R-6 (pool): query pools arrive here; replication connections never share them
  (card 1.6; the 1.6 release row: separate query pools from capture connections
  from day one).
- R-7 (ledger): cites ConnectorX and ADBC per the 1.6 row's standing instruction
  (owner, 2026-09-13); C-2 is where ConnectorX's partitioned-read design first
  applies and ADBC's cursor contract is met or declared.

## 2. Files

`crates/repark-connect/src/provider/{catalog,schema,table}.rs`,
`read/postgres.rs`, `pushdown.rs`, `pool.rs`; the `repark-core → repark-connect`
edge (pre-declared, added here with the mount, R-4); `catalog_state.rs` and
`named_sources.rs` (mount, stub retirement); `crates/repark-python/src/session.rs`
(`read_postgres` live); `tests/it/` pins; `map.md` lockstep. **PENDING on C-1**:
exact settings edits and any type-map extension C-1's table forces.

## 4. Steps

1. The Opus design sketch: the provider trait shape, the pushdown subset table,
   the pool shape, the endpoint key list. **PENDING on C-1** where it touches
   settings or identity.
2. Build slices per the sketch, one commit each; then the stub retirements.
3. Live cells against the C-0 container (`make pg-up`); the ledger with the
   ConnectorX/ADBC citations.

## 5. Gates and the line that means green

`cargo test -p` on touched crates; `./scripts/check_crate_dag.sh` (the
`core → connect` edge now real); `./scripts/check_manifest.sh`;
`bash scripts/check_map_md.sh --base origin/main`; clippy/fmt clean. Pins: the
pushdown pin per predicate class including the residual-filter pin, the federated
statement (Iceberg × Postgres join) with its `EXPLAIN` boundary pin (card 1.6
pins). **PENDING on the sketch**: the predicate-subset table the pins enumerate.

## 6. Halt rules

- **H-SKETCH** The sketch is missing or leaves a §2 file undesigned: halt; do not
  build ahead of it.
- **H-C1** C-1's H-GEN or H-AUTH is still open: halt; do not route around it.
- **H-SEM** A pushdown changes semantics (collation, NULL ordering): declare, never
  approximate (card 1.6); halt only if no declared row fits.

## 7. Hand-back

```json
{"unit":"C-2","tests":"","live":"","dag":"","halt":null}
```

Plus the ledger with the ConnectorX/ADBC citations (R-7).
