# C-1 — the connect skeleton: crate, identity move, settings, Postgres type map · grade B · Claude Opus 5.5 (`claude-opus-5-5`) · 1.6

## 0. Why, and what is out

Card 1.6 of [the design plan](../roadmap/epic-term/roadmap-design-plan-2026-08-29.md)
creates `repark-connect` through units C-0…C-6. C-0 (landed `82f1483c`) delivered the
disposable Postgres and the live-cell rules. C-1 creates the crate on its pre-declared
footprint (CL-8), moves the minimal source identity to `repark-common` (CC-2), lands
`settings.rs` with the reserved auth-method field (ES-1) and the Postgres type map, and
proves the pre-declaration path end to end: crate, `map.md`, workspace member, manifest
row flipped to `delivered`, DAG gate green.

**Out:** any provider, any read path, any pool (C-2 and later, per the card). The
`core → connect` edge (no caller until C-2 mounts the providers). Driver dependencies
(`tokio-postgres`, `sqlx`, `tiberius`). Endpoint key interpretation (C-2, per CC-3).
Generation assignment and re-pointing detection (1.7 capture, per CC-2/CC-9). Both stubs
stay: `refuse_source_ddl` and the `read_postgres` refusal retire in C-2, per the card.

## 1. Rulings already made

- R-1 (engine): card 1.6 lists C-1 as design-heavy (an Opus executor with a design
  sketch). The owner's 2026-10-05 go first put C-1 on Muse with every decision
  pre-made in this order. The owner's ruling later the same day ("assign opus for
  executor and review for the cdc work … anything related to 1.6 and the cdc or
  Iceberg … use Opus 5.5 for everything", for 12 hours) puts it on **Claude Opus 5.5**
  (`claude-opus-5-5`), with a scoped Opus 5.5 verifier.
- R-2 (home and role): `crates/repark-connect`, tier 1, role `table service`, peer of
  `repark-iceberg` (card 1.6; [the layout](../roadmap/epic-term/crate-layout-1-8-2026-10-01.md)
  §1; CL-8). Pre-declared in `scripts/check_crate_dag.py` (`TIERS`, `ROLES`) and as a
  `planned` row in `repo-manifest.toml` (CL-8).
- R-3 (edges): C-1 wires exactly one pre-declared edge, `repark-connect → repark-common`
  (`normal`). The `repark-core → repark-connect` edge is **not** added: nothing in
  §2 calls it (mounting is C-2's "providers mounted", card 1.6), and the gate bans
  edges but never requires one (`scripts/check_crate_dag.py`, header comment). C-2 adds
  it with the mount.
- R-4 (CC-2 identity move): the split is
  [ruled](../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md) CC-2 —
  `repark-common` owns the minimal source identity (configured name, kind, a source
  generation); `repark-connect` owns settings and conversions; `repark-core` keeps
  loading, precedence, registration and the user-facing handles; lineage and offsets
  wait for `repark-cdc`. Found by grep on `main`: `SourceKind` (variants `Postgres`,
  `SqlServer`, `Trino`) and `SourceSpec` (fields `name`, `kind`, `profile`,
  `auto_register`, `props`) are `pub(crate)` in
  `crates/repark-core/src/config_file/sources.rs`. `SourceKind` moves to
  `repark-common` unchanged, all three variants: CFG-2 parses `trino` today and the
  card moves Trino's connector to 1.10 without removing the spelling, so dropping the
  variant would be a behavior change outside this unit. The call sites that re-import
  (nothing else moves): `named_sources.rs` (`refuse_source_ddl`,
  `RefusingSourceCatalogProvider::new`, `SourceRow::from_spec`,
  `NamedSource::from_spec`, `register_configured_sources`), `catalog_state.rs`
  (`insert_database_source`, `database_source`), `session.rs` (the `source_specs`
  fields), `config_file/wiring.rs` (the `profile_sources` assembly), and
  `config_file/tests`. Loader behavior is unchanged; every CFG-2 test stays green.
  The generation field's representation is R-12.
- R-5 (settings): `settings.rs` holds the connection settings with the reserved
  `auth_method` field (card 1.6; ES-1). The key spelling follows the only multi-word
  database key on `main`, `auto_register`
  (`crates/repark-core/src/config_file/sources.rs`). Per CC-3 ("no key is defined
  until its unit lands; the owner crate interprets and validates"), C-1 interprets
  **only** `auth_method` and carries every other prop through untouched; endpoint
  keys land with C-2, their first consumer. Absent `auth_method` means password, the
  1.6 value (ES-1). The value spellings are R-13. Any
  other value is an invalid specification (CC-4).
- R-6 (auth refusals): IAM token (RDS) and Kerberos / Active Directory are declared
  refusals with one dated registry row each (ES-1, CC-4). Rows go in
  `docs/spark-sql-iceberg-parity.md` beside the `SES-DECL` family, following that
  row shape (repark behavior, Spark behavior, pin, dated rationale); the executor
  records the chosen row ids in the ledger.
- R-7 (Postgres type map): `types/postgres.rs` holds the Postgres ↔ Arrow map as a
  table, one row per Postgres type, with a round-trip pin per mapped type (card 1.6
  pins). A type with no Arrow mapping is **declared**, never approximated (card 1.6
  hand-back rule); the same holds for a type with two plausible mappings. Declared
  types get dated registry rows like R-6. There is no minimum mapped count: C-2
  extends the table, so an undecided row is a declared row, not a halt.
- R-8 (no driver deps): C-1 adds no `tokio-postgres`, `sqlx` or `tiberius` (card 1.6
  names them for the read/write paths, which are C-2 and later). Deps are
  `repark-common`, `arrow`, `thiserror` (`thiserror` for libs is house style).
- R-9 (tests live in `tests/it/`): every crate carries one integration binary,
  `tests/it/main.rs` (the layout §1). The type-map pins and the settings pins live
  there; the CC-2 move is pinned by the untouched CFG-2 suite staying green plus one
  identity round-trip test (name, kind, generation survive the move).
- R-10 (ledger cites the bars): the 1.6 release row's standing instruction (owner,
  2026-09-13) requires every unit's ledger to cite which of ConnectorX and Arrow
  ADBC shaped its design. For C-1 that is ADBC for the type map's Arrow contract;
  ConnectorX shapes the partitioned reads and is recorded as not-yet (C-2/C-3).
- R-11 (crate paperwork): edition, version, license and lints ride the workspace
  (`*.workspace = true`, the `repark-iceberg` precedent); `unsafe_code = "forbid"`
  (house rule; only `repark-python` allows it). No code comments, doc comments
  included (owner ruling); new `map.md` files carry the reasons.

## 2. Files and the design sketch

| action | path |
|---|---|
| new | `crates/repark-connect/Cargo.toml` — package (workspace inheritance, R-11), one internal dep (`repark-common`), `arrow`, `thiserror`; no driver deps (R-8) |
| new | `crates/repark-connect/src/lib.rs` — `mod settings; mod types;` plus re-exports |
| new | `crates/repark-connect/src/settings.rs` — the sketch below |
| new | `crates/repark-connect/src/types.rs` — `pub mod postgres;` |
| new | `crates/repark-connect/src/types/postgres.rs` — the sketch below |
| new | `crates/repark-connect/tests/it/main.rs` — the pins (R-9) |
| new | `crates/repark-connect/map.md` — the crate map |
| edited | `crates/repark-common/src/lib.rs` (or one new module file it declares) — receives `SourceKind` and the identity |
| edited | the six core call sites of R-4 — re-import only, no behavior change |
| edited | root `Cargo.toml` — the workspace member |
| edited | `repo-manifest.toml` — the `repark-connect` row flips `planned` → `delivered` |
| edited | `crates/map.md` — one crate row |
| edited | `docs/spark-sql-iceberg-parity.md` — the R-6 rows and the R-7 declared-type rows |

Design sketch, naming types and signatures:

- `repark-common`: `SourceKind` moved unchanged (`Postgres`, `SqlServer`, `Trino`,
  with `spelling()`); new `SourceIdentity { name: String, kind: SourceKind,
  generation: u64 }`. `SourceSpec` in core keeps `profile`, `auto_register`,
  `props` and carries the identity; `key_path()` renders from it unchanged.
- `settings.rs`: `ConnectionSettings` built from one source's props: the parsed
  `auth_method` plus the untouched prop map. `AuthMethod`: password accepted
  (explicit or absent); `iam_token` and `kerberos` refused with the R-6 dated rows (R-13);
  anything else an invalid-specification error (CC-4).
- `types/postgres.rs`: one table, one row per Postgres type: the Postgres name, the
  Arrow `DataType` or `DECLARED` with its registry row, and the pin name. One
  round-trip pin per mapped row in `tests/it/`: Arrow value → Postgres wire form →
  Arrow value, equal.

## 4. Steps

1. Add the workspace member, the crate files and the `map.md` files from §2;
   flip the manifest row to `delivered`.
2. Land the R-4 move; `cargo test -p repark-core -p repark-common` green with no
   behavior change.
3. Land `settings.rs` with the R-5/R-6 pins; land `types/postgres.rs` with the R-7
   pins; `cargo test -p repark-connect` green.
4. Write the registry rows (R-6, R-7); record the row ids and the ConnectorX/ADBC
   citations (R-10) in `task/ledgers/staging/c-1-ledger.md`, linked from that
   directory's `map.md` in the same commit.
5. Run the §5 gates. Commit:
   `feat(c-1): the repark-connect skeleton, the CC-2 identity move, settings and the Postgres type map`
   with the `Authored-By:` trailer.

## 5. Gates and the line that means green

| command | green |
|---|---|
| `cargo test -p repark-connect -p repark-common -p repark-core` | all pass, zero failures |
| `./scripts/check_crate_dag.sh` | clean (the `connect → common` edge declared; no undeclared edge) |
| `./scripts/check_manifest.sh` | clean (the row is `delivered`, the directory exists, the `map.md` exists) |
| `cargo clippy -p repark-connect -p repark-common -- -D warnings` | no output, exit 0 |
| `cargo fmt --check -p repark-connect -p repark-common` | no output, exit 0 |
| `python3 scripts/check_rust_file_size.py` | clean (new files under the default ceiling; ceilings never move up) |
| `bash scripts/check_map_md.sh --base origin/main` | clean |
| `python3 scripts/check_docs_links.py` | clean (the registry rows link correctly) |
| `cargo clippy --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` | exit 0 |
| `make rust-panic-ban` | exit 0 |
| `./scripts/check_lib_rs.sh` and `python3 scripts/sync_map_md.py --check` | clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py <clone> origin/main HEAD` | `hits=0` |

## 6. Halt rules

- ~~H-GEN~~ **Ruled R-12 (orchestrator, 2026-10-05; reversible before 1.6 ships):** the
  identity's generation field is a `u64`, where `0` means unassigned. 1.7's capture
  assigns it on first use (CC-2/CC-9). Equality is the only operation C-1 needs.
- ~~H-AUTH~~ **Ruled R-13 (orchestrator, 2026-10-05; reversible before 1.6 ships):**
  `auth_method` values are `password` (the default when absent), `iam_token` and
  `kerberos`. That is snake case, following `auto_register`, the only multi-word
  database key. `iam_token` and `kerberos` are the ES-1 declared refusals with
  their dated rows. Any other value is an invalid specification (CC-4).
- **H-GATE** A gate needs a change outside §2's file list: hand back the gate's
  output; do not edit the gate and do not widen the file list.
- **H-AMBIG** Any other decision this order does not pre-make: halt on the first
  one with the premise, the question and the recommended option. Do not invent.

## 7. Hand-back

```json
{"unit":"C-1","tests":"","dag":"","manifest":"","row_ids":[],"halt":null}
```

Plus the ledger (`task/ledgers/staging/c-1-ledger.md`) with the ConnectorX/ADBC
citations (R-10) and, on a halt, the premise, question and recommendation.
