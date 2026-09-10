# Review-fix slate — 2026-09-10: the REVIEW-1 findings ruled and ordered, Ballista Milestone 2 opened

The disposition of [review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md) (24 Grok
critic rounds, 51 findings, 15 fix cards, 8 owner questions) and of the open questions in
[docs/design/distributed-m1.md](../../../docs/design/distributed-m1.md). Every ruling below was
taken under the owner's standing instruction of 2026-09-09 ("proceed with your recommendation on
the rulings"); the cards run on the cheap tier under the same loop, preamble and gates as
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) §1.

## 0. Rulings (2026-09-10)

| # | Question | Ruling | Binds |
|---|---|---|---|
| RF-1 | Q-8: a discovered file with only `[prod]` and `REPARK_ENV` unset applies nothing. | **Keep** — `None` selects `[default]` alone (CFG-1 C-004); no warning. | — |
| RF-2 | Q-16: does `DESCRIBE` complete short names? | **Yes.** One- and two-part names complete from the session's current catalog and namespace exactly as `SELECT` does (the reTest bed measured `DESCRIBE TABLE desc_demo` raising a parse error on 2026-09-10 while `SELECT * FROM desc_demo` works). REVIEW-FIX-5 gains D-3 with a pin for both spellings; the `R-DESCRIBE-TWO-PART` row leaves the registry when it lands. | REVIEW-FIX-5 |
| RF-3 | Q-20: CWD-discovered `repark.toml` + `${VAR}` + Glue-at-build reach AWS on ambient credentials. | **Warn, not refuse.** A catalog block whose `type` is `glue`, `s3tables` or `rest` in a file found by CWD or home discovery (not named by `REPARK_CONFIG` or `configFile`) emits one warning at session build naming the file path and the catalog; a file the user named is trusted. Same shape as R-19. | REVIEW-FIX-7 D-3 |
| RF-4 | Q-25: R-16 says "nine `datafusion.*` keys"; the probe's nine are four DataFusion keys, three `repark.*` session keys and two Iceberg table properties. | **Narrow.** CONF-UNREAD-1 covers `datafusion.execution.parquet.enable_page_index`, `datafusion.execution.parquet.bloom_filter_on_read`, `datafusion.execution.coalesce_batches`, `datafusion.execution.parquet.write_batch_size`. The three `repark.*` session keys and the two Iceberg properties are documented as accepted-at-session, applied-at-table (or refused loud) by REVIEW-FIX-8. | CONF-UNREAD-1, REVIEW-FIX-8 |
| RF-5 | Q-29: is a duckdb D-5 follow-up owed? | **Yes**, and it rides DISPLAY-LAZY-1: once a lazy `repr` is schema-only, the duckdb branch's count-first path only runs for eager frames, and step 1 of that card re-pins `max_rows_per_export` for the duckdb section the way R-11 did for polars. | DISPLAY-LAZY-1 |
| RF-6 | Q-30: which redaction predicate does `DESCRIBE TABLE EXTENDED` owe? | **`prop_key_is_secret`**, the one `DESCRIBE NAMESPACE EXTENDED` uses; the Spark delta (Spark prints `s3.access-key-id`) is documented as a deliberate divergence. Printing a credential is the worse of the two. | REVIEW-FIX-5 D-4 |
| RF-7 | Q-34: LEDGER-READING-1 D-3's prefix check was never implemented. | **Amend the card, not the gate.** The card author is the orchestrator; D-3's text is rewritten to what shipped (the `**Path:** READING` header exemption) and REVIEW-FIX-10 makes that exemption a field, not a substring. | REVIEW-FIX-10 |
| RF-8 | Q-39: `check-docs-links` is in `make ci` but not `ci.yml`'s guards job. | **Dual-wire.** An orchestrator step (O), since `.github/` is closed to workers. | §2 O-1 |
| RF-9 | Milestone 2's first question: can a RePark plan node cross to an executor? | **Take `datafusion-proto`.** `repark-distributed` gains `datafusion-proto = "=54.1.0"` behind the `cluster` feature by orchestrator seed (G-5); the codec wrapper becomes a real delegating `PhysicalExtensionCodec`; M1-B C-004 and the M1-D `IcebergTableScan` residue close on it. The parquet-file-group rewrite stays as the M1 read path and as the fallback for nodes without a codec. `arrow_flight` is **not** added: M1-C C-002 (deterministic retry) stays OPEN until Ballista exposes a fail-once hook. | BALLISTA-M2-A |
| RF-10 | Ballista Milestone 1 ledgers in `staging/`. | STATUS.md carries the Milestone 1 sentence (this PR); one M round moves the four ledgers to `completed/` with their attestation blocks unchanged. | §2 M-0 |

## 1. Order

Security and data-loss first, then gates that fail open, then the rest. Display cards wait for
DISPLAY-LAZY-1 so they are not pinned twice. Every card is in
[review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md) §3; its Home, pins and
tier are binding as written there plus the rulings above.

| # | Card | Tier | Rounds | Note |
|---|---|---|---|---|
| 1 | REVIEW-FIX-4 — eager frame checkpoint paths (`clearCache()` data loss, `SELECT * FROM None`) | M | 1 | first |
| 2 | REVIEW-FIX-7 — no TOML injection, no secret echo, + RF-3 warning (D-3) | M + I | 1–2 | Muse for the Rust half (R-15) |
| 3 | REVIEW-FIX-5 — DESCRIBE metadata-name intercept, + RF-2 short names (D-3), + RF-6 redaction (D-4) | I | 1–2 | Muse |
| 4 | REVIEW-FIX-10 — READING exemption is a field | M | 1 | gate fail-open |
| 5 | REVIEW-FIX-12 — docs-links gate measures what it claims (same-file anchors) | M | 1 | gate fail-open |
| 6 | REVIEW-FIX-1, REVIEW-FIX-2 — CFG-1 mirror agrees with the loader; pins stop reading `HOME` | M | 2 | |
| 7 | DISPLAY-LAZY-1 (slate 1, R-22) | M | 2 | before any display fix |
| 8 | REVIEW-FIX-3, REVIEW-FIX-9, REVIEW-FIX-14 — display boundaries and promises | M | 3 | after 7 |
| 9 | REVIEW-FIX-6, REVIEW-FIX-11 — `explain()` both-set shape, unpinned rows | M | 2 | |
| 10 | REVIEW-FIX-8 — the probe is re-runnable, its table true (RF-4) | M | 1 | with CONF-UNREAD-1 |
| 11 | REVIEW-FIX-13, REVIEW-FIX-15 — the audit trued up; pins cite clauses | M (+ I for FIX-13 D-3) | 2 | |
| 12 | BALLISTA-M2-A — the delegating codec (RF-9) | O seed + Grok/Muse I | 2 | opens Milestone 2 |

## 2. Orchestrator steps (O)

- **M-0** Ledger departure for BALLISTA-M1-A…D (one M round; the STATUS sentence is already on `main`).
- **O-1** Dual-wire `check-docs-links` into `ci.yml`'s guards job (RF-8); `.github/` is the orchestrator's.
- **O-2** Seed commit for BALLISTA-M2-A: `datafusion-proto = "=54.1.0"` under the `cluster` feature, the dependency-policy row, `cargo build -p repark-distributed --features cluster` green before the first worker round.

## 3. Card BALLISTA-M2-A — a RePark plan node crosses to an executor (RF-9)

**Why.** Every OPEN or narrowed clause in Milestone 1 traces to one wall: without
`PhysicalExtensionCodec` Ballista cannot encode `IcebergTableScan` or any later RePark
write/commit node. Milestone 2 cannot start on rewrites alone.

**Home.** `crates/repark-distributed/` (the codec wrapper, the session-provider seat), its
`map.md`, `docs/design/distributed-m1.md` (open questions 1 and 4 close), the M1-B ledger's C-004
row (re-proven, not rewritten), a new `distributed-m2.md` only if the design changes.

**Decisions.** D-1 the wrapper implements `PhysicalExtensionCodec` and delegates to Ballista's
default codec for every node it does not own. D-2 `IcebergTableScan` round-trips through the
codec with a pin over the five shuffle nodes plus the scan (the M1-B C-004 pin, un-parked). D-3 the
parquet-file-group rewrite stays and is pinned as the path for a node with no codec entry. D-4
no `arrow_flight`; C-002 stays OPEN with its residue.

**Steps.** 0 (O-2 seed); 1 (I): red-first codec pins, the delegating impl, `IcebergTableScan`
travelling; 2 (M): design doc, crate map, ledger.

## Pointers

- Up: [map.md](map.md) · The sweep: [review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md) · Run B's report: [overnight-report-2026-09-10-b.md](overnight-report-2026-09-10-b.md)
- Slate 1: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) · Slate 2: [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) · Procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
