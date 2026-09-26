# F-PARTSPEC-REDUNDANT-1 — the fork's partition-spec update path matches Java's redundancy rules (fork card)

**Filed:** 2026-09-26 by WO PARTNAME-1 (RePark lane `xp-partname`, branch
`feat/partname-1`). **Target:** the owned `iceberg-rust` fork, then an RP-N repin.
**Severity:** P2 — Spark accepts the shapes and the fork refuses them, so RePark
refuses `ALTER TABLE … ADD PARTITION FIELD` where Spark commits. **No RePark code
in PARTNAME-1** (ledger residue R-1).

## The divergence it closes

Measured 2026-09-26 against Spark 4.1.2 + Iceberg 1.11.0 (probes
`partname-spark-2026-09-26.json`, `partred-spark-2026-09-26.json`, RePark main
`partname-repark-main-2026-09-26.json`; RePark re-measured on main `5a1c8ebd`).

On the UPDATE door (`ALTER TABLE … ADD PARTITION FIELD`, Java
`BaseUpdatePartitionSpec`), Spark accepts every distinct pair of transforms on one
source — `years`+`hours`, `years`+`months`, `days`+`hours`, `days`+`months`,
`months`+`days`, `bucket(8, id)`+`id`, `id`+`bucket(8, id)`,
`bucket(8, id)`+`bucket(16, id)`, `truncate(2, s)`+`truncate(4, s)` — and refuses
only the exact duplicate:

- Spark: `IllegalArgumentException`:
  `Cannot add duplicate partition field null=day(ref(name="ts")), conflicts with
  1000: ts_day: day(3)`

The fork refuses every distinct time-transform pair on one source as redundant:

- Fork today: `DataInvalid => Cannot add redundant partition with source id `3`
  and transform `time`. A partition with the same source id and transform already
  exists with name `ts_day`` (respectively `ts_year`, `ts_month` for the other
  pairs)
- Fork today on the exact duplicate: `DataInvalid => Cannot add duplicate
  partition field, conflicts with existing field: ts_day`

The non-time pairs already commit on the fork like Spark and must keep doing so.

On the CREATE door both engines refuse redundant time transforms, with different
texts:

- Spark: `IllegalArgumentException`: `Cannot add redundant partition: 1000:
  ts_year: year(0) conflicts with 1001: ts_month: month(0)` (and `… ts_day: day(0)
  conflicts with 1001: ts_month: month(0)`)
- Fork today: `DataInvalid => Cannot add redundant partition with source id `1`
  and transform `time`. A partition with the same source id and transform already
  exists with name `ts_year``

Spark refuses `(bucket(4, id), bucket(8, id))` on CREATE with `AnalysisException`:
`Found duplicate column(s) in the partitioning: id`; RePark's measured answer is
`PySparkException`: `DataInvalid => Cannot use partition name more than once:
id_bucket`.

## Why it is not a RePark fix

The table-format engine lives in the fork by contract
([AGENTS.md](../../../AGENTS.md)): the partition-spec builder and the update path
are fork code, and RePark consumes them through the catalog provider. Loosening
the rule RePark-side would simulate Iceberg semantics outside the engine that owns
them. The names on both doors are already Spark's on RePark (WO PARTNAME-1 pins);
only the redundancy rules and their texts diverge.

## The design

1. The fork's update path allows distinct transforms on one source and refuses
   only exact duplicates, like Java's `BaseUpdatePartitionSpec`: every pair above
   commits, and the exact duplicate refuses with Java's text (`Cannot add
   duplicate partition field null=day(ref(name="ts")), conflicts with 1000:
   ts_day: day(3)`).
2. The CREATE-door texts match Java: the redundant-time refusal carries
   `Cannot add redundant partition: 1000: ts_year: year(0) conflicts with 1001:
   ts_month: month(0)` (and the `ts_day` form), and `(bucket(4, id), bucket(8,
   id))` refuses with Spark's `Found duplicate column(s) in the partitioning:
   id`.

## Oracle cells to re-measure on the day

The nine UPDATE-door pairs above (commit, with the PARTNAME-1 names), the exact
duplicate (refusal text), the two CREATE-door redundant-time pairs (refusal
texts), `(bucket(4, id), bucket(8, id))` on CREATE (refusal text), and the
PARTNAME-1 pin suite (`python/repark/tests/test_partname_1.py`) to confirm no
name moves.

## Clauses (draft)

- **C-001** The update path commits every distinct transform pair on one source.
- **C-002** The exact duplicate refuses with Java's text.
- **C-003** The CREATE-door redundant-time and bucket+bucket texts match
  Java/Spark.
- **C-004** The PARTNAME-1 pins stay green on the repin; residue R-1 retires.

## Pointers

- Up: [map.md](map.md) · Ledger:
  [../../ledgers/staging/partname-1-ledger.md](../../ledgers/staging/partname-1-ledger.md)
  (residue R-1) · Pins:
  [../../../python/repark/tests/test_partname_1.py](../../../python/repark/tests/test_partname_1.py)
