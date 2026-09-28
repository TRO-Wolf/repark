# NS-NESTED — nested namespaces, a v1.6.0 card

**Filed:** 2026-09-27 on the owner's ruling C-2 (2026-09-24): cell
`D-NS-NESTED` leaves the v1.5.0 gate and is scheduled for **v1.6.0**.
This card closes when the v1.6.0 nested-namespace unit lands or the owner
declines it.

## Why it is its own release item

RePark catalogs are flat today. Registry row NS-2 in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
records the refusal: `SHOW NAMESPACES IN cat.ns` answers an
`AnalysisException` naming the supported one-part `IN <catalog>` form, and a
three-part `CREATE NAMESPACE` name refuses with the two-part-name text
("expected a two-part `catalog.namespace` name"). The remainder spec
carries the same cell twice: the U5 owner item
([v1-5-0-remainder-spec-2026-09-23.md](v1-5-0-remainder-spec-2026-09-23.md)
U5, `D-NS-NESTED` under `NS-2`: nested namespaces `sc.a.b` plus `SHOW
NAMESPACES IN sc.a`, Spark answering on the hadoop catalog) and the
"Carved out" paragraph (owner ruling C-2, 2026-09-24, decision 3 closed by
it). The carve-out keeps the v1.5.0 gate green; this card is the promised
follow-up. The v1.5.0 gate does not wait on this card.

## Step 0 — the oracle (before any design is ruled)

The scoreboard cell `D-NS-NESTED` (`CREATE NAMESPACE sc.a.b (nested) +
SHOW NAMESPACES IN sc.a`, registry `NS-2`) is defined in
`cells_ddl.py` of the 2026-09-26 scoreboard:

```python
x.sql(f"CREATE NAMESPACE sc.nn_{x.tname}")
x.sql(f"CREATE NAMESPACE sc.nn_{x.tname}.inner")
x.o("rows", x.rows(f"SHOW NAMESPACES IN sc.nn_{x.tname}"))
```

The recorded Spark leg (`out/spark-core.json`) answers `ok` with one
observation: `rows` is `[["nn_t_d_ns_nested.inner"]]` — one row holding
the mangled parent name plus `.inner`. The recorded RePark leg
(`out/repark-core.json`) errors at the nested create with
`AnalysisException`: ``Error during planning: expected a two-part
`catalog.namespace` name, got `sc.nn_t_d_ns_nested.inner` ``.

Already measured: `CREATE NAMESPACE sc.a.b` (two-level nesting) and
`SHOW NAMESPACES IN sc.a` (listing the child of a nested parent).
Still to measure, on the scoreboard's Spark leg before any ruling: a
table created and read in `sc.a.b`; `DROP NAMESPACE` on a nested
namespace with and without `CASCADE`; the refusal text when the nested
namespace is non-empty; and the same four shapes one level deeper
(`sc.a.b.c`). Every design question below is answered from these cells,
not from the documentation.

## Design questions to rule, in order

1. **Session-catalog scope.** Spark itself limits the session catalog to
   a single-part namespace: `SELECT * FROM SC.ns.k` (`cs_probe2.py`,
   key `cat_catalog_upper`, recorded in `p2-spark.json`) answers
   `AnalysisException [REQUIRES_SINGLE_PART_NAMESPACE]`, message
   ``spark_catalog requires a single-part namespace, but got `SC`.`ns` ``
   (SQLSTATE 42K05). Nested
   namespaces are therefore a non-session-catalog feature; the session
   catalog keeps the single-part rule with Spark's class. The probe
   decides it.
2. **Where a four-part relation name resolves.** RePark qualifies
   relation names in two places today (read 2026-09-27, unchanged by
   this card): the Python
   `python/repark/src/repark/spark/session/catalog_resolution.py`
   `resolve_table_name` (one part →
   `currentCatalog.currentDatabase.t`; two parts →
   `currentCatalog.ns.t`; three or more parts → as-is) and the Rust
   `crates/repark-spark/src/use_ddl.rs` `complete_name` (one part →
   `[default_catalog, default_schema, table]`; two parts →
   `[default_catalog, namespace, table]`; three or more → as-is).
   Both pass three-plus parts through untouched, so neither splits a
   `cat.a.b.t` spelling into namespace levels plus table today. The
   card rules which layer owns that split and how `USE` and current
   namespace state carry a multi-level default. The two functions
   decide it.
3. **Namespace DDL arity.** `catalog_ops.rs` resolves only two-part
   `catalog.namespace` names and `describe_show.rs` accepts only the
   one-part `IN <catalog>` form. The card rules the qualifying grammar
   for `CREATE`, `DROP`, `SHOW`, `DESCRIBE` and `ALTER NAMESPACE` with
   multi-level names, keyed off the step-0 cells. The oracle decides
   it.
4. **DROP semantics for nested namespaces.** Plain versus `CASCADE`
   drops of a nested namespace, the non-empty refusal text, and the
   parent-holds-a-child-namespace text (the parity doc's
   `ICE-DROP-NS-1` row already records Spark's `Contains 1 child
   namespace(s).` spelling from the 2026-09-19 live oracle; the card
   re-verifies it on the nested shape). The new cells decide it.
5. **Catalog coverage.** The spec's U5 owner item says Spark answers
   nested namespaces on the hadoop catalog; RePark must measure each
   catalog kind it ships (memory, hadoop, Glue, S3 Tables, REST)
   separately and refuse loud where the fork has no multi-level
   namespace. No catalog is assumed. The per-catalog cells decide it.

## Size and tier

M, 3–5 executor rounds, tier terra (parser/grammar) + opus (name
resolution and catalog semantics). The oracle family is small — one
recorded cell plus four new shapes — but the split of a four-part name
touches every entry point (native DataFrame, ANSI SQL, Spark facade),
and catalog coverage is measured per catalog kind, not once. A fork
lane joins only if a catalog kind needs multi-level namespace support
the fork lacks.

## Rules that bind it

The fork rules, the comment ban on every actor, measure before ruling,
recorded-oracle pins with a live leg, a verification critic before every
product-Rust merge. The v1.5.0 gate does not wait on this card.
