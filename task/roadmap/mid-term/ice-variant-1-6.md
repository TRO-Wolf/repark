# ICE-VARIANT — Iceberg `variant` read and write, a v1.6.0 card

**Filed:** 2026-09-27 by the orchestrating session on the owner's ruling C-4
("Carve it out for now"): the `variant` type leaves the v1.5.0 gate (cell
`TY-VARIANT-V3`, verdict REFUSED-REGISTERED under `V3-VARIANT-SHRED-1`) and is
scheduled for **v1.6.0**. The cell's Spark leg is recorded; the first step below
re-reads it, not the documentation.

## Why it is its own release item

Spark 4.1.2 + Iceberg 1.11 wrote the cell's column as SHREDDED Parquet: a
`typed_value` struct with int / string / decimal(38,18) / array / object /
variant branches, `metadata` and `value` null on every row, one row group, an
empty `typed_value` variant fallback branch. The fork (TRO-Wolf/iceberg-rust)
maps Iceberg variant to Arrow `struct<metadata: Binary, value: Binary>` and
refuses both the Parquet write and any scan projecting the column (fork issue
R88 open); it has its own binary-variant module underneath. The v1.6.0 work is
INTEGRATION, not a codec: the fork already links the upstream arrow-rs
`parquet-variant`, `parquet-variant-json` and `parquet-variant-compute` crates
(58.4.0 in the lock file; upstream current 60.0.0), which provide the shredding
state, `shred_variant` / `unshred_variant`, `variant_get`, `cast_to_variant`,
and json↔variant conversion. Neither door can reach a variant value until the
fork gap closes, so the whole item moves as one card.

## Step 0 — the oracle (before any design is ruled)

The recorded shapes dump `variant_cell_shapes.txt` on the scoreboard: Spark's
shredding layout for the recorded shapes, checked against the Iceberg Java
`VariantShreddingWriter` default. Every design question below is answered from
that dump, not from the documentation. The card closes by repinning
`TY-VARIANT-V3` to the recorded Spark rows.

## Scope, in order

1. **Fork: schema mapping.** Map the Iceberg variant type to `VariantArray`.
2. **Fork: unshred on scan.** Read shredded Parquet into variant values.
3. **Fork: shred on write.** Write variant values with a schema matching Spark's
   layout for the recorded shapes; the array branch is the uncertain part.
4. **RePark: CREATE variant on the three doors** (native DataFrame, ANSI SQL,
   Spark facade).
5. **RePark: the four SQL functions** `parse_json`, `variant_get`,
   `try_variant_get`, `to_json`.
6. **Repin `TY-VARIANT-V3`** to the recorded Spark rows.

Read-only shredded scan plus the four functions would satisfy the scoreboard
rows; the card requires read AND write because the cell is a type-parity cell
and the writer refusal is the other half of R88.

## Out of scope

Partial updates, `variant_explode`, predicate pushdown into `typed_value`, and
every scalar kind beyond null / boolean / int32 / int64 / decimal(38,18) /
string / array / object.

## Size and tracking

4–6 executor rounds. Fork issue R88 stays open until this card lands.

## Rules that bind it

The fork rules, the comment ban on every actor, measure before ruling,
recorded-oracle pins with a live leg, a verification critic before every
product-Rust merge. The v1.5.0 gate does not wait on this card.
