# Unit ledger — X-4 / G17: catalog wrapper explicit forwards

**Unit:** X-4 (H-2 gap G17) · **Date:** 2026-08-11 · **Lane:** X-4 ·
**Branch:** `grok/x4-g17-catalog-forwards` · **Executor:** Grok (grok-4.5) ·
**Freeze base:** `9acb566`

Charter: `planning/grok/BRIEF-x4-g17-catalog-forwards.md` (conductor overnight #3).

---

## 1. What landed

| Artifact | Role |
|---|---|
| [`crates/repark-iceberg/src/catalog/provider.rs`](../../../../crates/repark-iceberg/src/catalog/provider.rs) | `NamespaceScopedCatalog`: explicit forwards for 13 defaulted methods + 3 stated omissions; HIGH `publish_replace_table` forwards |
| [`crates/repark-iceberg/src/catalog/namespace_scoped_tests.rs`](../../../../crates/repark-iceberg/src/catalog/tests/namespace_scoped.rs) | 4 file-backed wrapper pins (spy/memory) |
| [`crates/repark-iceberg/src/catalog/mod.rs`](../../../../crates/repark-iceberg/src/catalog/mod.rs) | wires `#[cfg(test)] mod namespace_scoped_tests` |
| map lockstep | crate-root + `src/catalog/map.md` Known-limitations / Contents |
| this ledger | linked from [`task/map.md`](../../../map.md) |

**Out of scope (honored):** fork-side changes; registry file; locks; catalog behavior beyond forwarding.

---

## 2. §0 — Roster (fork pin `b009ac1` / `Cargo.toml` `[patch.crates-io]`)

Fork: `https://github.com/TRO-Wolf/iceberg-rust` rev
`b009ac158f7584a956fa9292c0e9675a411ecf0d`.

Trait surface at pin: **30 methods** = **14 required** + **16 defaulted**.
Slate's "16 defaulted" count **re-verified: 16**.

Impl under audit: `NamespaceScopedCatalog` in
`crates/repark-iceberg/src/catalog/provider.rs` (struct ~line 390; `impl Catalog` follows).

### 2.1 Required methods (14) — all explicit in the impl

| Method | Disposition |
|---|---|
| `list_namespaces` | **explicit** — filtered to `only` (the wrapper's purpose) |
| `create_namespace` | **explicit forward** |
| `get_namespace` | **explicit forward** |
| `namespace_exists` | **explicit forward** |
| `update_namespace` | **explicit forward** |
| `drop_namespace` | **explicit forward** |
| `list_tables` | **explicit forward** |
| `create_table` | **explicit forward** |
| `load_table` | **explicit forward** |
| `drop_table` | **explicit forward** |
| `table_exists` | **explicit forward** |
| `rename_table` | **explicit forward** |
| `register_table` | **explicit forward** |
| `update_table` | **explicit forward** |

### 2.2 Defaulted methods (16) — every fall-through resolved

| Method | Before | Disposition | Why |
|---|---|---|---|
| `update_namespace_properties` | silent default | **stated omission** | default composes `get_namespace` + `update_namespace` (both forwarded) |
| `set_namespace_properties` | silent default | **stated omission** | thin over `update_namespace_properties` |
| `remove_namespace_properties` | silent default | **stated omission** | thin over `update_namespace_properties` |
| `publish_create_table` | silent default | **explicit forward** | inner may override (default composes `register_table`) |
| **`publish_replace_table`** | silent default | **explicit forward (HIGH)** | trait default = `FeatureUnsupported`; `MemoryCatalog` implements CAS replace |
| `list_views` | silent default | **explicit forward** | MemoryCatalog implements views; default = unsupported |
| `create_view` | silent default | **explicit forward** | same |
| `load_view` | silent default | **explicit forward** | same |
| `drop_view` | silent default | **explicit forward** | same |
| `view_exists` | silent default | **explicit forward** | same |
| `rename_view` | silent default | **explicit forward** | same |
| `update_view` | silent default | **explicit forward** | same |
| `name` | silent default | **explicit forward** | Memory/Glue override with construction name; default = `UNNAMED_CATALOG` |
| `properties` | silent default | **explicit forward** | Memory/Glue override; default = empty map |
| `invalidate_table` | silent default | **explicit forward** | cache-bearing catalogs may override no-op default |
| `invalidate_view` | silent default | **explicit forward** | same |

**Summary:** 14 required explicit + **13 explicit forwards** of defaulted + **3 stated omissions** = full roster closed. Zero silent fall-throughs.

---

## 3. Tests (4)

| Test | Claim |
|---|---|
| `publish_replace_table_forwards_to_inner_spy` | HIGH: spy count = 1; succeeds via MemoryCatalog (not `FeatureUnsupported`) |
| `name_forwards_inner_value_unchanged` | forwarded read returns inner `"memory"` |
| `update_namespace_properties_composes_via_forwarded_methods` | stated omission: spy sees `get_namespace` + `update_namespace`; property persists |
| `list_views_forwards_to_inner_spy` | views forward: spy count = 1; `Ok(empty)` not unsupported |

All AWS-free on `memory_catalog` + `SpyCatalog` + `NamespaceScopedCatalog`.

---

## 4. Gate evidence

```text
cargo test -p repark-iceberg --lib namespace_scoped
# 4 passed

cargo clippy -p repark-iceberg --lib -- -D warnings
# clean

# full gate: make verify (recorded at PR time)
```

---

## 5. Map lockstep

- `crates/repark-iceberg/map.md` — Known limitations: G17 closed + repin duty restated
- `crates/repark-iceberg/src/catalog/map.md` — Contents lists `namespace_scoped_tests.rs`
- `task/map.md` — this ledger linked

---

## 6. Conductor A11 freeze

Base freeze `9acb566`. No fork-side, registry, or lock edits.

## Landing note (L-1, 2026-08-12)

§6 / no-handoff classified **ALREADY-LANDED** — no registry surface (catalog forwards are
engine-wrapper completeness, not a Spark divergence).
