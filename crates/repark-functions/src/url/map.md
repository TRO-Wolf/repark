# map — repark-functions/src/url

## Purpose

The child module of [`url.rs`](../url.rs), which implements Spark `parse_url` / `try_parse_url`.

Created by **LRS-5 (2026-08-20)**: `java_uri.rs` had been included from the crate `src/` root with
`#[path = "java_uri.rs"]`, which AGENTS.md forbids. `url.rs` may sit beside `url/` in Rust 2018, so
the move needed no rename.

## Contents

- `java_uri.rs` — **X8:** `JavaUri`, a splitter matching `java.net.URI`. Spark's `parse_url` reports
  the components as Java splits them; the `url` crate normalizes instead, which is a different
  answer for the same input. This is why the crate does not simply call `url::Url`.

## Pointers

- Up: [`../../map.md`](../../map.md)
