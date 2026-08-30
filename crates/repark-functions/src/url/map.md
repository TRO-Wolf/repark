# map — repark-functions/src/url

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

The child module of [`url.rs`](../url.rs), which implements Spark `parse_url` / `try_parse_url`.

Rust's default module layout keeps `java_uri.rs` under `url/` beside `url.rs`.

## Contents

- `java_uri.rs` — **X8:** `JavaUri`, a splitter matching `java.net.URI`. Spark's `parse_url` reports
  the components as Java splits them; the `url` crate normalizes instead, which is a different
  answer for the same input. This is why the crate does not simply call `url::Url`.

## Pointers

- Up: [`../../map.md`](../../map.md)
