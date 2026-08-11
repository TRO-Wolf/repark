# Superseding design note — `SessionExtension::configure` takes a `SessionBuildConf`

**Settled 2026-08-10** · supersedes the `SessionExtension` half of the seam freeze in
[session-api.md](session-api.md) § "Seam freeze (2026-08-08, phase-2 PR-6)", repeated in
[sql-doors.md](sql-doors.md) §3 · implemented by V2 Engine Hardening unit **H-1a split B**
(ledger [`task/h1a-ledger.md`](../../task/h1a-ledger.md) "§ Split B", decision D-B6) ·
`SqlDialect::execute` is **not** touched and stays frozen exactly as shipped.

## Why this note exists at all

The 2026-08-08 freeze says, in its own words:

> `EngineContext` is `#[non_exhaustive]`, so adding a field stays non-breaking; **changing or
> removing a method or an existing field now requires a superseding design note.**

H-1a split B changes `SessionExtension::configure`'s signature. That is exactly the case the
sentence names, so this note is the amendment the freeze demands — written before the change lands,
not after it was noticed. `docs/design/map.md`'s standing rule ("changing a decision here means a
new dated design pass, not an in-place edit") is why it is a new file rather than an edit to
`session-api.md`.

## The change

```rust
// FROZEN 2026-08-08 (phase-1 shape)
fn configure(&self, conf: &HashMap<String, String>, config: SessionConfig)
    -> DFResult<SessionConfig>;

// SUPERSEDING 2026-08-10
pub struct SessionBuildConf<'a> {
    pub conf: &'a HashMap<String, String>,          // unchanged, same meaning
    pub session_time_zone: &'a SessionTimeZone,     // NEW: what build() already resolved
}
fn configure(&self, session: SessionBuildConf<'_>, config: SessionConfig)
    -> DFResult<SessionConfig>;
```

`register` is untouched. Both hooks stay defaulted, so a session built without an extension is
still pure DataFusion.

## Why the seam had to move

Spark resolves every calendar field of a `TIMESTAMP` in `spark.sql.session.timeZone`. The engine
crate `repark-core` owns that key and validates it **once**, in `build()`. The extractors live in
`repark-functions`, a capability leaf with no engine edge — it cannot import the key's constant, so
the value has to travel. The door (`repark-spark`) is the one crate that sees both, and `configure`
is the one hook that runs at the right moment (after the write knobs, before the `RuntimeEnv`).

Three ways to get the value across were considered:

| Option | Why not |
|---|---|
| **A. The door re-parses the map.** `SparkExtension::configure` already receives `conf`, so it could call `repark_core::resolve_session_time_zone` itself. | A **second resolution** of a value the engine has already settled. Split A's headline property is "resolved ONCE, at construction"; two resolvers is how a validated value and an unvalidated one drift apart, and nothing would catch the day one of them normalizes differently. |
| **B. A new defaulted hook** — `fn configure_with(&self, session: SessionBuildConf<'_>, config)` alongside an intact `configure`. | Non-breaking, and genuinely the cheaper option. Rejected because it leaves **two** configure positions in a seam whose entire value is that there is one, and the older one becomes a trap: an extension that implements only `configure` silently never sees resolved session values. A seam that is frozen but confusing is worse than a seam amended in the open. |
| **C. Widen the argument (chosen).** One hook, one position, and `SessionBuildConf` is the extension point for the next resolved value. | Breaks the frozen signature — hence this note. |

## What it costs, priced honestly

* **Three in-tree implementors**, all updated in the same change: `repark_spark::SparkExtension`,
  `repark_ta::TaExtension` (a defaulted pass-through), and `repark_core`'s test-only
  `RecordingExtension`. A fourth, `NoopExtension`, uses the default body.
* **No external implementors.** `SessionExtension` is not exported from the wheel and appears in no
  published API surface; `repark` v0.0.0 has never shipped.
* **The hook order pin was strengthened rather than merely fixed:** it now sets a *padded* zone
  (`"  Asia/Tokyo "`), so a door that re-parsed the map instead of taking the resolved value reds
  — option A is now mechanically excluded, not merely argued against.

## What stays frozen

* `SqlDialect::execute(EngineContext<'_>, &str)` — untouched.
* `SessionExtension::register(&SessionContext)` — untouched.
* **Extensions are session-scoped, not dialect-scoped**, with all three consequences
  ([session-api.md](session-api.md) § "Seam freeze"). This change does not touch that, and the
  ANSI-door cell of H-1a split B's matrix is a *single-session* row precisely because of it.
* The amendment rule itself. `SessionBuildConf` is a plain struct, **not** `#[non_exhaustive]`
  today; adding a field to it is therefore also a breaking change and needs its own note. That is
  deliberate — the point of widening the argument was to make the dependency visible, and a silent
  growth path would give that away.

## Revisit trigger

If a second resolved value ever needs to reach the seam, add it to `SessionBuildConf` with a note
like this one — and at that point reconsider marking the struct `#[non_exhaustive]`, which trades
the visibility above for a non-breaking growth path. One value is not enough evidence to make that
trade now.
