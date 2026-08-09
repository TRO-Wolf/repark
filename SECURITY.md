# Security policy

## Reporting a vulnerability

Report suspected vulnerabilities **privately via GitHub security advisories**: use
["Report a vulnerability"](https://github.com/TRO-Wolf/repark/security/advisories/new) (Security
tab → Advisories → Report a vulnerability). Please do not open a public issue for an undisclosed
vulnerability. Include a description, the affected component/commit, and reproduction steps if
available. You'll get an acknowledgement and, once triaged, a remediation timeline.

## Supported versions

The project is **pre-alpha**; there are no supported versions yet. The published `repark 0.0.1`
packages on PyPI and crates.io are name-holding placeholders, not releases. Security fixes target
`main`; there is no LTS branch.

## Automated supply-chain controls

Defense in depth runs in CI (see [.github/workflows/](.github/workflows/)):

| Surface | Gate |
|---|---|
| Rust licenses / banned / duplicate crates | `cargo deny` (config in [deny.toml](deny.toml)) |
| GitHub Actions workflows | `zizmor` static analysis; every action SHA-pinned |
| Typos / TOML hygiene | `typos`, `taplo` |
| Dependency freshness | Dependabot — grouped PRs (cargo + actions); same-day for advisories |

Additional gates (cargo-audit on a populated dependency tree, pip-audit, CodeQL) return with the
code they audit as the port lands crates and Python packages. Lockfiles are checked in for
reproducible builds. Tier-1 CI runs with a read-only token and **no secrets**; live-AWS CI (tier 2)
runs only against merged code via OIDC role assumption — never against unmerged PRs.

## AWS credential handling (applies as the engine lands, phase 1+)

The engine reads AWS credentials only through the standard AWS SDK chain (environment → shared
profile → instance/task role); credentials are never hardcoded and never logged. AWS SDK usage is
confined to the Iceberg catalog/IO crate (`crates/repark-iceberg`). Scope
the IAM principal to the minimum needed for Glue + S3 Tables + the warehouse / read S3 buckets.

### Accepted single-user credential posture

RePark is a **single-user / single-process** engine, not a multi-tenant SaaS. Callers control the
SQL and config they pass; ambient AWS credentials from the default chain apply to any `s3://`
bucket the session is asked to read and to configured Glue/S3 Tables catalogs. There is **no**
in-engine bucket allowlist or endpoint allowlist. Operators must use least-privilege IAM and treat
untrusted SQL as out of scope for this threat model. A future **server mode** changes this: column
masking, row filters, and per-session credential vending are only enforceable by a server, and are
consciously deferred to that milestone — see
[docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md).
