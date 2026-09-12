# RePark deterministic silver-layer compiler

**Design proposal · 2026-09-12 · revision 1**

_Filed 2026-09-12 by the orchestrator at the owner's instruction ("add this to the 1.5 roadmap");
classified as a campaign document per its own section 18 note. The v1.5 row and the Q&A log of
[release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) carry the ruling; the first unit is
S-0 of section 17, and the ten SIL decisions of section 18 are open for the owner. The original
silver-layer generator brief this proposal develops is not in the repository yet; its link below
is kept as prose until it is filed beside this document._

This document proposes a Rust-native capability for turning bronze data into repeatable,
source-aligned silver datasets. It develops the ideas in the original silver-layer generator
brief (`deterministic_silver_layer_generator.md`, the owner's source document, not yet filed).
The original remains unchanged.

This is a design and implementation-planning artifact. Proposed names, schemas, and
interfaces are illustrative; they are not claims about shipped RePark APIs. Open decisions
and implementation proof obligations are listed in section 18. The document retires when
the accepted scope is delivered or superseded and its decisions are preserved in the
implementation records. If imported into the repository, classify it as a campaign document.

## 1. Product objective

Given a pinned bronze dataset and an explicit transformation policy, RePark should produce:

- Accepted silver rows with a declared schema.
- Quarantined rows with explainable failures and recoverable source values.
- An account of rows superseded by version selection.
- Reproducible plan and run identities.
- Validation evidence tied to the exact published result.
- Safe retry and publication behavior when a process fails.

The core runs without an LLM, a JVM, or generated Python compute. Rust owns reusable
production behavior. Python provides a thin authoring and invocation interface.

Silver remains source-aligned. It can standardize representation, apply approved cleansing,
validate values, and select record versions. It does not infer business entities, join
domains, or invent a business model. Even a simple transformation can change meaning, so
its authorization must be visible in policy.

## 2. Recommended decisions

These are the proposed defaults for review, not owner-approved implementation rulings.

| Decision | Proposed default | Reason |
|---|---|---|
| Durable specification | A typed, versioned `SilverPlan`. | Generated code is an export; the plan owns the contract. |
| First backend | Native RePark execution through DataFusion and Arrow. | Fits the Rust-first architecture and reuses the existing engine. |
| First input | An existing bronze Iceberg table at an explicit snapshot. | Separates transformation correctness from connector and CDC correctness. |
| First execution mode | Bounded full-snapshot batch. | Gives replay and validation a finite, fixed input. |
| Initial publication | One classified Iceberg result table per logical dataset. | Accepted, quarantined, and superseded rows share one table commit. |
| Version selection | Latest declared source version, before payload validity decides acceptance. | An invalid update cannot silently expose an older valid value. |
| Ambiguous ordering | Fail the run if conflicting records tie at the greatest source-order tuple. | A deterministic arbitrary winner is not necessarily a correct winner. |
| Profile-driven inference | Advisory until an explicit policy is accepted. | Observed values do not establish column meaning. |
| Schema drift | Compare and classify; block unapproved output changes. | Regeneration is not permission to change a consumer contract. |
| Further backends | Add after the native contract passes independent acceptance. | Portability requires semantic qualification, not just code generation. |

## 3. Scope and boundaries

### First implementation

- One logical source table per plan; multiple plans may be run independently.
- An existing, retained bronze snapshot with a stable per-record identifier.
- Explicit source-to-target field mappings.
- A small, published matrix of supported source types, keys, ordering types, and transforms.
- Required-value checks, approved text normalization, and explicit timestamp parsing.
- Optional version selection with a declared key and source-order tuple.
- A classified result, validation metrics, and one atomic publication point.
- Replay and recovery for a bounded set of failure conditions.

The first input can be a row snapshot or an explicitly declared version-history dataset
without delete events. A history dataset must carry usable source ordering. Neither form
is a claim of general CDC support. A dataset that needs tombstones to represent current
state is ineligible until delete semantics are implemented.

### Deferred extensions

- Source database ingestion and snapshot-to-CDC handoff.
- PostgreSQL and SQL Server metadata discovery adapters.
- Incremental maintenance, delete processing, and continuously running jobs.
- Automatic type changes based on profiling.
- Nested-array explosion and row multiplication.
- Cross-table joins, foreign-key enforcement, entity resolution, and business modeling.
- Multiple physical output tables with coordinated publication.
- Spark, Polars, or generic SQL export backends.
- Optional agent-assisted policy proposals.

Metadata may describe foreign keys. That does not imply that this no-join system validates
referential integrity. Nested values remain intact where supported; a field's presence
must not implicitly trigger `dynamicFlatten` or an explosion of its arrays.

## 4. Architecture and ownership

```text
Source facts / metadata           Approved policy
           \                         /
            \                       /
             Typed plan construction
                       |
              Semantic validation
                       |
          Frozen SilverPlan + input binding
                       |
          Native RePark plan compilation
                       |
     DataFusion / Arrow batch execution
                       |
         Candidate classified result
                       |
          Quality and integrity gates
                       |
          One Iceberg publication commit
                       |
         Snapshot-bound read handle
          /            |             \
    Accepted      Quarantined      Superseded
```

Profiling supplies structured evidence to plan authoring. Execution uses an already
accepted plan; it does not ask a classifier or an agent to reinterpret a column mid-run.

The compiler lowers supported operations into existing engine expressions and plans.
It should not introduce another optimizer, scheduler, catalog, or transaction engine.
Missing reusable kernels belong in the existing owning Rust component. Table-format
primitives belong in the separate owned Iceberg fork.

Python may accept a policy document, construct a request, and return a plan explanation
or run handle. It does not iterate over rows to cleanse, profile, rank, or validate them.
The existing user-supplied UDF exception does not authorize built-in Python fallbacks.

### Module organization

Use one feature boundary with focused modules. A possible layout inside the selected
owning Rust component is:

```text
silver.rs
silver/
  map.md
  plan.rs          # typed contract and structural validation
  policy.rs        # matching, precedence, and effective policy
  identity.rs      # source, field, plan, and run identities
  profile.rs       # evidence requests and profile metadata
  compile.rs       # lowering to existing engine operations
  classify.rs      # version selection and row disposition
  validate.rs      # quality gates and accounting
  publish.rs       # adapter to existing Iceberg write lifecycle
  tests.rs         # focused contract tests
```

This is a responsibility map, not a request to create every file or a new crate immediately.
The implementation intake must check the current crate DAG and actual callers before
choosing the owning component. Introduce separate crates only when a real dependency or
reuse boundary warrants them. Keep table commit code in its existing storage owner.

Public Python integration tests stay with the relevant feature tests. Each new repository
directory receives its required hand-written map. Durable invariants belong in those maps
or the authoritative design; worker briefs link to them.

## 5. Input identity and bronze contract

An input binding identifies a dataset, not just a mutable table name:

| Field | Purpose |
|---|---|
| Source-system identity | Distinguishes independent source systems and tenants. |
| Source table identity | Includes catalog/schema/table and, where available, a stable native identity and generation. |
| Bronze table UUID | Detects a table dropped and recreated under the same name. |
| Bronze snapshot ID | Freezes the data read by profiling, execution, and replay. |
| Schema identity and fingerprint | Records field identities, types, nullability, and relevant comparison semantics. |
| Bronze record identity | Uniquely identifies each input record within the bound snapshot. |
| Dataset mode | Declares row snapshot or eligible version history; never inferred from duplicates. |
| Ordering contract | Defines source-version columns and comparison rules when selection is enabled. |

A stable bronze record ID is an MVP precondition. Validate that it is non-null and unique
within the input. Do not derive it from arbitrary scan order. If a future adapter uses a
snapshot-bound file/row locator, that mechanism needs its own proof of stable replay.
The initial record-ID types are integers with numeric comparison and UTF-8 strings with
bytewise comparison. The schema fixes the type; a plan never mixes comparison modes.

Preserve source field identity through renames where the source supports it. Keep an
explicit source-to-target mapping. Reject normalized-name collisions, reserved-name
collisions, or ambiguous identifiers before execution. Preserve case and quoting semantics.
Two sources called `contacts` must not automatically share a target table.

Bronze is the durable representation for replay. Silver processing does not rewrite it.
The retention policy must preserve input snapshots and referenced source values for the
promised replay window. If retention has removed that evidence, report that replay is no
longer available; do not claim that a plan hash alone can reconstruct the result.

## 6. Determinism and identity

The primary guarantee is logical:

> The same bound input and effective semantics produce the same accepted values, types,
> version winners, row dispositions, and validation decisions.

Physical file names, task placement, wall-clock audit timestamps, and incidental scan order
are outside that guarantee. Ordered output is guaranteed only when the contract requests it.
Do not call a floating-point reduction bit-reproducible without proving its execution order
or defining an appropriate numerical contract.

Keep separate identities:

- **Plan ID:** hash of the canonical semantic plan, effective policies, source-field
  contract, plan format version, and compiler semantics version.
- **Run key:** hash of the plan ID, bound input table/snapshot/schema, target identity,
  engine semantics version, and effective semantic settings.
- **Attempt ID:** unique identifier for one execution attempt, used for diagnostics and
  staging. It does not change the logical result contract.
- **Publication identity:** the output table UUID, committed snapshot ID, and output
  schema identity, associated with the run key.

Canonicalization must define field ordering, enum representation, numeric encoding,
absent versus explicit-null values, and Unicode handling. Reject duplicate configuration
keys and unknown semantic fields. Preserve transform order because it changes meaning.
Exclude credentials, timestamps, and purely descriptive comments from semantic hashes.

Record timezone, timestamp grammar, key comparison, null ordering, numeric rounding,
and relevant implementation versions. Reuse the effective plan on replay; do not rebuild
it from a fresh profile and assume that the same policy file guarantees the same plan.

## 7. Discovery, profiling, and policy are separate concerns

### Discovery reports facts

Discovery records source types, column identity, constraints, generated/default expressions,
collations, and potential change-tracking fields. It also records which facts are unavailable
or untrusted. A declared key is not proof that the bound bronze snapshot contains no duplicates.
A timestamp-shaped column is not proof of a reliable source-version order.

The first version consumes supplied metadata and the bronze schema. Later database adapters
produce the same model. Source schema discovery and ingestion must agree on the schema that
actually produced each bronze batch.

### Profiling supplies evidence

Each profile carries input identity, profile algorithm/version, scan or sample method,
sampling seed where relevant, observation counts, approximation parameters, and completion
status. Unavailable statistics remain unavailable; they do not become zero.

Start with bounded, useful measures: row/null counts, supported min/max values, approved parse
attempts and failures, and key/order diagnostics. Exact distinct counts and candidate-format
classification are optional work with explicit memory and scan budgets.

Combine compatible measures into shared aggregations. Frequent schema checks need not trigger
a full profile. Schedule or incrementally maintain expensive evidence only after the basic
batch contract is established. Approximate statistics must not secretly become hard exact
acceptance gates.

### Policies authorize decisions

Keep three concepts distinct: declared source facts, observed evidence, and approved
transformations. A column containing `00123` may be an identifier. Empty strings and nulls
may be different source values. A parseable date string does not identify its timezone.

Proposed precedence, from most specific to least specific:

1. Explicit table-and-field policy.
2. Table policy.
3. Explicitly declared logical-type policy.
4. Source-type policy.
5. Organization default.

At each precedence level, conflicting scalar settings are errors. A more specific transform
list replaces a less specific list unless composition is explicitly requested. Composition
preserves declared order. Do not concatenate overlapping rule lists by accident.

Name patterns and profile classifications can suggest a policy. They do not automatically
authorize a destructive transformation in the first version. Each effective field policy
must explain which rules supplied its settings. Explanations come from structured evidence,
without an LLM call.

## 8. Typed SilverPlan contract

The plan must be rich enough that the backend does not invent missing semantics.

| Group | Required content |
|---|---|
| Identity | Plan format, source/target identities, schema contract, policy and compiler versions. |
| Input | Dataset mode, source-field mapping, record identity, required metadata, supported input types. |
| Columns | Source field ID, target name/type/nullability, ordered typed transforms, named validators. |
| Selection | Enabled/disabled, key fields, comparison semantics, ordering tuple, tie policy. |
| Classification | Required disposition rules and treatment of invalid keys and invalid winners. |
| Quality | Named gates, units, denominators, thresholds, empty-input behavior, and severity. |
| Execution | Supported backend, semantic settings, resource limits, and overflow/spill behavior. |
| Publication | Target schema contract, required commit preconditions, run-marker contract, and publication strategy. The expected base snapshot is filled by the run binding. |

Operations are typed enums with validated parameters, not arbitrary snippets of Rust,
Python, or SQL. Examples include `Trim`, `EmptyToNull`, `ParseTimestamp`, `RequireNonNull`,
and `CheckAllowedValues`. Unsupported operations or parameter combinations fail at compile
time. A backend must never omit a rule or substitute a Python row loop.

The compiler validates type transitions and operation order before scanning data. Identifier
construction uses the engine's typed or correctly quoted interface. Source names and policy
values must never be interpolated into executable source text without proper handling.

### Illustrative policy definition

This is a proposed authoring format. It is not an existing RePark configuration API. The
listed field IDs are examples and must be resolved against the bound bronze schema.

```yaml
plan_format_version: 1
dataset_id: crm.contacts.silver
source:
  source_system_id: crm-production
  catalog: application
  schema: crm
  table: contacts
input_contract:
  kind: versioned_rows_without_deletes
  bronze_record_id_field: 90
columns:
  - source_field_id: 1
    target_name: contact_id
    target_type: {kind: int64}
    nullable: false
    transforms: []
    validators:
      - {rule_id: contact_id_required, kind: require_non_null}
  - source_field_id: 2
    target_name: first_name
    target_type: {kind: utf8}
    nullable: true
    transforms:
      - {kind: trim, characters: ["U+0020"]}
      - {kind: empty_to_null}
    validators: []
  - source_field_id: 3
    target_name: created_at
    target_type: {kind: timestamp, unit: microsecond, timezone: UTC}
    nullable: true
    transforms:
      - kind: parse_timestamp
        format_id: iso8601_seconds_offset_v1
        invalid_value: quarantine_record
    validators: []
selection:
  strategy: latest_source_version
  key_fields: [1]
  order_fields:
    - {source_field_id: 91, direction: descending}
  conflicting_tie: fail_run
  identical_source_payload_tie: lowest_bronze_record_id
quality:
  - {rule_id: row_accounting, kind: exact_disposition_accounting}
  - {rule_id: accepted_keys, kind: accepted_key_uniqueness}
  - rule_id: invalid_winners
    kind: ratio_at_most
    numerator: quarantined_keyed_winners
    denominator: keyed_winners
    threshold: 0.01
    on_empty: pass
  - rule_id: invalid_keys
    kind: ratio_at_most
    numerator: unkeyed_quarantined_rows
    denominator: input_rows
    threshold: 0.0
    on_empty: pass
publication:
  strategy: single_classified_table
  empty_input: require_explicit_empty_replace_option
```

The example thresholds illustrate configuration, not recommended limits for every source.
The input snapshot and expected output base snapshot belong to the run binding.

`iso8601_seconds_offset_v1` would accept a calendar-valid date/time in the form
`YYYY-MM-DDTHH:mm:ssZ` or `YYYY-MM-DDTHH:mm:ss±HH:MM`. Its first contract excludes fractional
seconds, leap seconds, and timezone-free values. Other grammars need separately specified
profiles. The compiler must refuse this profile until the backend implements and tests it.

## 9. Transformation and validation semantics

### Text and identity

Text transforms specify their character and comparison rules. The example trims only
U+0020; it makes no claim about every Unicode whitespace character. Key normalization is
disabled by default. A plan that changes key values needs an explicit identity decision
and tests for collisions.

An email or phone validator must name its precise acceptance profile. A short email regex
is not universal email validation. Lowercasing an entire field is an explicit transformation,
not an inference justified by its column name. These logical-type profiles can follow the
first small transform set once their semantics are agreed.

### Null, invalid, and unknown

- A source null in a nullable field is a valid absence unless policy says otherwise.
- A malformed non-null value is a parse failure, even if the target field is nullable.
- A missing required source field is a schema error, not a row of synthetic nulls.
- A validator has an explicit outcome. Unknown or unevaluated must not leak through SQL
  three-valued logic and disappear from both accepted and quarantine filters.

Record typed failures such as rule ID, source field ID, failure code, and safe diagnostic
context. Validation applies to the declared source or transformed stage; that stage is part
of the rule. Required source values and required post-transform values are different checks.

For tolerant timestamp conversion, preserve a parse-success signal separately from the
nullable result. Spark's `try_to_timestamp` illustrates this distinction: invalid input can
produce null, so null alone does not explain why conversion produced it. The native compiler
must implement the declared contract independently of a session's incidental cast settings.
[Spark reference](https://spark.apache.org/docs/4.1.2/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_timestamp.html).

### Numeric and comparison behavior

Declare precision, scale, overflow, and rounding for numeric conversions. Silent truncation
or saturation is never an implicit cleansing rule. Restrict the initial key/order types to
a finite supported set, such as integers and explicitly supported binary-comparison strings.
Do not treat a source's case-insensitive unique key as a binary key without a compatibility
decision. Unsupported collations and ordering types fail before execution.

## 10. Version selection and row disposition

The default answers: "What does the latest eligible source version say, and is that version
acceptable?" It does not silently search backward for a version that happens to pass validation.

For selection-enabled input:

1. Validate structural input identity and required ordering columns.
2. Evaluate transforms and row rules while retaining source identity and original evidence.
3. Quarantine rows whose key is invalid; they cannot participate in keyed selection.
4. Treat a null or invalid ordering value on a keyed row as an unorderable history: fail the
   run. Do not discard it and select an older row.
5. Rank all remaining rows by the declared source-order tuple, regardless of payload validity.
6. If the greatest ordering tuple has conflicting source payloads, fail the run. For identical
   payload repeats, select the lowest stable bronze record ID and supersede the other repeats.
7. Mark the winner accepted if its row rules pass; otherwise mark it quarantined.
8. Mark other keyed rows superseded. Retain their rule failures as diagnostic evidence.

Payload equality for repeated versions must use a defined canonical representation of the
original source business fields and schema identity. Exclude ingestion-attempt metadata and
record locator fields. A hash can accelerate comparison but must not silently replace equality
without a documented collision policy. Conflicting greatest-version ties are checked even if
both candidate rows are invalid.

| Input history for one key | Disposition |
|---|---|
| Version 10 valid, version 11 valid | 11 accepted; 10 superseded. |
| Version 10 valid, version 11 invalid | 11 quarantined; 10 superseded. No accepted row for this key. |
| Two version-11 rows with identical source payloads and distinct record IDs | Stable record-ID winner; other row superseded. Winner's validity decides its disposition. |
| Two conflicting version-11 payloads | Run fails; published output does not advance. |
| A keyed row has unknown source order | Run fails; published output does not advance. |
| A row has an invalid key | Quarantined and counted separately from keyed winners. |

Rows superseded by a greater version remain accountable even if older equal-version payloads
conflict. The first contract blocks ambiguity in the selected greatest version; a stricter
historical-consistency policy can additionally reject conflicts anywhere in the history.

When selection is disabled, each input row is accepted or quarantined according to its rules;
there are no superseded rows and no uniqueness promise unless another explicit rule requires it.
No key discovered by a crawler enables selection automatically.

Ingestion timestamps are provenance by default. They become version order only through a
deliberate policy that defines ties, late arrival, replay, and the consequences of ingestion
order differing from source order.

## 11. Classified output and quality gates

The first version stores one physical classified result. Its conceptual envelope contains:

- Run key and bronze record identity.
- Source table/snapshot reference and output schema identity.
- One disposition: `accepted`, `quarantined`, or `superseded`.
- Typed transformed payload.
- Named rule failures and the relevant source-value evidence or a retained bronze reference.

Separate the envelope from user field names, for example with a typed payload struct. Do not
reserve an arbitrary prefix and silently overwrite a colliding source field.

The public silver projection exposes accepted payloads. The quarantine projection exposes
rejected records and their failures. An audit projection exposes superseded records. Access
to the underlying classified table includes rejected data; grant it accordingly. A filtered
view alone is not an access boundary for a user who can read the physical table directly.

Every successful bounded batch satisfies:

```text
input_rows = accepted_rows + quarantined_rows + superseded_rows
```

The sets are disjoint and collectively cover the input record IDs. Counts alone are not
enough: duplicate output IDs could conceal missing input IDs. Validate identity accounting
through exact checks or a separately justified scalable mechanism; a checksum alone is not
proof of exact membership. Abort structural failures before publishing an incomplete account.
The no-join boundary applies to combining source business datasets. Internal integrity checks
may compare record identity sets, with their additional I/O included in the resource budget.

Metrics distinguish their populations:

| Metric | Population / denominator |
|---|---|
| Invalid-key ratio | Unkeyed quarantined rows / input rows. |
| Invalid-winner ratio | Quarantined keyed winners / all keyed winners. |
| Parse-failure ratio | Failed non-null parse attempts / all non-null parse attempts for that field. |
| Superseded ratio | Superseded rows / rows eligible for keyed selection. |
| Accepted key uniqueness | Accepted winners only; required when keyed selection is enabled. |

Define empty-denominator behavior for each ratio. An empty input must not accidentally
replace a populated result: the run requires an explicit empty-replacement choice. A generic
"silver must contain at least 95% of bronze rows" rule is inappropriate when selection
legitimately removes many versions.
An operator can supply the empty-replacement choice through an approved job configuration;
the execution path does not require a human or agent call to make that decision mid-run.

Quality gates run against candidate output before publication. A failed threshold leaves
the last accepted snapshot current. A failed attempt can retain controlled diagnostic
artifacts, but those artifacts are not a newly published silver dataset.

## 12. Publication, concurrency, and recovery

### One publication boundary

Iceberg commits table state atomically. Two independent writes to a silver table and a
quarantine table do not become one transaction merely because the same script issues them.
The initial design uses one classified table so all dispositions share one publication point.
[Iceberg reliability model](https://iceberg.apache.org/docs/1.7.1/reliability/).

This requires a storage integration that can prepare output, complete quality checks, and
then publish exactly that prepared result. It is an implementation proof obligation. Do not
assume that an existing high-level `createOrReplace` call exposes this lifecycle. If the
necessary stage/validate/commit primitive is missing, scope it in the owning storage layer
or fork before claiming this publication design is implementable.

### Proposed run lifecycle

```text
BOUND → VALIDATED_PLAN → PREPARING_OUTPUT → QUALITY_PASSED → COMMITTING → PUBLISHED
                             |                  |               |
                           FAILED             FAILED      RESOLVING_COMMIT
```

1. Bind the bronze snapshot, effective plan, semantic settings, target identity, and expected
   output base snapshot. Persist the immutable plan needed to explain the run.
2. Check whether the run key already has a publication. A retry may return that publication
   without executing the transformation again.
3. Write candidate artifacts without advancing the consumer-visible table snapshot. Compute
   metrics over the actual classified batches and verify the staged representation where needed.
4. Complete the declared quality and accounting gates. A candidate must not change between
   validation and publication. Persist any externally referenced final metrics before commit,
   with immutable content identity; failure to persist them blocks publication.
5. Commit the replacement result conditionally against the expected output base. Include the
   run key and provenance/metrics references in the same table commit's metadata.
6. Return a run handle containing the committed output UUID, snapshot, schema, and plan identity.

Do not implement a second write by recomputing the transformation after validation. Reuse
the validated candidate. Do not publish valid rows first and discover quarantine or threshold
failures afterward.

### Failure behavior

| Failure point | Required behavior |
|---|---|
| Before candidate preparation | No visible output change; a retry can restart. |
| During preparation | No visible output change; record abandoned candidate artifacts for approved maintenance. |
| Quality gate fails | Preserve current publication and retain structured failure evidence. |
| Commit response is lost | Resolve whether the run marker committed before attempting another publication. |
| Another run advances the target | Report a publication conflict; do not blindly rebase and overwrite it. |
| Two attempts publish the same run | At most one becomes the publication; the other resolves and returns that identity. |
| An old run is replayed after a newer publication | Return its retained publication or require an explicit rebuild decision; do not silently make old input current. |
| Metrics export fails after commit | Recover metrics from committed provenance; avoid a second data publication. |

Run-key lookup needs a retained, authoritative path through table metadata or a proven
publication index. If retention makes an ambiguous commit impossible to resolve, stop with
an explicit indeterminate result. An absent response is not evidence that a commit failed.
Never delete candidate data while its commit status is unknown.

The idempotency claim is limited to publication under this protocol and retention window.
It does not assert end-to-end exactly-once database ingestion.

### Reader contract

A query captures one output snapshot at its start. Consumers that read accepted and
quarantined results together use the same run handle or explicitly pinned snapshot/schema.
Two independent latest-state queries issued across a commit can observe different runs,
even though each individual table commit is atomic.

Separate physical outputs are a later extension. They require a proven catalog transaction
or a publication manifest that every participating reader actually honors. Writing a manifest
does not protect clients that continue reading independent latest table names.

## 13. Drift and replay

Treat these changes separately:

| Change | Default response |
|---|---|
| New optional source field | Report it; keep the accepted output schema until policy chooses a mapping. |
| Missing mapped field or incompatible type | Reject the run before publication. |
| Rename with stable identity | Propose a mapping update; preserve consumer names unless deliberately changed. |
| Rename without reliable identity | Report ambiguity; do not guess from similar names. |
| Changed key, ordering, or comparison semantics | Require a new reviewed plan and version-selection evidence. |
| Profile distribution shift | Emit evidence/alerts; change semantics only through an accepted policy. |
| Policy or compiler semantics change | Create a new plan identity; validate against fixed snapshots before promotion. |
| Index-only metadata change | Refresh discovery evidence; avoid rebuilding an unchanged effective plan. |

The first version has a fixed output schema per accepted contract. Breaking output changes
need a versioned target or a separately reviewed consumer migration. Preserve the previous
published result while validating a candidate plan.

Replay binds the original input snapshot and effective plan. If code upgrades alter semantics,
either execute the compatible recorded contract or report that exact replay is unavailable.
Do not compare results from different input snapshots and attribute all differences to policy.

## 14. Performance design and measurement

Plan for wide tables, skewed keys, invalid-value bursts, and many versions of a hot key.
The full-snapshot mode can rewrite substantial data; it is an honest initial cost model,
not an incremental-processing claim.

- Lower compatible column transforms into shared projections and reuse parsed expressions.
- Combine compatible profiling and validation aggregates into shared scans.
- Profile the physical execution plan; several chained API calls do not by themselves prove
  multiple scans, and a compact API does not prove a single scan.
- Avoid moving rows through Python or crossing the language boundary once per cell.
- Use bounded batches and the existing resource-management facilities. Spill support is a
  measured execution property; do not promise it for an operator without exercising it.
- Selection needs an explicit strategy for skew and memory limits. Compare sorting/window
  plans with supported keyed aggregation without changing tie semantics.
- Account for the storage cost of superseded rows and diagnostic evidence. Source references
  reduce duplication only when bronze retention and access meet the replay contract.

Before selecting thresholds, record hardware, engine/build identity, data shape, cache state,
source/target storage, and comparison method. Measure wall time, rows/bytes per second, peak
memory, bytes scanned/written, spill, physical scans, and output file counts.

The performance corpus should include narrow and wide tables, mostly-null columns, malformed
timestamps, high-cardinality keys, a dominant hot key, and many repeated versions. Include a
wide case representative of the previously discussed roughly 190-column workload. No speed
target is asserted by this design; acceptance budgets are set before the implementation trial.

## 15. Testing and acceptance evidence

Generated validation checks test conformance to a plan. Independent fixtures and adversarial
tests must also challenge whether the plan and compiler encode the intended behavior.

| Area | Required evidence |
|---|---|
| Plan parsing | Unknown operations, duplicate keys, invalid parameters, missing fields, and unsupported versions reject clearly. |
| Canonical identity | Irrelevant formatting does not change semantic identity; transform order and semantic settings do. |
| Mapping | Quoted identifiers, case, reserved fields, normalized collisions, and same-named source tables remain distinct. |
| Transforms | Value and type assertions for null/empty text, approved trimming, malformed timestamps, offsets, and overflow where supported. |
| Selection | Older-valid/newer-invalid, late arrival, composite keys, null keys/order, identical repeats, conflicting greatest-version ties. |
| Accounting | Every input record appears in exactly one disposition; duplicate or missing output IDs fail. |
| Quality | Correct denominators, empty populations, all-invalid input, and threshold boundaries. |
| Storage | Candidate invisibility, failed validation, concurrent publishers, lost commit response, retry, and old-run replay. |
| Reader consistency | Accepted/quarantine reads bound to one snapshot agree across a concurrent publication. |
| Drift | Missing fields, unsupported type changes, stable/ambiguous renames, key changes, and evidence-only profile drift. |
| Resources | Bounded failure or proven spill under memory pressure; skew and wide-table behavior are measured. |

Exercise the public entry points that the feature exposes. Shared SQL-visible behavior keeps
RePark's native/Spark-door testing obligations; a native-only first API does not justify an
unmeasured parity claim. Tests inspect collected values and Arrow types, not just formatted
display output.

Use reference-engine comparisons where they provide an oracle, and label deliberate
divergences. Oracle tooling can live outside production and does not introduce a production
JVM requirement. Seed plausible incorrect implementations to prove that acceptance tests
detect the named defects, especially arbitrary tie winners and publication before validation.

## 16. Security and operational limits

Discovery uses the minimum source permissions it needs. Runtime policy documents carry
source identities and approved references, not credentials. Credentials and connection
details remain with the existing session/configuration mechanisms.

Plans cannot execute arbitrary code or direct unapproved writes. Validate target ownership
and identifiers before execution. Use the current sanctioned catalog and write paths.
Diagnostic output should identify failed rules without indiscriminately copying sensitive
source values into logs. Quarantine access and retention are explicit operational choices.

Bronze immutability describes the transformation contract. It does not mean keeping every
record forever: retention and deletion obligations still apply, with their effect on replay
made visible. Cleanup and snapshot expiration use existing authorized maintenance procedures.

## 17. Implementation sequence

Each unit receives a bounded brief, explicit proof obligations, required tests, and its own
reviewable change. These are capability slices, not reserved package versions.

| Slice | Deliverable | Exit evidence |
|---|---|---|
| S-0: contract and storage feasibility | Settle MVP input/types, selection, quality, and publication decisions; inspect the pinned storage API. | A finite acceptance matrix and evidence that stage/validate/conditional-commit can be implemented in the correct owner. |
| S-1: typed plan | Rust plan/policy models, strict parsing, canonical identity, and deterministic explanation. | Positive/negative fixtures; stable identity tests; no data execution. |
| S-2: batch semantics | Exercise existing engine primitives over bounded Arrow fixtures; add only missing reusable kernels for the declared contract. | Independent expected values/types and exact row accounting, including adversarial selection cases. No second execution engine. |
| S-3: native compilation | Lower the supported contract into RePark/DataFusion operations. | End-to-end collected results match S-2's independent fixtures; unsupported operations fail before writes. |
| S-4: safe publication | Staging, quality gates, one classified result, run markers, and snapshot-bound reads. | Fault injection, concurrent-attempt tests, ambiguous-commit recovery, and no invalid candidate made current. |
| S-5: bounded profiling and drift | Structured evidence, policy provenance, and schema/profile/semantic-change classification. | Reuse of accepted plans is stable; advisory evidence cannot silently alter results. |
| S-6: product interface | Thin Python authoring/run interface, examples, operational documentation, and performance baseline. | A consumer can author, explain, execute, inspect failures, and replay the supported batch without Python row compute. |

The pilot should use an existing bronze table that meets the eligibility contract. A synthetic
adversarial fixture is sufficient for early semantics work; a representative production-shaped
sample is needed before setting performance or operational expectations.

After these slices, evaluate PostgreSQL discovery and ingestion as separate work. SQL Server
and CDC follow the same metadata/input boundary, but each must establish its own capture,
ordering, delete, transaction, and recovery contract. Candidate change-tracking columns are
not automatically interchangeable with a database log position. Define the initial snapshot
and change-stream handoff before claiming an incremental current-state silver dataset.

Additional backends follow only after a backend-neutral semantic corpus exists. Each backend
declares a capability matrix and refuses unsupported combinations. Optional agents can then
suggest policy changes outside execution; accepted policy artifacts remain the executable
authority.

## 18. Decisions and proof obligations before implementation

| ID | Decision or obligation | Recommended resolution / required proof |
|---|---|---|
| SIL-1 | First dataset and supported type matrix | One eligible bronze table; explicit key, source-order, and stable record-ID fields. Publish the finite supported type/operation set. |
| SIL-2 | Invalid latest version | Adopt latest-source-version semantics; no automatic fallback to an older valid row. |
| SIL-3 | Ordering conflicts | Fail on conflicting greatest-version ties or unorderable keyed history; define canonical payload equality for repeats. |
| SIL-4 | Publication primitive | Prove candidate staging, quality-before-commit, conditional publication, and committed run-marker lookup in the pinned storage integration. |
| SIL-5 | Consumer interface | One classified physical table with snapshot-bound projections/read handles; agree how ordinary SQL consumers bind a run. |
| SIL-6 | Retention and quarantine | Set replay/retry windows, source-reference availability, diagnostic access, and approved cleanup behavior. |
| SIL-7 | Quality limits | Choose denominators, thresholds, empty-input behavior, and whether evidence loss blocks publication. |
| SIL-8 | Module ownership | Check current crate DAG and APIs; select the smallest owning feature boundary without creating deferred crates prematurely. |
| SIL-9 | Performance budget | Set hardware/data methodology and memory/latency budgets before measuring candidate implementations. |
| SIL-10 | Schema evolution | Fixed initial output contract; choose a reviewed migration or versioned target for breaking changes. |

These are open for review. Drafting this document does not prove storage capabilities,
approve a policy, authorize deployment, or establish production readiness.

## 19. Reference and authority notes

- Original proposal (`deterministic_silver_layer_generator.md`, the owner's source document, not yet filed in the repository): source material for the concept.
- [RePark contributor contract](../../../AGENTS.md): engineering authority, Rust/Python boundary, crate ownership, testing, and operational limits.
- [RePark architecture](../../../ARCHITECTURE.md): existing component boundaries to inspect at implementation pickup.
- [Owned fork engine contract](https://github.com/TRO-Wolf/iceberg-rust/blob/main/docs/ENGINE_CONTRACT.md) and [parity matrix](https://github.com/TRO-Wolf/iceberg-rust/blob/main/docs/parity/GAP_MATRIX.md): verify capability claims at the exact fork revision consumed by RePark; moving-main links are navigation only.
- [Iceberg reliability](https://iceberg.apache.org/docs/1.7.1/reliability/): table commit and concurrency background, not evidence that a particular RePark staging API is delivered.
- [Spark 4.1.2 ANSI behavior](https://spark.apache.org/docs/4.1.2/sql-ref-ansi-compliance.html) and [tolerant timestamp parsing](https://spark.apache.org/docs/4.1.2/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_timestamp.html): reference behavior to distinguish explicit parsing policy from incidental engine defaults.
- The sibling proposal [database-discovery-lakehouse-planning-2026-09-09.md](database-discovery-lakehouse-planning-2026-09-09.md) owns source discovery, profiling and the CDC recovery protocol that this document's deferred extensions point at.

All implementation and performance claims require evidence from the actual implementation
revision. This proposal deliberately carries no inventory of delivered fork capabilities.
