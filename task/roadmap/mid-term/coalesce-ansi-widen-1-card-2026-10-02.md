# COALESCE-ANSI-WIDEN-1 — bare coalesce/array widening over STRING with DATE, BINARY and DECIMAL

**Filed: 2026-10-02 from the #888 review** (sources:
`/tmp/oc-worker/direct/wo/review-888-fable/README.md` and
the #888 park note (<https://github.com/TRO-Wolf/repark/pull/888#issuecomment-5933337339>),
both verified present 2026-10-02;
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md` line 101 names
this card: bare `coalesce`/`array` STRING × {DATE, BINARY, DECIMAL}).
**Severity:** wrong answers / loud refusals, not yet rated (owner).
**Status:** pre-existing on base. **Target:** mid-term.

## The divergence it records

Bare `coalesce` / `array` (and `upper`) over STRING mixed with DATE, BINARY
or DECIMAL refuse or mistype on base where Spark widens. This is the OUTER
function's coercion, not the nvl family's: after the review's experiment
moved the nvl rewrite before `type_coercion`, the 15 remaining cap-b cells
were all this gap. Base only "got them right" through `nvl`/`nullif` because
DataFusion's core `nvl` and `nullif` mistype their result in a way that
happened to feed the outer function a type it could handle.

## Repro and measured answers

Measured on base versus Spark 4.1.2 (from `review-888-fable/README.md`):

| Cell | Base | Spark |
|---|---|---|
| `coalesce('2024-01-02', DATE'2024-01-02')` | refuses | `date` |
| `array(sdt, dt)` | refuses | `array<date>` |
| `upper(TIMESTAMP)` | refuses | `string` |
| `coalesce(1.50BD, '1.5')` | types `decimal(3,2)` | `double` |

BINARY shapes are not yet measured. Repro to run on the day: bare
`coalesce` / `array` over STRING × BINARY under ANSI on and off, on the
facade, F.expr and SQL doors, against live Spark 4.1.2.

## Scope

Widening of bare outer functions (`coalesce`, `array`, and the measured
`upper` cell) over STRING mixed with DATE, BINARY and DECIMAL, under both
ANSI modes, on every door.

## Out of scope

- NVL-TYPE-COERCION-2 (the nvl-family rewrite; its own card).
- Reintroducing the accidental base behaviour (relying on DataFusion's
  `nvl`/`nullif` mistype to feed the outer function) counts as a regression,
  not a fix.

## Clauses (draft)

- **C-001** The four measured cells answer Spark's type and value.
- **C-002** BINARY shapes are measured against the oracle and pinned.
- **C-003** The nvl-nested p12 cells keep their experiment answers.

## Pointers

- Up: [map.md](map.md) · Sibling card:
  [nvl-type-coercion-2-card-2026-10-02.md](nvl-type-coercion-2-card-2026-10-02.md)
  · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/review-888-fable/README.md`
