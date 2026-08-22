---
name: code-quality
version: "1.0"
description: >-
  Portable code-quality conventions for a typed, multi-language codebase, with
  the Python rules stated in enforceable form: type everything, one structured
  data container (Pydantic v2 BaseModel) rather than dataclasses or attrs, no
  function defined inside another function, and names that carry the work they
  do. Use this skill when writing or reviewing Python or Rust in a repo that
  installs it, when deciding whether a convention should be a review duty or a
  mechanical gate, or when a conventions gate fails and the sanctioned outs
  need to be weighed. It states rules and their rationale; it does not run the
  gate, and where a host repo has its own contract, that contract wins.
---

# Code quality — conventions that survive a year

## What this skill is for

Conventions decay in one predictable way: they are written once, in a document
that only the person who wrote it reads, and then every reviewer applies them
from memory. The rules below are the ones worth writing down, each with the
failure it prevents, and each marked with **how it is held** — by a linter, by
a purpose-built gate, or by review. That third category is not a lesser one,
but it should be small and deliberate, because a rule held only by review is a
rule that decays at the speed of reviewer attention.

This skill is portable. It names no repository, no crate and no file. A host
repo binds it by pointing its own contract at these rules and by installing a
gate; the host contract is authoritative wherever the two differ.

---

## The four Python rules

### 1. Type everything

Every parameter, every return, every public attribute, every module-level
constant carries an annotation.

**The failure it prevents.** An unannotated signature is a hole in the
contract. The type checker cannot see through it, the reader cannot tell
whether `None` is a legal return, and the next caller finds out at runtime.
Partial typing is worse than none, because it makes the untyped holes look
deliberate.

**How it is held: a linter.** Ruff's `ANN` rule set covers this completely.
Select `ANN` and the rule enforces itself. `ANN401` (`typing.Any` in an
annotation) is the one worth ignoring by default, because there are real
signatures whose argument genuinely is any object.

Tests are the standard carve-out — a per-file ignore of `ANN201`/`ANN202`/
`ANN001` under the test tree — on the grounds that a test function's return
type is always `None` and annotating it is noise, not contract.

### 2. One structured-data container

Pydantic v2 `BaseModel` for everything that holds named fields together:
configs, payloads, internal records, value objects, argument groups. Never
`dataclasses`, never `attrs`. For immutability set
`model_config = ConfigDict(frozen=True)` rather than reaching for a frozen
dataclass.

**The failure it prevents.** Three container libraries in one codebase means
three conversion styles, three notions of what "frozen" means, and a constant
low-grade tax at every boundary where one meets another. Pydantic is not
chosen here because it is the best data holder in the abstract; it is chosen
because it is the one that also validates, serializes, round-trips through
JSON, and generates a schema. Picking the superset once is cheaper than
picking per-file and converting forever.

**The cost, stated honestly.** A `BaseModel` validates on construction, which
a `dataclass` does not. Converting an existing `dataclass` therefore adds a
runtime check that was not running before, and it can reject input the old
container silently accepted. That is usually the bug being found rather than
the bug being introduced, but it is a behaviour change and it belongs in the
commit message.

**How it is held: a purpose-built gate.** No linter has this rule, because it
is a house choice rather than a defect. A gate that flags `import dataclasses`
and `import attrs` outside an exceptions table is about thirty lines and is
the only thing that keeps the rule from decaying.

### 3. No function defined inside another function

A `def` inside a `def` is a defect by default. Lift it to module or class
level and pass what it needs as arguments.

**The failure it prevents.** Four distinct ones:

- **It cannot be tested.** No import reaches it, so the only way to exercise
  it is through its parent, with whatever setup the parent demands.
- **It is invisible.** A reader scanning the module for what it contains does
  not see it. Neither does grep for a call site, since the only call site is
  inside the parent.
- **It is rebuilt on every call.** A closure defined inside a hot function is
  re-created per invocation. This is cheap, but it is never free, and in a
  per-batch or per-row path it stops being cheap.
- **It hides its inputs.** The closure reads the parent's locals, so its real
  signature is invisible. Changing a local three lines up silently changes
  what the nested function computes. This is the one that produces the
  genuinely surprising bugs.

**The three sanctioned exceptions.** Each needs a reason on the line, because
the point of naming exceptions is to stop them being argued case by case:

1. **A decorator that closes over its own arguments.** `def deco(n):` returning
   an inner `wrapper` is the standard shape and there is no non-nested way to
   write it.
2. **A callback whose closure over local state IS the point** — a signal
   handler bound to a per-call timeout, a predicate handed to a shrink loop, a
   comparator built from a locally computed key. The test is whether passing
   the state explicitly would be more code and less clear, not whether it
   would be possible.
3. **`functools.wraps` wrappers**, which are case 1 by another name.

Everything else lifts. A formatter that closes over a computed column width
takes the width as an argument. A recursive walker takes its accumulator as an
argument. Both get shorter and both become testable.

**How it is held: a purpose-built gate** with an inline pragma for the three
exceptions, in the same shape as a `# noqa: RULE — reason` comment. Ruff has
no rule for this. The pragma must require a non-empty reason, or it becomes a
silent opt-out.

### 4. Names carry the work

A function is named as a verb phrase a reader can check against the body:
`resolve_partition_spec`, not `handle`, `process`, `do_work`, `helper` or
`_inner`. A name that would fit any function in the module is not a name.

Alongside it, the general naming rules: spell domain concepts out rather than
abbreviating (`configuration` not `cfg`, `index` not `idx`); acronyms only
when universally understood; no single-letter names outside bounded numeric
loops; booleans read as questions (`is_valid`, `has_expired`); verbs for
functions, nouns for values, plurals for collections.

**The failure it prevents.** Bad names cost more than bad logic, because bad
logic is local and a bad name spreads through every call site that adopts its
vocabulary. A codebase that calls the same concept three things has three
concepts as far as the next reader is concerned.

**How it is held: review.** No tool can judge whether a name means anything,
and pretending otherwise produces a gate that flags `id` and misses
`process_data`. This is the rule that needs a human, which is exactly why the
other three are worth automating: reviewer attention is finite, and it should
be spent on the rule that cannot be spent anywhere else.

---

## The general rules the four sit inside

- **One responsibility per function.** If you cannot describe it in a sentence
  without "and", it does too much. Function length is a symptom, not the
  disease: target something like 100 lines, but extract when the name of the
  extracted thing makes the caller easier to read, not to hit a number.
- **Iterate by default.** Recursion is permitted when the structure is
  genuinely recursive, the depth is bounded, and the iterative version would
  be materially harder to read. Any hierarchy built from user-influenced input
  needs a depth limit or an explicit stack, because unbounded recursion over
  external data is a denial-of-service vector rather than a style question.
- **Make illegal states unrepresentable.** `Literal` types and discriminated
  models over free strings and parallel booleans; validate untrusted input at
  the boundary so the typed object is trusted everywhere inside.
- **Fail loudly, never silently.** Never catch bare `Exception` unless you
  immediately re-raise or log the full traceback.
- **Log, do not print.** `logging` for anything that runs unattended, with
  structured fields at boundaries and decision points. Never log secrets or
  personal data.
- **`pathlib.Path` over string paths; f-strings over `%` and `.format()`.**

---

## Arming a rule: the decision, and the ratchet

A convention becomes a gate when three things are true: it is mechanically
decidable, its violations are cheap to fix, and it will otherwise be violated
faster than review catches it. If a rule fails the first test, it stays a
review duty and the contract says so out loud.

An armed gate meets an existing codebase that already violates it. The pattern
that works is a **ratchet**, not a flag day:

1. **Measure first.** Count the violations per file with the same parser the
   gate will use. An estimate here produces an exceptions table that is wrong
   on day one.
2. **Seed an exceptions table from the measurement**, keyed by path, carrying
   a per-file ceiling and a reason. The reason states what the code is and
   what the fix would be, so that the row reads as debt rather than as
   sanction.
3. **Ceilings ratchet DOWN only.** Raising one needs a stated reason in the
   commit that raises it. That duty is a convention, not a check, because no
   table can tell whether a reason string is real.
4. **Delete a row rather than zeroing it** when its file converts. A table of
   zeroes is a table nobody reads.
5. **Fail closed on every ambiguity.** An unreadable file, a file that will not
   parse, an empty scan set, and an exceptions key pointing at a path that no
   longer exists are all errors. A guard that silently scans nothing is worse
   than no guard, because it reports success.
6. **Dual-wire it.** A gate wired only into the local `make` target does not
   run in CI; a gate wired only into CI does not run before the commit. Wire
   both, and say in each place that the other exists.

**Prove the gate catches things before trusting it.** Introduce each violation
class deliberately, confirm the gate goes red, confirm the pragma path goes
green, confirm an empty pragma reason goes red, and confirm a stale exceptions
key goes red. A gate that has only ever been observed passing is a gate whose
detection has never been tested.

---

## When these rules meet existing code

The rules apply to code as it is written and to code as it is touched. They do
not license a tree-wide refactor on their own, and a pure-refactor sweep is a
real risk: it changes working code that the test suite was never written to
pin. Lifting a closure changes what it can see. Converting a container adds
validation that was not running. Renaming a function moves every call site.

The invariant for such a sweep is the one worth stating in the charter: **no
call that worked before returns a different value.** If a change cannot meet
that bar, it is a behaviour change wearing a cleanup's clothes, and it belongs
in its own commit with its own justification.
