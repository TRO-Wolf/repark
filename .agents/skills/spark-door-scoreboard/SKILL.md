---
name: spark-door-scoreboard
description: >-
  Measure the whole Spark door against Spark 4.1.2 on the recorded audit
  corpus: replay a RePark package against the saved Spark answers, score every
  comparable cell by class, and compare two scoreboards cell by cell to list
  what moved onto Spark and what moved off it. Reach for it to put a measured
  number on Spark-door parity, before and after a fix campaign, or to check
  that a branch moves no cell off Spark across the whole corpus. Do not use it
  for Iceberg catalogs, AWS, performance, or one surface's audit; a single
  surface goes to audit-repark-parity.
---

# Spark-door scoreboard

A whole-door measurement of the Spark door against Spark 4.1.2. The corpus is
the 2026-11-21 outside audit's: 51 lanes of cell definitions with their
recorded Spark answers. A replay runs only the RePark half, so it needs a
built package and the Spark 4.1.2 interpreter's libraries, but no Spark run.

Each comparable cell lands in one class: `EQUAL`, `WRONG-TYPE`, `WRONG-VALUE`,
`NULLABILITY-ONLY`, `BOTH-REFUSED`, `REFUSED-WHERE-SPARK-ANSWERS` or
`ACCEPTED-WHERE-SPARK-REFUSED`. The scoreboard files hold the counts; this
skill holds the sequence.
