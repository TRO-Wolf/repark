# task/wo/string-hour-minute-second-1/evidence/ — probes, verdicts, records, `javap` excerpts

The probes carry no comments and take every path as an argument. Raw run files (per-part answers, logs, every
`repark._native.__file__` print, the 75,032-row `verdicts.jsonl`) stay in `N/string-hms-out/` (`raw/`, `logs/`).

- [javap.md](javap.md) — the `javap -c -p` lines Spark's rule rests on (builders, `GetTimeField` /
  `GetDateField` input types, both coercions' `STRING → AnyTimestampType` arm, the implicit `Cast`'s mode, the zone
  of the field, `extract` / `date_part`, the neighbours' input types).
- [probes/](probes/map.md) — `cells.py`, `probe.py`, `cost.py`, `cmp.py`.
- [verdicts/](verdicts/map.md) — `cmp.py`'s counts and the card's per-cell table.
- [raw/](raw/map.md) — the `EXPLAIN` records of both engines and main's cost record.

Provenance (lane A, under `flock -s` of the measure lock, `nice -n 10 taskset -c 32-47 prlimit --as=51539607552`,
Spark through `spark_slot.sh`): `probe.py spark <wh> <pq> <out> setup`, then for each configuration
`<cfg>:proj:<input>`, `<cfg>:g`, and `a1-la:explain`, on Spark 4.1.2 (`JAVA_HOME` zulu 17, the Iceberg 1.11.0
runtime jar); `probe.py main <wh> <pq> <out> <cfg>:proj` (and `:g`, `:explain`, the NTZ `:proj:LI`) with
`PYTHONPATH=S/ddu-work/pkg-main-4c365814` and `S/lane-nan-write-canonical-1/.venv/bin/python`; `cost.py <wh> <out> 9`;
`cmp.py N/string-hms-out/raw verdicts N/string-hms-out/verdicts.jsonl`.

Up: [../map.md](../map.md).
