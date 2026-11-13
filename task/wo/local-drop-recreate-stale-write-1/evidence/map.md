# task/wo/local-drop-recreate-stale-write-1/evidence/ — the measured matrix

The before and after evidence for LOCAL-DROP-RECREATE-STALE-WRITE-1, scrubbed of machine paths.

- [table.md](table.md) — every matrix row on main `ae247767` and on this unit's build: session read, path read and the current file's uuid; the Spark 4.1.2 leg on the six wrong-write rows.
- [cells_repark.py](cells_repark.py) — the RePark matrix probe, one session and warehouse per cell (root and output as arguments).
- [spark_read.py](spark_read.py) — the Spark leg: reads each wrong-write row's table directory by path (cells file and jar as arguments).
- [provenance.md](provenance.md) — revs, build commands, interpreters and the pin's red and green counts.

Up: [../map.md](../map.md).
