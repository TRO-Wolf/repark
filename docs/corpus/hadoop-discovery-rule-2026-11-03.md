# Hadoop metadata discovery: the reference rule (2026-11-03)

CORPUS-1 output 2, the rule half. This is Iceberg Java's rule for finding a `type=hadoop` table's
current metadata, ported from the source at tag `apache-iceberg-1.11.0` (commit `6976e020`), the
version of the Iceberg jar the Spark 4.1.2 oracle on this box loads. Every line number below is in
that checkout. The cells that measure it are
[hadoop-discovery-cells-2026-11-03.jsonl](hadoop-discovery-cells-2026-11-03.jsonl); the inventory
that places it among the upstream tests is
[discovery-inventory-2026-11-03.md](discovery-inventory-2026-11-03.md).

## The files

- `core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java` (HTO below)
- `core/src/main/java/org/apache/iceberg/TableMetadataParser.java` (TMP)
- `core/src/main/java/org/apache/iceberg/hadoop/HadoopCatalog.java` (HC)
- `core/src/main/java/org/apache/iceberg/hadoop/Util.java` (`VERSION_HINT_FILENAME = "version-hint.text"`, line 45)
- `core/src/main/java/org/apache/iceberg/CatalogUtil.java` (CU)

## R1. Metadata file names

A version `N` has three candidate names, probed in this order (HTO `getMetadataFile`, lines 237 to
255, over `Codec.values()`, TMP lines 50 to 53):

1. `vN.metadata.json` (codec NONE, `getFileExtension`, TMP line 144)
2. `vN.gz.metadata.json` (codec GZIP, the name Java's writer produces)
3. `vN.metadata.json.gz` (codec GZIP, the legacy name, `getOldFileExtension`, TMP line 148)

The first name that exists is the version's file. The codec used to read it is chosen from the
name, not from the bytes (`Codec.fromFileName`, TMP lines 69 to 80, used by `read` at line 298):
a gzip name over plain bytes fails to parse, and a plain name over gzip bytes fails to parse.

## R2. The hint read

`findVersion` (HTO lines 313 to 350) opens `metadata/version-hint.text`, reads its first line,
strips `\n` and calls `Integer.parseInt`.

- Any exception (missing file, unreadable file, a directory, an empty file whose `readLine` is
  null, a non-numeric value, a leading space, a trailing `\r` (corrected below), a value beyond Java `int`) is
  caught, and the rule falls to R3, provided `metadata/` exists. If `metadata/` does not exist
  the answer is version 0.
- A value that parses is returned as is, including `0` and negative numbers. A trailing `\n` is
  removed by `readLine`; `CRLF` leaves `\r`, which does not parse (corrected below).

Correction (2026-11-03, HADOOP-DISCOVERY-RULE-1 verifiers): `BufferedReader.readLine` strips `\r\n` and a lone `\r`, so a CRLF hint parses as the number; the earlier sentence was wrong.

## R3. The listing fallback

When the hint cannot be read (R2), HTO lists `metadata/` with `VERSION_PATTERN` =
`v([^\.]*)\..*` (line 59), parses each match's stem (`version`, lines 274 to 285; a stem that does
not parse is `-1`), and keeps the highest `N` for which R1 finds a file. A catalog-style
`NNNNN-<uuid>.metadata.json` does not match the pattern and is never a candidate. With no
candidate the version is 0.

## R4. The refresh walk

`refresh` (HTO lines 102 to 127) takes the version from R2 or R3, then:

- If R1 finds no file for it and the version is 0 on a first load, the table does not exist
  (`NoSuchTableException` at the door).
- If R1 finds no file for any other version, it raises `ValidationException("Metadata file for
  version %d is missing under %s")`. A hint past the newest file, a negative hint and a hint into
  a version that cleanup removed all land here.
- Otherwise it walks `N+1, N+2, …` while R1 finds a file, and the last one found is current. The
  walk stops at the first missing version, so a gap below the newest file hides every file above
  it from a hinted read, while the listing (R3) still reaches the newest.

## R5. The writer side

`commit` (HTO lines 131 to 170) writes the new metadata to a temporary name, renames it to
`v(N+1)` with the codec from `write.metadata.compression-codec` (R1 name 1 or 2), then writes the
hint best-effort (`writeVersionHint`, lines 292 to 304, delete then rename; a failure only logs).
`deleteRemovedMetadataFiles` (CU lines 581 to 607) then deletes metadata files that fell out of
the metadata log when `write.metadata.delete-after-commit.enabled` is true, keeping
`write.metadata.previous-versions-max`.

## R6. Catalog listing

`HadoopCatalog.listTables` (HC line 190) lists a directory as a table only when `isTableDir`
(lines 156 to 172) finds at least one file in its `metadata/` whose name ends `.metadata.json`
(`TABLE_FILTER`, lines 84 to 87). A table whose files all carry the legacy name
`vN.metadata.json.gz` loads by path but is not listed.

## What the measurement adds to the rule

- **Checksum sidecars.** Hadoop's local file system writes `.<name>.crc` beside every file it
  creates and verifies it on read. A fixture that rewrites `version-hint.text` and leaves
  `.version-hint.text.crc` stale makes Java's hint read throw, so the rule falls to R3: a stale
  hint then answers the newest version for the wrong reason. Every pilot fixture strips the
  sidecars; twenty control fixtures keep them, and the cells file marks those `crc_kept`. The
  second audit's cleanup-case measurement was this artefact
  ([parity matrix, AUD-010 line](../spark-sql-iceberg-parity.md)).
- **The door.** Spark reaches R1 to R4 through both its catalog (`hd.ns.t`) and the path read.
  RePark's `type=hadoop` catalog is the in-process memory catalog
  (ICE-LOCAL-COMMIT-POINT-1 in the parity matrix): it does not read tables already on disk, so
  on RePark only the path read applies this rule. The pilot measures the catalog doors on control
  fixtures to record that.
