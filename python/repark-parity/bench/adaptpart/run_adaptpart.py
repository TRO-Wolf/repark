"""ADAPT-PART AP-0 measurement: P-2 candidates and P-3 scores read off manifest bounds."""

from __future__ import annotations

import argparse
import datetime as dt
import math
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from repark import ReparkSession

BASE_US = 1672531200_000000
SPAN_US = 730 * 86400 * 1_000_000
GROUP_COUNT = 20
BUCKET_WIDTHS = (8, 16, 32, 64, 128)
DISTINCT_LIMIT = 1000
TRUNCATE_GRAINS = ("year", "month", "day", "hour")


class SampleNote(BaseModel):
    """One bounded head sample the run took, and the distinct count it found."""

    column: str
    distinct: int
    limit: int


class CandidateScore(BaseModel):
    """One ranked P-2 candidate with its P-3 score and its file projections."""

    candidate: str
    score: float
    partitions: int
    proj_files: int
    note: str = ""


class BedReport(BaseModel):
    """Everything the AP-0 document needs for one measured bed."""

    bed: str
    table: str
    rows: int
    files: int
    total_bytes: int
    target: int
    sample_limit: int
    partitions_rows: int
    partitions_files: int
    partitions_bytes: int
    partitions_has_column: bool
    constant_columns: list[str]
    sampled: list[SampleNote]
    null_columns: list[str]
    notes: list[str]
    candidates: list[CandidateScore]


class _ManifestFile(BaseModel):
    """One files-metadata row: its byte size, raw bound pairs, null flags."""

    size: int
    bounds: dict[str, list[Any] | None] = {}
    nulls: dict[str, bool] = {}


class _FileRange(BaseModel):
    """One file's normalized bound pair for a single candidate column."""

    size: int
    low: Any = None
    high: Any = None


class _Rated(BaseModel):
    """One candidate column with normalized per-file ranges and identity domain."""

    name: str
    kind: str
    ranges: list[_FileRange] = []
    domain: list[Any] = []
    constant: bool = False


class _PartitionsInfo(BaseModel):
    """One partitions-metadata snapshot: the manifest-side census of a bed."""

    records: int
    files: int
    bytes: int
    has_column: bool


class _Spec(BaseModel):
    """One P-2 candidate spec before scoring."""

    column: str = ""
    grain: str = ""
    width: int = 0


def _epoch_seconds(value: dt.datetime) -> int:
    """Map one bound timestamp to whole epoch seconds, naive read as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return int(value.timestamp())


def _day_ordinal(value: dt.date | dt.datetime) -> int:
    """Map one bound date to its proleptic ordinal day number."""
    if isinstance(value, dt.datetime):
        return value.date().toordinal()
    if isinstance(value, dt.date):
        return value.toordinal()
    return int(value)


def _norm_bound(value: Any, kind: str) -> Any:
    """Map one raw bound to its comparison domain, or None when missing."""
    if value is None:
        return None
    if kind == "ts" and isinstance(value, dt.datetime):
        return _epoch_seconds(value)
    if kind == "date" and isinstance(value, dt.date | dt.datetime):
        return _day_ordinal(value)
    if kind == "int":
        return int(value)
    if kind == "str":
        return str(value)
    return None


def _truncate_value(value: int, kind: str, grain: str) -> int:
    """Map one epoch second or ordinal day to its truncate bucket number."""
    moment = dt.datetime.fromtimestamp(value, tz=dt.UTC) if kind == "ts" else None
    day = moment.date() if moment is not None else dt.date.fromordinal(value)
    if grain == "year":
        return day.year
    if grain == "month":
        return day.year * 12 + day.month - 1
    if grain == "day":
        return day.toordinal()
    return value // 3600 if kind == "ts" else value * 24


def _column_kind(field: pa.Field) -> str:
    """Classify one Arrow field into a P-2 column kind, else other."""
    field_type = field.type
    if pa.types.is_timestamp(field_type):
        return "ts"
    if pa.types.is_date(field_type):
        return "date"
    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return "str"
    if pa.types.is_integer(field_type):
        return "int"
    return "other"


def _spec_label(spec: _Spec) -> str:
    """Render one candidate spec in Iceberg transform spelling."""
    if spec.grain == "none":
        return "none"
    if spec.grain == "bucket":
        return f"bucket({spec.width}, {spec.column})"
    return f"{spec.grain}({spec.column})"


def _uniform_schedule(batches: int) -> list[str]:
    """Assign batches to groups round-robin for the uniform bed."""
    return [f"g{batch % GROUP_COUNT:02d}" for batch in range(batches)]


def _skewed_schedule(batches: int) -> list[str]:
    """Assign even batches to g00 and odd batches cycling g01..g19."""
    odd = [f"g{(index % (GROUP_COUNT - 1)) + 1:02d}" for index in range(batches // 2)]
    positions = iter(odd)
    return ["g00" if batch % 2 == 0 else next(positions) for batch in range(batches)]


def _write_seed(seed_dir: Path, schedule: list[str], batch_rows: int) -> Path:
    """Write one seed parquet holding every batch with its group label."""
    seed_dir.mkdir(parents=True, exist_ok=True)
    total = len(schedule) * batch_rows
    batch_ids: list[int] = []
    stamps: list[int] = []
    groups: list[str] = []
    ids: list[int] = []
    for batch, group in enumerate(schedule):
        base = batch * batch_rows
        for row in range(batch_rows):
            batch_ids.append(batch)
            stamps.append((base + row) * SPAN_US // total + BASE_US)
            groups.append(group)
            ids.append(base + row)
    table = pa.table(
        {
            "b": pa.array(batch_ids, type=pa.int32()),
            "ts": pa.array(stamps, type=pa.timestamp("us")),
            "grp": pa.array(groups),
            "id": pa.array(ids, type=pa.int64()),
        }
    )
    seed = seed_dir / "seed.parquet"
    pq.write_table(table, seed)
    return seed


def _build_futures(session: ReparkSession, table: str, parquet: Path) -> None:
    """CTAS the unpartitioned futures bed straight from the source parquet."""
    session.read.parquet(str(parquet)).createOrReplaceTempView("ap0_fut")
    session.sql(f"CREATE TABLE {table} USING iceberg AS SELECT * FROM ap0_fut").collect()


def _build_synthetic(session: ReparkSession, table: str, view: str, batches: int) -> None:
    """Create one empty table, then append one data file per batch number."""
    session.sql(
        f"CREATE TABLE {table} (ts TIMESTAMP, grp STRING, id BIGINT) USING iceberg"
    ).collect()
    for batch in range(batches):
        session.sql(
            f"INSERT INTO {table} SELECT ts, grp, id FROM {view} WHERE b = {batch}"
        ).collect()


def _read_manifest_files(session: ReparkSession, table: str) -> list[_ManifestFile]:
    """Read every live data file row off the files metadata table."""
    arrow = session.sql(f"SELECT * FROM {table}.files").to_arrow()
    found: list[_ManifestFile] = []
    for row in arrow.to_pylist():
        if int(row.get("content") or 0) != 0:
            continue
        metrics = row.get("readable_metrics") or {}
        bounds: dict[str, list[Any] | None] = {}
        nulls: dict[str, bool] = {}
        for name, entry in metrics.items():
            if not isinstance(entry, dict):
                bounds[name] = None
                nulls[name] = False
                continue
            bounds[name] = [entry.get("lower_bound"), entry.get("upper_bound")]
            nulls[name] = int(entry.get("null_value_count") or 0) > 0
        found.append(_ManifestFile(size=int(row["file_size_in_bytes"]), bounds=bounds, nulls=nulls))
    return found


def _read_partitions_info(session: ReparkSession, table: str) -> _PartitionsInfo:
    """Read the manifest-side census off the partitions metadata table."""
    arrow = session.sql(f"SELECT * FROM {table}.partitions").to_arrow()
    rows = arrow.to_pylist()
    first = rows[0] if rows else {}
    records = (
        int(first.get("record_count") or 0)
        if len(rows) == 1
        else sum(int(r.get("record_count") or 0) for r in rows)
    )
    return _PartitionsInfo(
        records=records,
        files=int(first.get("file_count") or 0) if len(rows) == 1 else 0,
        bytes=int(first.get("total_data_file_size_in_bytes") or 0),
        has_column="partition" in arrow.schema.names,
    )


def _sample_column(session: ReparkSession, table: str, column: str, limit: int) -> list[Any]:
    """Read one bounded head sample of a single data column."""
    arrow = session.sql(f'SELECT "{column}" FROM {table} LIMIT {limit}').to_arrow()
    return [value for value in arrow.column(column).to_pylist() if value is not None]


def _rate_column(files: list[_ManifestFile], name: str, kind: str) -> _Rated:
    """Normalize one column's per-file bound pairs into its comparison domain."""
    rated = _Rated(name=name, kind=kind)
    for found in files:
        pair = found.bounds.get(name)
        if pair is None or pair[0] is None or pair[1] is None:
            rated.ranges.append(_FileRange(size=found.size))
            continue
        low = _norm_bound(pair[0], kind)
        high = _norm_bound(pair[1], kind)
        if low is None or high is None:
            rated.ranges.append(_FileRange(size=found.size))
            continue
        rated.ranges.append(_FileRange(size=found.size, low=low, high=high))
    return rated


def _penalty(value: float, target: int) -> float:
    """Score one projected partition size against the P-3 target band."""
    low = 0.25 * target
    high = 4.0 * target
    return max(0.0, (low - value) / low) + max(0.0, (value - high) / high)


def _accumulate(items: list[tuple[int, list[Any]]], target: int) -> tuple[float, int, int]:
    """Fold per-file 1/k byte spreads into a score with partition projections."""
    totals: dict[Any, float] = {}
    for size, values in items:
        share = size / len(values)
        for value in values:
            totals[value] = totals.get(value, 0.0) + share
    score = 0.0
    proj = 0
    for amount in totals.values():
        score += _penalty(amount, target)
        proj += math.ceil(amount / target)
    return score, len(totals), proj


def _truncate_items(rated: _Rated, grain: str) -> tuple[list[tuple[int, list[Any]]], int]:
    """Expand one temporal column into per-file truncate value lists."""
    present = [
        (item.low, item.high)
        for item in rated.ranges
        if item.low is not None and item.high is not None
    ]
    if not present:
        return [(item.size, [0]) for item in rated.ranges], len(rated.ranges)
    glo = min(low for low, _ in present)
    ghi = max(high for _, high in present)
    first = _truncate_value(glo, rated.kind, grain)
    last = _truncate_value(ghi, rated.kind, grain)
    full = list(range(first, last + 1))
    items: list[tuple[int, list[Any]]] = []
    fallback = 0
    for item in rated.ranges:
        if item.low is None or item.high is None:
            items.append((item.size, full))
            fallback += 1
            continue
        start = max(_truncate_value(item.low, rated.kind, grain), first)
        stop = min(_truncate_value(item.high, rated.kind, grain), last)
        if stop < start:
            items.append((item.size, full))
            fallback += 1
            continue
        items.append((item.size, list(range(start, stop + 1))))
    return items, fallback


def _identity_items(rated: _Rated) -> tuple[list[tuple[int, list[Any]]], int]:
    """Expand one low-cardinality column into per-file identity value lists."""
    items: list[tuple[int, list[Any]]] = []
    fallback = 0
    for item in rated.ranges:
        if item.low is None or item.high is None:
            items.append((item.size, list(rated.domain)))
            fallback += 1
            continue
        inside = [value for value in rated.domain if item.low <= value <= item.high]
        if not inside:
            items.append((item.size, list(rated.domain)))
            fallback += 1
            continue
        items.append((item.size, inside))
    return items, fallback


def _single_items(
    spec: _Spec, rated: _Rated, sizes: list[int], total: int, target: int
) -> tuple[CandidateScore, list[tuple[int, list[Any]]]]:
    """Score one single-field P-2 candidate and keep its per-file value lists."""
    label = _spec_label(spec)
    if spec.grain == "none":
        items = [(size, [0]) for size in sizes]
        score, partitions, proj = _accumulate(items, target)
        cand = CandidateScore(candidate=label, score=score, partitions=partitions, proj_files=proj)
        return cand, items
    if spec.grain == "bucket":
        items = [(size, list(range(spec.width))) for size in sizes]
        score, partitions, proj = _accumulate(items, target)
        note = "uniform-hash spread over N buckets"
        cand = CandidateScore(
            candidate=label, score=score, partitions=partitions, proj_files=proj, note=note
        )
        return cand, items
    if spec.grain == "identity":
        items, fallback = _identity_items(rated)
    else:
        items, fallback = _truncate_items(rated, spec.grain)
    score, partitions, proj = _accumulate(items, target)
    note = f"{fallback} files spread uniformly" if fallback else ""
    return CandidateScore(
        candidate=label, score=score, partitions=partitions, proj_files=proj, note=note
    ), items


def analyze_bed(
    session: ReparkSession, bed: str, table: str, target: int, sample_limit: int
) -> BedReport:
    """Score every P-2 candidate for one bed from its manifest bounds."""
    schema = session.sql(f"SELECT * FROM {table} LIMIT 0").to_arrow().schema
    kinds = {field.name: _column_kind(field) for field in schema}
    columns = [name for name, kind in kinds.items() if kind in ("ts", "date", "str", "int")]
    files = _read_manifest_files(session, table)
    total = sum(found.size for found in files)
    part = _read_partitions_info(session, table)
    null_columns = sorted({name for found in files for name, flag in found.nulls.items() if flag})
    rated: dict[str, _Rated] = {}
    sampled: list[SampleNote] = []
    constant: list[str] = []
    skipped: set[str] = set()
    notes: list[str] = []
    for name in columns:
        kind = kinds[name]
        item = _rate_column(files, name, kind)
        rated[name] = item
        if kind in ("ts", "date"):
            continue
        present = [
            (entry.low, entry.high)
            for entry in item.ranges
            if entry.low is not None and entry.high is not None
        ]
        if (
            len(present) == len(item.ranges)
            and present
            and all(low == high == present[0][0] for low, high in present)
        ):
            item.constant = True
            item.domain = [present[0][0]]
            constant.append(name)
            continue
        values = _sample_column(session, table, name, sample_limit)
        distinct = sorted(set(values))
        sampled.append(SampleNote(column=name, distinct=len(distinct), limit=sample_limit))
        if len(distinct) > DISTINCT_LIMIT:
            notes.append(
                f"{name}: {len(distinct)} sampled distinct values exceed 1000, bucket branch"
            )
            continue
        if not distinct:
            notes.append(f"{name}: empty sample domain, column skipped")
            skipped.add(name)
            continue
        item.domain = distinct
    specs: list[_Spec] = [_Spec(column="", grain="none")]
    for name in columns:
        if name in skipped:
            continue
        kind = kinds[name]
        if kind in ("ts", "date"):
            specs.extend(_Spec(column=name, grain=grain) for grain in TRUNCATE_GRAINS)
        elif rated[name].constant or rated[name].domain:
            specs.append(_Spec(column=name, grain="identity"))
        elif kind in ("str", "int"):
            specs.extend(_Spec(column=name, grain="bucket", width=width) for width in BUCKET_WIDTHS)
    scored: list[tuple[_Spec, CandidateScore, list[tuple[int, list[Any]]]]] = []
    sizes = [found.size for found in files]
    for spec in specs:
        if spec.grain == "none":
            cand, items = _single_items(spec, _Rated(name="", kind=""), sizes, total, target)
        else:
            cand, items = _single_items(spec, rated[spec.column], sizes, total, target)
        scored.append((spec, cand, items))
    best: dict[str, tuple[float, str, _Spec]] = {}
    for spec, cand, _ in scored:
        if spec.grain == "none":
            continue
        current = best.get(spec.column)
        if current is None or (cand.score, cand.candidate) < (current[0], current[1]):
            best[spec.column] = (cand.score, cand.candidate, spec)
    fields = sorted(best.items(), key=lambda entry: (entry[1][0], entry[0]))[:3]
    pairs = [
        (fields[i][1][2], fields[j][1][2])
        for i in range(len(fields))
        for j in range(i + 1, len(fields))
    ]
    pair_rows: list[CandidateScore] = []
    for first, second in pairs:
        label = f"{_spec_label(first)}+{_spec_label(second)}"
        left = next(items for spec, _, items in scored if _spec_label(spec) == _spec_label(first))
        right = next(items for spec, _, items in scored if _spec_label(spec) == _spec_label(second))
        combined = [
            (size, [(a, b) for a in mine for b in other])
            for (size, mine), (_, other) in zip(left, right, strict=True)
        ]
        score, partitions, proj = _accumulate(combined, target)
        pair_rows.append(
            CandidateScore(
                candidate=label,
                score=score,
                partitions=partitions,
                proj_files=proj,
                note="cross-product spread",
            )
        )
    ranked = sorted(
        [cand for _, cand, _ in scored] + pair_rows,
        key=lambda cand: (cand.score, cand.proj_files, cand.candidate),
    )
    return BedReport(
        bed=bed,
        table=table,
        rows=part.records,
        files=len(files),
        total_bytes=total,
        target=target,
        sample_limit=sample_limit,
        partitions_rows=part.records,
        partitions_files=part.files,
        partitions_bytes=part.bytes,
        partitions_has_column=part.has_column,
        constant_columns=sorted(constant),
        sampled=sampled,
        null_columns=null_columns,
        notes=notes,
        candidates=ranked,
    )


def format_bed_report(report: BedReport) -> str:
    """Render one bed's facts and its ranked candidate table as text."""
    constant = ", ".join(report.constant_columns) if report.constant_columns else "none"
    sampled = "; ".join(f"{note.column} distinct={note.distinct}" for note in report.sampled)
    nulls = ", ".join(report.null_columns) if report.null_columns else "none"
    lines = [
        f"== bed {report.bed} ({report.table}) ==",
        f"rows {report.rows} files {report.files} bytes {report.total_bytes}",
        f"target {report.target} sample_rows {report.sample_limit}",
        f"partitions table: rows {report.partitions_rows}",
        f"partitions table: file_count {report.partitions_files}",
        f"partitions table: bytes {report.partitions_bytes}",
        f"partitions table: partition_column {report.partitions_has_column}",
        f"constant from bounds (no sample): {constant}",
        f"sampled: {sampled if sampled else 'none'}",
        f"nulls observed: {nulls}",
    ]
    lines.extend(f"note: {text}" for text in report.notes)
    width = max([len(cand.candidate) for cand in report.candidates] + [9])
    head = f"{'rank':>4} {'candidate':<{width}}"
    lines.append(head + f" {'score':>12} {'partitions':>10} {'proj_files':>10} note")
    for rank, cand in enumerate(report.candidates, start=1):
        row = f"{rank:>4} {cand.candidate:<{width}}"
        row += f" {cand.score:>12.3f} {cand.partitions:>10} {cand.proj_files:>10}"
        lines.append(row + f" {cand.note}" if cand.note else row)
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    """Build the AP-0 command line."""
    parser = argparse.ArgumentParser(description="ADAPT-PART AP-0 partition-candidate measurement")
    parser.add_argument(
        "--scratch", type=Path, required=True, help="scratch root for warehouse and seeds"
    )
    parser.add_argument(
        "--target-file-size-bytes", type=int, default=524288, help="P-3 target file size in bytes"
    )
    parser.add_argument(
        "--sample-rows", type=int, default=200000, help="LIMIT for one-column distinct samples"
    )
    parser.add_argument("--batches", type=int, default=200, help="append batches per synthetic bed")
    parser.add_argument("--batch-rows", type=int, default=2000, help="rows per synthetic batch")
    parser.add_argument(
        "--futures-parquet",
        type=Path,
        default=Path.home() / "CodeRepos" / "myTemp" / "reTest" / "test_futures.parquet",
        help="source frame for the futures bed",
    )
    return parser


def main() -> int:
    """Build all three beds, score them, and print one ranked table per bed."""
    args = build_parser().parse_args()
    if not args.futures_parquet.is_file():
        print(f"missing futures source: {args.futures_parquet}")
        return 2
    scratch = args.scratch
    warehouse = scratch / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    session = ReparkSession.builder.appName("ap-0-measure").getOrCreate()
    try:
        session.register_memory_catalog("ap", str(warehouse))
        session.sql("CREATE NAMESPACE ap.ns")
        started = time.perf_counter()
        _build_futures(session, "ap.ns.futures", args.futures_parquet)
        futures_seconds = time.perf_counter() - started
        for bed, schedule in (
            ("uniform", _uniform_schedule(args.batches)),
            ("skewed", _skewed_schedule(args.batches)),
        ):
            seed = _write_seed(scratch / f"seed_{bed}", schedule, args.batch_rows)
            session.read.parquet(str(seed)).createOrReplaceTempView(f"ap0_{bed}")
            started = time.perf_counter()
            _build_synthetic(session, f"ap.ns.{bed}", f"ap0_{bed}", len(schedule))
            print(f"built {bed}: {len(schedule)} batches in {time.perf_counter() - started:.1f}s")
        print(f"built futures: CTAS in {futures_seconds:.1f}s")
        print(
            f"target_file_size_bytes {args.target_file_size_bytes} sample_rows {args.sample_rows}"
        )
        for bed in ("futures", "uniform", "skewed"):
            report = analyze_bed(
                session, bed, f"ap.ns.{bed}", args.target_file_size_bytes, args.sample_rows
            )
            print(format_bed_report(report))
            print("")
    finally:
        session.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
