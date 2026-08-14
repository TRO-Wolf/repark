"""r25 T4 — smart CSV ingest + type-inference PROTOCOL (Python facade).

# === r25 T4: csv-smart ===

Implements the greylit Q1 ladder as a **documented protocol**, not shared Rust:

    bool → int32 → int64 → decimal128 → float64 → date → timestamp → string

Every failure falls back one rung; terminal is string; resolution never errors; deterministic.
See ``task/t4-csv-smart-ledger.md`` and claim-board ``inference-protocol.md``.

``session.read.smartCsv`` is a disclosed RePark extension.
Inference sampling: default full scan up to 10_000 data rows, else first
10_000 (override with ``samplingRows``); data materialisation always uses
the full file. Default ``spark.read.csv`` is untouched (r20-R1 Spark-parity
pins stand).
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

# ==================================================================================================
# Protocol constants
# ==================================================================================================

# Rung order: least general → most general (terminal last).
RUNG_ORDER: tuple[str, ...] = (
    "bool",
    "int32",
    "int64",
    "decimal128",
    "float64",
    "date",
    "timestamp",
    "string",
)

_RUNG_INDEX: dict[str, int] = {name: index for index, name in enumerate(RUNG_ORDER)}

# Bool tokens — deliberately exclude 1/0 so pure integer id columns stay int.
_BOOL_TRUE: frozenset[str] = frozenset({"true", "t"})
_BOOL_FALSE: frozenset[str] = frozenset({"false", "f"})

_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_DECIMAL_MAX_PRECISION = 38

# Greylit r26 Q4: inference-only row budget (full file still read for data).
DEFAULT_INFERENCE_SAMPLING_ROWS = 10_000

# ISO date / timestamp (protocol-stable subset).
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_TIMESTAMP_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?:Z)?$"
)

# Integer (optional sign) / decimal with `.` / euro-style comma decimal / scientific.
_INT_RE = re.compile(r"^[+-]?\d+$")
_DECIMAL_DOT_RE = re.compile(r"^[+-]?(?:\d+\.\d*|\.\d+)$")
_DECIMAL_COMMA_RE = re.compile(r"^[+-]?(?:\d+,\d+|,\d+)$")
_SCI_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)[eE][+-]?\d+$")

_DEFAULT_NULL_TOKENS: frozenset[str] = frozenset({"", "null", "none", "na", "n/a", "nan"})


# ==================================================================================================
# Diagnostics report
# ==================================================================================================


@dataclass
class ColumnIngestReport:
    """Per-column inference diagnostics (surfaceable; no silent magic)."""

    name: str
    resolved_type: str
    fallback_count: int
    null_count: int
    sample_count: int
    decimal_precision: int | None = None
    decimal_scale: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly dict for :meth:`DataFrame.describe_ingest`."""
        payload: dict[str, Any] = {
            "name": self.name,
            "resolved_type": self.resolved_type,
            "fallback_count": self.fallback_count,
            "null_count": self.null_count,
            "sample_count": self.sample_count,
        }
        if self.decimal_precision is not None:
            payload["decimal_precision"] = self.decimal_precision
        if self.decimal_scale is not None:
            payload["decimal_scale"] = self.decimal_scale
        return payload


@dataclass
class IngestReport:
    """Full smartCsv ingest diagnostics."""

    source: str = "smartCsv"
    path: str = ""
    skipped_lines: int = 0
    header_row_index: int | None = None
    delimiter: str = ","
    bom_stripped: bool = False
    ragged_rows_padded: int = 0
    header_normalized: bool = False
    null_tokens: list[str] = field(default_factory=list)
    columns: list[ColumnIngestReport] = field(default_factory=list)
    synthesized_headers: bool = False
    data_row_count: int = 0
    # r26 Q4 sampling diagnostics (inference-only; data read is always full file).
    inference_rows_scanned: int = 0
    inference_capped: bool = False
    sampling_rows_limit: int = DEFAULT_INFERENCE_SAMPLING_ROWS

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly dict for :meth:`DataFrame.describe_ingest`."""
        return {
            "source": self.source,
            "path": self.path,
            "skipped_lines": self.skipped_lines,
            "header_row_index": self.header_row_index,
            "delimiter": self.delimiter,
            "bom_stripped": self.bom_stripped,
            "ragged_rows_padded": self.ragged_rows_padded,
            "header_normalized": self.header_normalized,
            "null_tokens": list(self.null_tokens),
            "synthesized_headers": self.synthesized_headers,
            "data_row_count": self.data_row_count,
            "inference_rows_scanned": self.inference_rows_scanned,
            "inference_capped": self.inference_capped,
            "sampling_rows_limit": self.sampling_rows_limit,
            "columns": [column.to_dict() for column in self.columns],
        }


# ==================================================================================================
# Cell parse / rung try
# ==================================================================================================


def is_null_token(raw: str | None, null_tokens: frozenset[str] | set[str]) -> bool:
    """Return True when ``raw`` is missing or matches a configured null token (case-fold)."""
    if raw is None:
        return True
    stripped = raw.strip()
    return stripped.casefold() in null_tokens


def try_bool(raw: str) -> bool | None:
    """Parse protocol bool tokens; ``None`` on failure."""
    token = raw.strip().casefold()
    if token in _BOOL_TRUE:
        return True
    if token in _BOOL_FALSE:
        return False
    return None


def try_int32(raw: str) -> int | None:
    """Parse signed int32; ``None`` on failure or out-of-range."""
    text = raw.strip()
    if not _INT_RE.match(text):
        return None
    try:
        value = int(text, 10)
    except ValueError:
        return None
    if value < _INT32_MIN or value > _INT32_MAX:
        return None
    return value


def try_int64(raw: str) -> int | None:
    """Parse signed int64; ``None`` on failure or out-of-range."""
    text = raw.strip()
    if not _INT_RE.match(text):
        return None
    try:
        value = int(text, 10)
    except ValueError:
        return None
    if value < _INT64_MIN or value > _INT64_MAX:
        return None
    return value


def _decimal_from_text(text: str) -> tuple[Decimal, int, int] | None:
    """Parse a fixed-point decimal; return ``(value, precision, scale)`` or ``None``."""
    cleaned = text.strip()
    if _SCI_RE.match(cleaned):
        # Scientific notation is float64 territory (decimal fails; float rung next).
        return None
    normalized = cleaned
    if _DECIMAL_COMMA_RE.match(cleaned) and "." not in cleaned:
        # Euro-style single comma as decimal separator (no thousands grouping).
        normalized = cleaned.replace(",", ".")
    elif not (_INT_RE.match(cleaned) or _DECIMAL_DOT_RE.match(cleaned)):
        return None
    try:
        value = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None
    if not value.is_finite():
        return None
    # Decimal.as_tuple() → digits + exponent; scale = -exponent when exponent < 0.
    sign, digits, exponent = value.as_tuple()
    del sign  # unused — magnitude only
    digit_count = len(digits)
    if exponent >= 0:
        # Integer-valued decimal — int digits = digit_count + exp.
        scale = 0
        precision = digit_count + int(exponent)
    else:
        scale = -int(exponent)
        # Significant digits: max(digit_count, scale) covers leading frac zeros.
        precision = max(digit_count, scale)
    if precision == 0:
        precision = 1
    if precision > _DECIMAL_MAX_PRECISION:
        return None
    return value, precision, scale


def try_decimal128(raw: str) -> tuple[Decimal, int, int] | None:
    """Parse decimal128-capable fixed point; ``None`` on failure/overflow."""
    return _decimal_from_text(raw)


def try_float64(raw: str) -> float | None:
    """Parse IEEE-754 float64 (incl. scientific); ``None`` on failure/non-finite."""
    text = raw.strip()
    if not (
        _INT_RE.match(text)
        or _DECIMAL_DOT_RE.match(text)
        or _DECIMAL_COMMA_RE.match(text)
        or _SCI_RE.match(text)
    ):
        return None
    if _DECIMAL_COMMA_RE.match(text) and "." not in text:
        normalized = text.replace(",", ".")
    else:
        normalized = text
    try:
        value = float(normalized)
    except ValueError:
        return None
    # NaN / Inf are not protocol float64 — fall through to later rungs / string.
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return value


def try_date(raw: str) -> date | None:
    """Parse ISO date ``YYYY-MM-DD``; ``None`` on failure."""
    match = _DATE_RE.match(raw.strip())
    if match is None:
        return None
    year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    try:
        return date(year, month, day)
    except ValueError:
        return None


def try_timestamp(raw: str) -> datetime | None:
    """Parse ISO timestamp; ``None`` on failure."""
    match = _TIMESTAMP_RE.match(raw.strip())
    if match is None:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6))
    frac = match.group(7) or "0"
    micro = int(frac.ljust(6, "0")[:6])
    try:
        return datetime(year, month, day, hour, minute, second, micro)
    except ValueError:
        return None


def try_rung(rung: str, raw: str) -> Any | None:
    """Try parsing ``raw`` at ``rung``; return parsed value or ``None`` on failure."""
    if rung == "bool":
        return try_bool(raw)
    if rung == "int32":
        return try_int32(raw)
    if rung == "int64":
        return try_int64(raw)
    if rung == "decimal128":
        return try_decimal128(raw)
    if rung == "float64":
        return try_float64(raw)
    if rung == "date":
        return try_date(raw)
    if rung == "timestamp":
        return try_timestamp(raw)
    if rung == "string":
        return raw
    return None


def resolve_cell_rung(raw: str) -> str:
    """Return the least-general rung that accepts ``raw`` (terminal string always succeeds)."""
    for rung in RUNG_ORDER:
        if try_rung(rung, raw) is not None:
            return rung
    return "string"


# ==================================================================================================
# Column resolution
# ==================================================================================================


@dataclass
class ColumnResolution:
    """Resolved column type + diagnostics counters."""

    rung: str
    fallback_count: int
    null_count: int
    sample_count: int
    decimal_precision: int | None = None
    decimal_scale: int | None = None


def resolve_column_type(
    values: list[str | None],
    *,
    null_tokens: frozenset[str] | set[str] | None = None,
) -> ColumnResolution:
    """Resolve a column under the protocol ladder (deterministic).

    Starts at ``bool``; each non-null cell that fails the current rung forces a one-rung
    (or multi-rung) promotion to the cell's required rung. Terminal is ``string``.
    """
    tokens = frozenset(null_tokens) if null_tokens is not None else _DEFAULT_NULL_TOKENS
    current = "bool"
    fallback_count = 0
    null_count = 0
    sample_count = 0
    # Decimal union: precision = max(integer digits) + max(scale), not max(per-cell precision).
    # Integer digits = precision - scale from _decimal_from_text (0 for pure fractions like 0.001).
    max_int_digits = 0
    max_scale = 0
    saw_non_null = False

    for raw in values:
        if is_null_token(raw, tokens):
            null_count += 1
            continue
        # is_null_token already rejected None / null tokens; raw is a live cell string.
        cell_text = raw if raw is not None else ""
        sample_count += 1
        saw_non_null = True
        cell_rung = resolve_cell_rung(cell_text)
        if _RUNG_INDEX[cell_rung] > _RUNG_INDEX[current]:
            # Count how many rungs we step (each failure = one fallback step conceptually).
            fallback_count += _RUNG_INDEX[cell_rung] - _RUNG_INDEX[current]
            current = cell_rung
        # Track fixed-point envelope for EVERY cell that parses as decimal128
        # (integers parse as scale 0). Must not gate on current rung — integer cells
        # that appear *before* the first fractional cell still widen precision
        # (octo C3-Q-001 order-dependence bug).
        parsed = try_decimal128(cell_text)
        if parsed is not None:
            _value, precision, scale = parsed
            del _value
            int_digits = precision - scale
            if int_digits < 0:
                int_digits = 0
            max_int_digits = max(max_int_digits, int_digits)
            max_scale = max(max_scale, scale)

    if not saw_non_null:
        return ColumnResolution(
            rung="string",
            fallback_count=0,
            null_count=null_count,
            sample_count=0,
        )

    # Integer-looking values stored under decimal rung: if current is int32/int64, keep it.
    # Decimal union (greylit r26 Q5): scale = max_scale; precision = int_digits + scale;
    # floor precision >= max(1, scale); p > 38 → promote to float64.
    decimal_precision: int | None = None
    decimal_scale: int | None = None
    if current == "decimal128":
        decimal_scale = max_scale
        decimal_precision = max_int_digits + max_scale
        decimal_precision = max(decimal_precision, max(1, decimal_scale))
        if decimal_precision > _DECIMAL_MAX_PRECISION:
            current = "float64"
            decimal_precision = None
            decimal_scale = None
        else:
            decimal_precision = min(_DECIMAL_MAX_PRECISION, decimal_precision)

    return ColumnResolution(
        rung=current,
        fallback_count=fallback_count,
        null_count=null_count,
        sample_count=sample_count,
        decimal_precision=decimal_precision,
        decimal_scale=decimal_scale,
    )


def rung_to_spark_type(resolution: ColumnResolution) -> Any:
    """Map a :class:`ColumnResolution` to a ``repark.types`` DataType instance."""
    from repark.spark.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )

    rung = resolution.rung
    if rung == "bool":
        return BooleanType()
    if rung == "int32":
        return IntegerType()
    if rung == "int64":
        return LongType()
    if rung == "decimal128":
        precision = resolution.decimal_precision or 10
        scale = resolution.decimal_scale or 0
        return DecimalType(precision, scale)
    if rung == "float64":
        return DoubleType()
    if rung == "date":
        return DateType()
    if rung == "timestamp":
        return TimestampType()
    return StringType()


def rung_to_engine_cast(resolution: ColumnResolution) -> str:
    """Canonical engine cast string for :meth:`Column.cast`."""
    return rung_to_spark_type(resolution)._engine_type()


def rung_to_sql_cast(resolution: ColumnResolution) -> str:
    """SQL ``CAST(... AS <type>)`` token for :meth:`DataFrame.selectExpr`.

    Differs from :func:`rung_to_engine_cast` where the SQL parser rejects the engine
    alias (notably ``long`` → ``bigint``).
    """
    engine = rung_to_engine_cast(resolution)
    if engine == "long":
        return "bigint"
    if engine == "int":
        return "int"
    if engine == "boolean":
        return "boolean"
    if engine == "double":
        return "double"
    if engine == "date":
        return "date"
    if engine == "timestamp":
        return "timestamp"
    if engine.startswith("decimal"):
        return engine
    return "varchar"


# ==================================================================================================
# Messy CSV preprocessing
# ==================================================================================================


def _strip_bom(text: str) -> tuple[str, bool]:
    """Strip a leading UTF-8 BOM if present."""
    if text.startswith("\ufeff"):
        return text[1:], True
    return text, False


def _score_delimiter(lines: list[str], delimiter: str) -> tuple[int, int]:
    """Score delimiter consistency: (mode_field_count_or_0, agreement_rows).

    Higher agreement with field_count >= 2 wins. Returns (0, 0) when unusable.
    """
    counts: list[int] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = next(csv.reader([line], delimiter=delimiter))
        except csv.Error:
            continue
        counts.append(len(row))
    if not counts:
        return 0, 0
    # Mode field count.
    frequency: dict[int, int] = {}
    for count in counts:
        frequency[count] = frequency.get(count, 0) + 1
    mode_count = max(frequency, key=lambda key: (frequency[key], key))
    if mode_count < 2:
        return 0, 0
    agreement = frequency[mode_count]
    return mode_count, agreement


def detect_delimiter(lines: list[str], *, preferred: str | None = None) -> str:
    """Pick the delimiter with the strongest field-count consistency (deterministic order)."""
    if preferred is not None:
        return preferred
    candidates = (",", ";", "\t", "|")
    best = ","
    best_score = (-1, -1)  # (agreement, mode_fields)
    for candidate in candidates:
        mode_fields, agreement = _score_delimiter(lines, candidate)
        score = (agreement, mode_fields)
        if score > best_score:
            best_score = score
            best = candidate
    return best


def detect_preamble_skip(lines: list[str], delimiter: str) -> int:
    """Return how many leading lines to skip before a consistent tabular block.

    Scans for the first line whose field count matches the modal field count of the
    remaining non-empty lines (delimiter-consistency). Blank lines at the top count as skip.
    """
    non_empty_indices = [index for index, line in enumerate(lines) if line.strip()]
    if not non_empty_indices:
        return 0
    # Modal field count over all non-empty lines.
    field_counts: list[tuple[int, int]] = []  # (line_index, n_fields)
    for index in non_empty_indices:
        try:
            row = next(csv.reader([lines[index]], delimiter=delimiter))
        except csv.Error:
            continue
        field_counts.append((index, len(row)))
    if not field_counts:
        return 0
    frequency: dict[int, int] = {}
    for _index, count in field_counts:
        frequency[count] = frequency.get(count, 0) + 1
    # Prefer the highest agreement with count >= 2; tie-break larger count.
    mode_fields = max(
        (count for count in frequency if count >= 2),
        key=lambda count: (frequency[count], count),
        default=1,
    )
    if mode_fields < 2:
        return 0
    # First line that has mode_fields starts the table.
    for index, count in field_counts:
        if count == mode_fields:
            return index
    return 0


def _looks_like_header(cells: list[str], data_rows: list[list[str]]) -> bool:
    """Heuristic: header if cells are non-numeric-ish and differ from data type shapes."""
    if not cells:
        return False
    # Empty header cells → not a confident header.
    if any(not cell.strip() for cell in cells):
        return False
    non_string_data = 0
    total = 0
    for row in data_rows[:20]:
        for cell in row:
            if not cell.strip():
                continue
            total += 1
            if resolve_cell_rung(cell) != "string":
                non_string_data += 1
    header_as_string = sum(1 for cell in cells if resolve_cell_rung(cell) == "string")
    # Header-like when most header cells are string tokens AND body has some typed cells,
    # or when header cells are mostly string identifiers even if the body is untyped.
    if header_as_string == len(cells) and (total == 0 or non_string_data > 0):
        return True
    return header_as_string >= max(1, len(cells) - 1)


def normalize_header_name(name: str, *, case: str | None) -> str:
    """Opt-in header case normalization. ``case`` in {None, 'lower', 'upper', 'snake'}."""
    text = name.strip()
    if not case:
        return text
    if case == "lower":
        return text.lower()
    if case == "upper":
        return text.upper()
    if case == "snake":
        # camelCase / spaces / hyphens → snake_case (deterministic).
        spaced = re.sub(r"[\s\-]+", "_", text)
        with_underscores = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", spaced)
        return with_underscores.lower()
    return text


@dataclass
class PreparedCsv:
    """Result of messy-CSV preprocessing ready for engine read + inference."""

    headers: list[str]
    rows: list[list[str | None]]  # null-normalized cells
    report: IngestReport


def prepare_messy_csv(
    path: str | Path,
    *,
    sep: str | None = None,
    header: bool | None = None,
    null_value: str | None = None,
    normalize_header_case: str | None = None,
    encoding: str = "utf-8",
) -> PreparedCsv:
    """Load + clean a messy CSV into headers/rows + diagnostics.

    Steps: BOM strip → delimiter detect → preamble skip → header detect → ragged pad →
    null-token normalize. Does **not** cast types (that is :func:`resolve_column_type`).
    """
    path_obj = Path(path)
    raw_bytes = path_obj.read_bytes()
    # Decode UTF-8 with BOM handling; other encodings residual (smart path documents UTF-8).
    text = raw_bytes.decode(encoding, errors="replace")
    text, bom_stripped = _strip_bom(text)
    # Normalize newlines.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    # Drop a single trailing empty line from final newline (keep intentional blanks inside).
    if lines and lines[-1] == "":
        lines = lines[:-1]

    delimiter = detect_delimiter(lines, preferred=sep)
    skip = detect_preamble_skip(lines, delimiter)
    table_lines = lines[skip:]
    # Remove fully blank lines inside the table (count as non-data).
    dense_lines = [line for line in table_lines if line.strip()]

    parsed_rows: list[list[str]] = []
    for line in dense_lines:
        try:
            row = next(csv.reader([line], delimiter=delimiter))
        except csv.Error:
            row = line.split(delimiter)
        parsed_rows.append(row)

    null_tokens: set[str] = set(_DEFAULT_NULL_TOKENS)
    if null_value is not None:
        null_tokens.add(null_value.strip().casefold())

    report = IngestReport(
        path=str(path_obj),
        skipped_lines=skip,
        delimiter=delimiter,
        bom_stripped=bom_stripped,
        null_tokens=sorted(null_tokens),
        header_normalized=bool(normalize_header_case),
    )

    if not parsed_rows:
        report.header_row_index = None
        report.synthesized_headers = True
        return PreparedCsv(headers=[], rows=[], report=report)

    # Width = max field count (ragged pad target).
    width = max(len(row) for row in parsed_rows)

    # Header detection.
    use_header: bool
    if header is True:
        use_header = True
    elif header is False:
        use_header = False
    else:
        body = parsed_rows[1:] if len(parsed_rows) > 1 else []
        use_header = _looks_like_header(parsed_rows[0], body)

    if use_header:
        header_cells = list(parsed_rows[0])
        while len(header_cells) < width:
            header_cells.append(f"_c{len(header_cells)}")
        data_raw = parsed_rows[1:]
        report.header_row_index = skip
        report.synthesized_headers = False
    else:
        header_cells = [f"_c{index}" for index in range(width)]
        data_raw = parsed_rows
        report.header_row_index = None
        report.synthesized_headers = True

    headers = [
        normalize_header_name(name if name.strip() else f"_c{index}", case=normalize_header_case)
        for index, name in enumerate(header_cells[:width])
    ]
    # Deduplicate header names deterministically (name, name_2, name_3, …).
    seen: dict[str, int] = {}
    unique_headers: list[str] = []
    for name in headers:
        base = name if name else "_c"
        count = seen.get(base, 0) + 1
        seen[base] = count
        unique_headers.append(base if count == 1 else f"{base}_{count}")
    headers = unique_headers

    rows: list[list[str | None]] = []
    ragged = 0
    for raw_row in data_raw:
        if len(raw_row) < width:
            ragged += 1
        padded: list[str | None] = []
        for index in range(width):
            if index < len(raw_row):
                cell = raw_row[index]
                padded.append(None if is_null_token(cell, null_tokens) else cell)
            else:
                padded.append(None)
        # Truncate over-wide rows to width (extra fields dropped; counted as ragged).
        if len(raw_row) > width:
            ragged += 1
        rows.append(padded)

    report.ragged_rows_padded = ragged
    report.data_row_count = len(rows)

    return PreparedCsv(headers=headers, rows=rows, report=report)


def infer_schema_from_rows(
    headers: list[str],
    rows: list[list[str | None]],
    *,
    null_tokens: frozenset[str] | set[str] | None = None,
) -> list[ColumnIngestReport]:
    """Run the protocol ladder per column; return column reports."""
    if not headers:
        return []
    columns: list[ColumnIngestReport] = []
    for index, name in enumerate(headers):
        values: list[str | None] = [(row[index] if index < len(row) else None) for row in rows]
        resolution = resolve_column_type(values, null_tokens=null_tokens)
        type_label = resolution.rung
        if resolution.rung == "decimal128" and resolution.decimal_precision is not None:
            type_label = (
                f"decimal128({resolution.decimal_precision},{resolution.decimal_scale or 0})"
            )
        columns.append(
            ColumnIngestReport(
                name=name,
                resolved_type=type_label,
                fallback_count=resolution.fallback_count,
                null_count=resolution.null_count,
                sample_count=resolution.sample_count,
                decimal_precision=resolution.decimal_precision,
                decimal_scale=resolution.decimal_scale,
            )
        )
    return columns


def load_smart_csv(
    session: Any,
    path: str | Path,
    *,
    sep: str | None = None,
    header: bool | None = None,
    null_value: str | None = None,
    normalize_header_case: str | None = None,
    sampling_rows: int | None = None,
) -> tuple[Any, IngestReport]:
    """Prepare, infer, cast via engine, return ``(DataFrame, IngestReport)``.

    Python owns inference decisions (protocol). Cleaned cells are loaded via
    ``createDataFrame`` as all-string then cast with engine ``selectExpr`` so the
    plan does not depend on a deleted temp path (lazy CSV scans would race unlink).
    Mixed-case headers use quoted identifiers (``F.col``+cast lowercases unquoted
    names on the createDataFrame path — use ``quote_ident``).

    **Sampling (r26 Q4):** inference scans at most ``sampling_rows`` data rows
    (default :data:`DEFAULT_INFERENCE_SAMPLING_ROWS` = 10_000 when the file is
    larger). The full file is always materialised for the data frame. A type
    class that appears only past the cap can under-widen the schema; the
    subsequent cast then fails loud (e.g. decimal overflow) rather than
    corrupting — raise ``samplingRows`` to scan more. ``sampling_rows`` ≤ 0 is
    refused by the caller (IllegalArgumentException).
    """
    from repark.spark._idents import quote_ident
    from repark.spark.types import StringType, StructField, StructType

    prepared = prepare_messy_csv(
        path,
        sep=sep,
        header=header,
        null_value=null_value,
        normalize_header_case=normalize_header_case,
    )
    report = prepared.report
    if not prepared.headers:
        frame = session.sql("SELECT 1 AS _repark_smart_empty WHERE 1 = 0").drop(
            "_repark_smart_empty"
        )
        report.columns = []
        return frame, report

    # Inference row budget (greylit): full scan when len(rows) <= limit.
    if sampling_rows is None:
        limit = DEFAULT_INFERENCE_SAMPLING_ROWS
    else:
        limit = int(sampling_rows)
        if limit <= 0:
            raise ValueError(
                f"sampling_rows must be > 0 (got {limit}); facade maps this to "
                "IllegalArgumentException"
            )
    total_rows = len(prepared.rows)
    if total_rows <= limit:
        inference_rows = prepared.rows
        capped = False
        effective_limit = total_rows if total_rows > 0 else limit
    else:
        inference_rows = prepared.rows[:limit]
        capped = True
        effective_limit = limit
    report.inference_rows_scanned = len(inference_rows)
    report.inference_capped = capped
    report.sampling_rows_limit = effective_limit

    null_tokens: set[str] = set(report.null_tokens)
    column_reports = infer_schema_from_rows(
        prepared.headers, inference_rows, null_tokens=null_tokens
    )
    report.columns = column_reports

    resolutions = [
        resolve_column_type(
            [(row[index] if index < len(row) else None) for row in inference_rows],
            null_tokens=null_tokens,
        )
        for index in range(len(prepared.headers))
    ]

    # All-string frame from cleaned cells (None stays None; no temp-file lifetime hazard).
    string_schema = StructType([StructField(name, StringType(), True) for name in prepared.headers])
    data_rows = [tuple(row) for row in prepared.rows]
    frame = session.createDataFrame(data_rows, schema=string_schema)

    # Build CAST expressions with quoted idents (preserves mixed-case headers).
    expressions: list[str] = []
    for name, resolution in zip(prepared.headers, resolutions, strict=True):
        quoted = quote_ident(name)
        if resolution.rung == "string":
            expressions.append(f"{quoted} AS {quoted}")
        else:
            cast_type = rung_to_sql_cast(resolution)
            # Empty string → NULL before cast so int/bool casts do not fail on "".
            expressions.append(
                f"CAST(CASE WHEN CAST({quoted} AS VARCHAR) = '' THEN NULL "
                f"ELSE {quoted} END AS {cast_type}) AS {quoted}"
            )
    frame = frame.selectExpr(*expressions)

    return frame, report
