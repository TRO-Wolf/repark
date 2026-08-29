"""Shared persistence helpers for delegated ML models.

Envelope layout (repark-ml format plus a booster blob)::

    <path>/metadata.json
        format, version, kind, class, uid, library, library_version, booster_blob, …
    <path>/fitted/params.parquet   (exactly 1 row; fit metadata, never training rows)
    <path>/fitted/<blob>           (library OWN non-pickle bytes / text)

Atomic write uses the staging helpers from :mod:`repark.ml.pipeline`
(``_begin_atomic_save`` / ``_commit_atomic_save`` / ``_abort_atomic_save``).

Pickle is forbidden for every delegated backend. Refusals use one exact reason.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repark.errors import IllegalArgumentException

PICKLE_FORBIDDEN_REASON = "pickle forbidden (arbitrary code execution on load)"

SKLEARN_SAVE_UNSUPPORTED = (
    f"save not supported for sklearn ext estimators: {PICKLE_FORBIDDEN_REASON}; "
    "repark.ml.ext never pickles fitted models. "
    "XGBoost* and LightGBM* models support library-native booster-bytes save/load."
)

EXT_SAVE_UNSUPPORTED = (
    "save not supported for ext estimators "
    "(repark.ml.ext persistence: XGBoost* + LightGBM* support booster-bytes; "
    f"sklearn refuses: {PICKLE_FORBIDDEN_REASON})"
)


def _pyspark_version_provenance() -> str:
    """Best-effort pyspark version string for metadata provenance."""
    try:
        import pyspark

        return str(getattr(pyspark, "__version__", "unknown"))
    except ImportError:
        return "unavailable"


def library_major_version(version_string: str) -> int:
    """Parse major version from ``X.Y.Z`` (or ``X``); refuse empty/non-numeric major."""
    text = str(version_string).strip()
    if not text:
        raise IllegalArgumentException("library version string is empty")
    head = text.split(".", maxsplit=1)[0]
    digits = ""
    for char in head:
        if char.isdigit():
            digits += char
        else:
            break
    if not digits:
        raise IllegalArgumentException(f"library version major is not numeric: {version_string!r}")
    return int(digits)


def check_library_version_major(
    *,
    recorded: str | None,
    current: str,
    library_name: str,
) -> None:
    """Refuse load when recorded library major differs from the installed major.

    Missing ``library_version`` in older envelopes is tolerated. When present,
    a major mismatch is loud.
    """
    if recorded is None or str(recorded).strip() == "":
        return
    recorded_major = library_major_version(str(recorded))
    current_major = library_major_version(current)
    if recorded_major != current_major:
        raise IllegalArgumentException(
            f"{library_name} major version mismatch on load: "
            f"saved with {recorded!r} (major {recorded_major}), "
            f"runtime is {current!r} (major {current_major})"
        )


def safe_booster_blob_path(root: Path, relative: str, *, kind: str) -> Path:
    """Resolve ``relative`` under ``root`` only; refuse ``..`` / absolute escape.

    Mirrors :func:`repark.ml.pipeline._safe_stage_dir` so hostile ``metadata.json``
    ``booster_blob`` values cannot read files outside the model directory.
    """
    if not isinstance(relative, str) or not relative.strip():
        raise IllegalArgumentException(
            f"{kind} load: booster_blob must be a non-empty relative path"
        )
    if (
        Path(relative).is_absolute()
        or relative.startswith(("~",))
        or (len(relative) > 1 and relative[1] == ":")
    ):
        raise IllegalArgumentException(
            f"{kind} load: booster_blob must be relative under the model "
            f"directory (got absolute/drive path {relative!r})"
        )
    normalized = relative.replace("\\", "/").strip("/")
    if not normalized:
        raise IllegalArgumentException(f"{kind} load: booster_blob is empty: {relative!r}")
    parts = normalized.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise IllegalArgumentException(
            f"{kind} load: booster_blob must not contain '.'/'..' segments (got {relative!r})"
        )
    candidate = (root / Path(*parts)).resolve()
    root_resolved = root.resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as error:
        raise IllegalArgumentException(
            f"{kind} load: booster_blob escapes model root: {relative!r}"
        ) from error
    return candidate


def write_params_parquet(path: Path, fitted_payload: dict[str, Any]) -> None:
    """Write fitted params as a single-row parquet (JSON-encode list/dict values)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    columns: dict[str, list[Any]] = {}
    for key, value in fitted_payload.items():
        if isinstance(value, (list, dict)):
            columns[key] = [json.dumps(value)]
        else:
            columns[key] = [value]
    pq.write_table(pa.table(columns), path)


def read_params_parquet(path: Path, *, kind: str) -> dict[str, Any]:
    """Load exactly-1-row fitted params parquet; JSON-decode complex fields."""
    import pyarrow.parquet as pq

    if not path.is_file():
        raise IllegalArgumentException(
            f"{kind} load: missing fitted/params.parquet under {path.parent.parent}"
        )
    table = pq.read_table(path)
    if table.num_rows != 1:
        raise IllegalArgumentException(
            f"{kind} load: fitted/params.parquet must have exactly 1 row (got {table.num_rows})"
        )
    fitted: dict[str, Any] = {}
    row = {name: table.column(name)[0].as_py() for name in table.column_names}
    for key, value in row.items():
        if isinstance(value, str) and value[:1] in "[{":
            try:
                fitted[key] = json.loads(value)
                continue
            except json.JSONDecodeError:
                pass
        fitted[key] = value
    return fitted


def write_ext_model_tree(
    path: str,
    *,
    overwrite: bool,
    kind: str,
    model_class: type[Any],
    uid: str,
    params: dict[str, Any],
    fitted_payload: dict[str, Any],
    booster_blob_name: str,
    booster_bytes: bytes,
    library_name: str,
    library_version: str,
    extra_metadata: dict[str, Any] | None = None,
) -> None:
    """Atomically write a repark-ml envelope and booster blob.

    Never writes training rows. On failure the staging directory is aborted and
    the previous target (if any) is left intact.
    """
    from repark.spark.ml.pipeline import (
        REPARK_ML_FORMAT,
        REPARK_ML_VERSION,
        _abort_atomic_save,
        _begin_atomic_save,
        _commit_atomic_save,
    )

    if not booster_bytes:
        raise IllegalArgumentException(f"{kind}.save: empty booster blob refused")
    if not booster_blob_name or "/" in booster_blob_name or "\\" in booster_blob_name:
        raise IllegalArgumentException(
            f"{kind}.save: booster_blob_name must be a plain basename (got {booster_blob_name!r})"
        )
    if "num_features" not in fitted_payload:
        raise IllegalArgumentException(f"{kind}.save: fitted_payload must include num_features")

    target = Path(path)
    staging = _begin_atomic_save(target, overwrite=overwrite)
    try:
        fitted_dir = staging / "fitted"
        fitted_dir.mkdir(exist_ok=True)
        (fitted_dir / booster_blob_name).write_bytes(booster_bytes)

        payload = dict(fitted_payload)
        payload.setdefault("booster_blob", booster_blob_name)
        write_params_parquet(fitted_dir / "params.parquet", payload)

        metadata: dict[str, Any] = {
            "format": REPARK_ML_FORMAT,
            "version": REPARK_ML_VERSION,
            "kind": kind,
            "class": f"{model_class.__module__}.{model_class.__name__}",
            "uid": uid,
            "pyspark_version": _pyspark_version_provenance(),
            "library": library_name,
            "library_version": library_version,
            "params": dict(params),
            "fitted_keys": sorted(payload.keys()),
            "booster_blob": f"fitted/{booster_blob_name}",
        }
        if extra_metadata:
            metadata.update(extra_metadata)
        (staging / "metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )
        _commit_atomic_save(staging, target, overwrite=overwrite)
    except Exception:
        _abort_atomic_save(staging)
        raise


def load_ext_model_envelope(
    path: str,
    *,
    expected_kind: str,
    library_name: str,
    current_library_version: str,
    default_blob_name: str,
    expected_classifier: bool | None = None,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    """Load + validate envelope; return ``(metadata, fitted, booster_bytes)``.

    Guards: format/version/kind, library major version, confined blob path,
    non-empty blob, required positive ``num_features`` in params.parquet, and
    (when present) fitted ``classifier`` flag versus the reader task type.
    """
    from repark.spark.ml.pipeline import REPARK_ML_FORMAT, REPARK_ML_VERSION

    target = Path(path)
    meta_path = target / "metadata.json"
    if not meta_path.is_file():
        raise IllegalArgumentException(f"missing metadata.json under {target}")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if metadata.get("format") != REPARK_ML_FORMAT:
        raise IllegalArgumentException(
            f"unsupported ML format {metadata.get('format')!r}; expected {REPARK_ML_FORMAT!r}"
        )
    if int(metadata.get("version", -1)) != REPARK_ML_VERSION:
        raise IllegalArgumentException(
            f"unsupported repark-ml version {metadata.get('version')!r}; "
            f"expected {REPARK_ML_VERSION}"
        )
    if metadata.get("kind") != expected_kind:
        raise IllegalArgumentException(
            f"expected kind {expected_kind}, got {metadata.get('kind')!r}"
        )

    recorded_lib = metadata.get("library_version")
    check_library_version_major(
        recorded=str(recorded_lib) if recorded_lib is not None else None,
        current=current_library_version,
        library_name=library_name,
    )

    fitted = read_params_parquet(target / "fitted" / "params.parquet", kind=expected_kind)
    if "num_features" not in fitted:
        raise IllegalArgumentException(
            f"{expected_kind} load: fitted/params.parquet missing num_features"
        )
    try:
        num_features = int(fitted["num_features"])
    except (TypeError, ValueError) as error:
        raise IllegalArgumentException(
            f"{expected_kind} load: num_features must be a positive int "
            f"(got {fitted.get('num_features')!r})"
        ) from error
    if num_features <= 0:
        raise IllegalArgumentException(
            f"{expected_kind} load: num_features must be > 0 (got {num_features})"
        )
    fitted["num_features"] = num_features

    # A rewritten kind must not change the classifier task. Older envelopes may omit the flag.
    if expected_classifier is not None and "classifier" in fitted:
        recorded_classifier = fitted.get("classifier")
        if recorded_classifier is not None and bool(recorded_classifier) != expected_classifier:
            raise IllegalArgumentException(
                f"{expected_kind} load: fitted classifier flag {recorded_classifier!r} "
                f"does not match reader task type (expected classifier={expected_classifier})"
            )

    blob_rel = metadata.get("booster_blob") or f"fitted/{default_blob_name}"
    blob_path = safe_booster_blob_path(target, str(blob_rel), kind=expected_kind)
    if not blob_path.is_file():
        alt_name = str(fitted.get("booster_blob") or default_blob_name)
        if "/" in alt_name.replace("\\", "/") or alt_name in (".", ".."):
            raise IllegalArgumentException(
                f"{expected_kind} load: fitted booster_blob name unsafe: {alt_name!r}"
            )
        alt = safe_booster_blob_path(target, f"fitted/{alt_name}", kind=expected_kind)
        if alt.is_file():
            blob_path = alt
        else:
            raise IllegalArgumentException(
                f"{expected_kind} load: missing booster blob at {blob_path}"
            )
    raw = blob_path.read_bytes()
    if not raw:
        raise IllegalArgumentException(f"{expected_kind} load: empty booster blob at {blob_path}")
    return metadata, fitted, raw


__all__ = [
    "EXT_SAVE_UNSUPPORTED",
    "PICKLE_FORBIDDEN_REASON",
    "SKLEARN_SAVE_UNSUPPORTED",
    "check_library_version_major",
    "library_major_version",
    "load_ext_model_envelope",
    "read_params_parquet",
    "safe_booster_blob_path",
    "write_ext_model_tree",
    "write_params_parquet",
]
