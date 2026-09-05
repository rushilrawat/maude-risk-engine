"""Read-only discovery and audit of local canonical MAUDE archives."""

from collections.abc import Iterable
from pathlib import Path

from maude.domain.enums import QualityLevel, TableKind
from maude.domain.models import LocalAuditItem, LocalAuditResult, QualityResult
from maude.ingestion.archive import inspect_source, open_source_text, select_source_encoding
from maude.ingestion.schemas import detect_table
from maude.quality.checks import has_blocking_failure, required_table_set, truncation_comparison


def _quality(
    check: str,
    level: QualityLevel,
    passed: bool,
    message: str,
    **metrics: int | float | str | None,
) -> QualityResult:
    return QualityResult(check=check, level=level, passed=passed, message=message, metrics=metrics)


def _header(path: Path, member: str | None, encoding: str) -> tuple[str, ...]:
    with open_source_text(path, member, encoding=encoding) as source:
        return tuple(source.readline().rstrip("\r\n").split("|"))


def _is_add_file(path: Path) -> bool:
    stem = path.stem.lower()
    return stem.endswith("add") or "_add" in stem or "-add" in stem


def _archive_paths(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in root.glob("*.zip") if not _is_add_file(path)))


def _conversion_path(root: Path, archive: Path) -> Path | None:
    expected = f"{archive.stem}_utf8.txt".lower()
    matches = tuple(
        sorted(path for path in root.glob("*_UTF8.txt") if path.name.lower() == expected)
    )
    return matches[0] if len(matches) == 1 else None


def _conversion_quality(archive: Path, member: str, converted: Path) -> tuple[QualityResult, ...]:
    try:
        inspected = inspect_source(converted)
        encoding = select_source_encoding(converted, inspected)
        header = _header(converted, None, encoding)
        return (
            _quality(
                "conversion_inspection",
                QualityLevel.BLOCKING,
                True,
                "converted UTF-8 sibling was inspected",
                encoding=encoding,
                header="|".join(header),
            ),
            truncation_comparison(archive, converted, member=member),
        )
    except Exception as error:
        return (
            _quality(
                "conversion_inspection",
                QualityLevel.BLOCKING,
                False,
                f"could not inspect converted UTF-8 sibling: {type(error).__name__}: {error}",
                converted_path=str(converted),
            ),
        )


def _conversion_ratio(quality: Iterable[QualityResult]) -> float | None:
    for result in quality:
        if result.check == "truncation_comparison":
            observed = result.metrics.get("observed")
            if isinstance(observed, int | float):
                return float(observed)
    return None


def audit_local_sources(source_root: Path) -> LocalAuditResult:
    """Identify exactly one safe current archive per MAUDE table without writing files."""
    root = Path(source_root)
    global_quality: list[QualityResult] = []
    by_table: dict[TableKind, list[LocalAuditItem]] = {}
    if not root.is_dir():
        global_quality.append(
            _quality(
                "source_root",
                QualityLevel.BLOCKING,
                False,
                f"source root is not a directory: {root}",
                source_root=str(root),
            )
        )
    else:
        for archive in _archive_paths(root):
            try:
                inspected = inspect_source(archive)
                if inspected.member is None:
                    raise ValueError("canonical source must be a ZIP archive member")
                encoding = select_source_encoding(archive, inspected)
                header = _header(archive, inspected.member, encoding)
                spec = detect_table(inspected.member, header)
            except Exception as error:
                global_quality.append(
                    _quality(
                        "archive_inspection",
                        QualityLevel.BLOCKING,
                        False,
                        f"archive {archive.name} could not be identified: "
                        f"{type(error).__name__}: {error}",
                        archive=str(archive),
                    )
                )
                continue

            converted = _conversion_path(root, archive)
            item_quality: list[QualityResult] = [
                _quality(
                    "archive_integrity",
                    QualityLevel.BLOCKING,
                    True,
                    "archive CRC, member safety, and identity checks passed",
                    archive=archive.name,
                    member=inspected.member,
                    checksum=inspected.sha256,
                ),
                _quality(
                    "schema_detection",
                    QualityLevel.BLOCKING,
                    True,
                    f"archive member matches {spec.kind.value} schema",
                    table=spec.kind.value,
                    header="|".join(header),
                    encoding=encoding,
                ),
            ]
            if converted is not None:
                item_quality.extend(_conversion_quality(archive, inspected.member, converted))
            item = LocalAuditItem(
                table=spec.kind,
                archive_path=str(archive),
                archive_member=inspected.member,
                sha256=inspected.sha256,
                encoding=encoding,
                converted_path=str(converted) if converted is not None else None,
                conversion_row_ratio=_conversion_ratio(item_quality),
                quality=tuple(item_quality),
            )
            by_table.setdefault(spec.kind, []).append(item)

    items: list[LocalAuditItem] = []
    for table, candidates in sorted(by_table.items(), key=lambda value: value[0].value):
        if len(candidates) == 1:
            items.append(candidates[0])
        else:
            global_quality.append(
                _quality(
                    "archive_family_ambiguity",
                    QualityLevel.BLOCKING,
                    False,
                    f"multiple archives match required table {table.value}",
                    table=table.value,
                    archives=", ".join(candidate.archive_path for candidate in candidates),
                )
            )
    global_quality.append(required_table_set(item.table for item in items))
    all_quality: Iterable[QualityResult] = (
        *global_quality,
        *(quality for item in items for quality in item.quality),
    )
    return LocalAuditResult(
        source_root=str(root),
        items=tuple(items),
        quality=tuple(global_quality),
        has_blocking_failure=has_blocking_failure(all_quality),
    )


def blocking_findings(
    result: LocalAuditResult, *, include_conversions: bool
) -> tuple[QualityResult, ...]:
    """Return failed blocking audit findings, optionally ignoring hand conversions."""
    findings = list(result.quality)
    for item in result.items:
        findings.extend(item.quality)
    return tuple(
        finding
        for finding in findings
        if finding.level is QualityLevel.BLOCKING
        and not finding.passed
        and (
            include_conversions
            or (
                not finding.check.startswith("conversion_")
                and finding.check != "truncation_comparison"
            )
        )
    )


def selected_archives(result: LocalAuditResult) -> tuple[Path, ...]:
    """Return the canonical archive path for each successful table identification."""
    ordered_items = sorted(result.items, key=lambda item: item.table.value)
    return tuple(Path(item.archive_path) for item in ordered_items)
