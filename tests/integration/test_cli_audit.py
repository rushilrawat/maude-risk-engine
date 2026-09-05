from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from typer.testing import CliRunner

import maude.ingestion.pipeline as pipeline
from maude.cli import app


def _write_archive(root: Path, archive_name: str, member: str, header: str, rows: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    archive = root / archive_name
    with ZipFile(archive, "w", ZIP_DEFLATED) as handle:
        handle.writestr(member, f"{header}\n{rows}")
    return archive


def _complete_archive_root(root: Path) -> Path:
    _write_archive(
        root,
        "mdrfoi.zip",
        "mdrfoi.txt",
        "MDR_REPORT_KEY|DATE_RECEIVED|EVENT_TYPE",
        "1|01/02/2025|D\n2|01/03/2025|M\n",
    )
    _write_archive(
        root,
        "device.zip",
        "device.txt",
        "MDR_REPORT_KEY|DEVICE_SEQUENCE_NO|DEVICE_REPORT_PRODUCT_CODE",
        "1|1|ABC\n2|1|XYZ\n",
    )
    _write_archive(
        root,
        "patient.zip",
        "patient.txt",
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER",
        "1|1\n2|1\n",
    )
    _write_archive(
        root,
        "foitext.zip",
        "foitext.txt",
        "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|FOI_TEXT",
        "1|10|N|First narrative\n2|11|N|Second narrative\n",
    )
    return root


def test_audit_local_reports_truncated_patient_conversion(tmp_path: Path) -> None:
    source_root = _complete_archive_root(tmp_path / "sources")
    (source_root / "patient_UTF8.txt").write_text(
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n1|1\n", encoding="utf-8"
    )

    result = CliRunner().invoke(app, ["audit-local", "--data-root", str(source_root)])

    assert result.exit_code == 1
    assert "patient_UTF8.txt" in result.stdout
    assert "truncation" in result.stdout.lower()


def test_audit_local_uses_schema_and_never_substitutes_add_files(tmp_path: Path) -> None:
    from maude.service.audit import audit_local_sources

    source_root = _complete_archive_root(tmp_path / "sources")
    (source_root / "patient.zip").unlink()
    _write_archive(
        source_root,
        "patient_add.zip",
        "patient_add.txt",
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER",
        "1|99\n",
    )

    result = audit_local_sources(source_root)

    assert result.has_blocking_failure is True
    assert all(item.table.value != "patient" for item in result.items)
    assert any("missing required tables: patient" in item.message for item in result.quality)


def test_audit_local_rejects_additive_member_in_a_neutral_archive_name(tmp_path: Path) -> None:
    from maude.service.audit import audit_local_sources

    source_root = _complete_archive_root(tmp_path / "sources")
    (source_root / "patient.zip").unlink()
    _write_archive(
        source_root,
        "current.zip",
        "patient_add.txt",
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER",
        "1|99\n",
    )

    result = audit_local_sources(source_root)

    assert result.has_blocking_failure is True
    assert all(item.table.value != "patient" for item in result.items)
    assert any("missing required tables: patient" in item.message for item in result.quality)


def test_ingest_local_prints_promoted_snapshot(tmp_path: Path) -> None:
    source_root = _complete_archive_root(tmp_path / "sources")
    data_root = tmp_path / "data"

    result = CliRunner().invoke(
        app,
        [
            "ingest-local",
            "fixture-2025-06",
            "--source-root",
            str(source_root),
            "--data-root",
            str(data_root),
        ],
    )

    assert result.exit_code == 0
    assert "PROMOTED fixture-2025-06" in result.stdout
    assert (data_root / "manifests" / "current.json").exists()
    assert (data_root / "manifests" / "quality-report.md").exists()


def test_ingest_local_allows_archive_only_after_invalid_conversion(tmp_path: Path) -> None:
    source_root = _complete_archive_root(tmp_path / "sources")
    (source_root / "patient_UTF8.txt").write_text(
        "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER\n1|1\n", encoding="utf-8"
    )
    data_root = tmp_path / "data"

    result = CliRunner().invoke(
        app,
        [
            "ingest-local",
            "fixture-2025-06",
            "--source-root",
            str(source_root),
            "--data-root",
            str(data_root),
            "--allow-archive-only",
        ],
    )

    assert result.exit_code == 0
    assert "ignored invalid conversion" in result.stdout.lower()
    assert "PROMOTED fixture-2025-06" in result.stdout


def test_ingest_local_refuses_success_when_current_pointer_publication_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = _complete_archive_root(tmp_path / "sources")
    data_root = tmp_path / "data"
    original_write = pipeline.write_manifest

    def fail_current_pointer(path: Path, snapshot: object) -> None:
        if path == data_root / "manifests" / "current.json":
            raise OSError("simulated current pointer failure")
        original_write(path, snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(pipeline, "write_manifest", fail_current_pointer)
    result = CliRunner().invoke(
        app,
        [
            "ingest-local",
            "fixture-2025-06",
            "--source-root",
            str(source_root),
            "--data-root",
            str(data_root),
        ],
    )

    assert result.exit_code == 1
    assert "incomplete" in result.stdout.lower()
    assert "PROMOTED fixture-2025-06" not in result.stdout
    assert not (data_root / "manifests" / "current.json").exists()
