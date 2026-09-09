from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from maude.domain.enums import SourceRole, TableKind
from maude.ingestion.reconcile import ReconciliationError, reconcile_table


def _write_role(path: Path, rows: list[dict[str, str]], schema: pa.Schema | None = None) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


def _master(key: str, value: str, role: SourceRole) -> dict[str, str]:
    return {
        "mdr_report_key": key,
        "event_type": value,
        "record_content_hash": value,
        "_source_role": role.value,
        "_source_filename": f"mdrfoi{'' if role is SourceRole.BASE else role.value}.txt",
    }


def test_reconcile_applies_precedence_without_deleting_absent_base_rows(tmp_path: Path) -> None:
    paths = {role: tmp_path / f"{role.value}.parquet" for role in SourceRole}
    base_rows = [
        _master("1", "base-one", SourceRole.BASE),
        _master("2", "same", SourceRole.BASE),
        _master("5", "base-only", SourceRole.BASE),
    ]
    _write_role(paths[SourceRole.BASE], base_rows)
    _write_role(
        paths[SourceRole.ADD],
        [_master("2", "same", SourceRole.ADD), _master("3", "add-three", SourceRole.ADD)],
    )
    _write_role(
        paths[SourceRole.CHANGE],
        [
            _master("1", "changed-one", SourceRole.CHANGE),
            _master("4", "change-four", SourceRole.CHANGE),
        ],
    )
    output = tmp_path / "current.parquet"

    stats = reconcile_table(TableKind.MASTER, paths, output)

    rows = pq.read_table(output).to_pylist()
    assert [(row["mdr_report_key"], row["event_type"], row["_source_role"]) for row in rows] == [
        ("1", "changed-one", "change"),
        ("2", "same", "add"),
        ("3", "add-three", "add"),
        ("4", "change-four", "change"),
        ("5", "base-only", "base"),
    ]
    by_role = {item.role: item for item in stats}
    assert (
        by_role[SourceRole.ADD].inserted,
        by_role[SourceRole.ADD].updated,
        by_role[SourceRole.ADD].unchanged,
    ) == (1, 0, 1)
    assert (
        by_role[SourceRole.CHANGE].inserted,
        by_role[SourceRole.CHANGE].updated,
        by_role[SourceRole.CHANGE].unchanged,
    ) == (1, 1, 0)
    assert by_role[SourceRole.BASE].superseded == 2


def test_reconcile_uses_complete_composite_business_key(tmp_path: Path) -> None:
    paths = {role: tmp_path / f"{role.value}.parquet" for role in SourceRole}
    base = [
        {
            "mdr_report_key": "1",
            "device_sequence_no": sequence,
            "record_content_hash": value,
            "value": value,
            "_source_role": "base",
        }
        for sequence, value in (("1", "first"), ("2", "second"))
    ]
    schema = pa.Table.from_pylist(base).schema
    _write_role(paths[SourceRole.BASE], base, schema)
    _write_role(paths[SourceRole.ADD], [], schema)
    _write_role(
        paths[SourceRole.CHANGE],
        [
            {
                "mdr_report_key": "1",
                "device_sequence_no": "2",
                "record_content_hash": "changed",
                "value": "changed",
                "_source_role": "change",
            }
        ],
        schema,
    )

    reconcile_table(TableKind.DEVICE, paths, tmp_path / "current.parquet")

    assert [row["value"] for row in pq.read_table(tmp_path / "current.parquet").to_pylist()] == [
        "first",
        "changed",
    ]


@pytest.mark.parametrize(
    ("table", "business_key"),
    (
        (TableKind.PATIENT, {"mdr_report_key": "1", "patient_sequence_number": "2"}),
        (TableKind.NARRATIVE, {"mdr_text_key": "10"}),
    ),
)
def test_reconcile_uses_each_table_business_key(
    tmp_path: Path, table: TableKind, business_key: dict[str, str]
) -> None:
    paths = {role: tmp_path / f"{role.value}.parquet" for role in SourceRole}
    base = {
        **business_key,
        "record_content_hash": "before",
        "value": "before",
        "_source_role": "base",
    }
    changed = {
        **business_key,
        "record_content_hash": "after",
        "value": "after",
        "_source_role": "change",
    }
    schema = pa.Table.from_pylist([base]).schema
    _write_role(paths[SourceRole.BASE], [base], schema)
    _write_role(paths[SourceRole.ADD], [], schema)
    _write_role(paths[SourceRole.CHANGE], [changed], schema)

    reconcile_table(table, paths, tmp_path / "current.parquet")

    assert pq.read_table(tmp_path / "current.parquet").to_pylist()[0]["value"] == "after"


def test_reconcile_rejects_duplicate_keys_within_one_role(tmp_path: Path) -> None:
    paths = {role: tmp_path / f"{role.value}.parquet" for role in SourceRole}
    duplicate = _master("1", "duplicate", SourceRole.BASE)
    schema = pa.Table.from_pylist([duplicate]).schema
    _write_role(paths[SourceRole.BASE], [duplicate, duplicate], schema)
    _write_role(paths[SourceRole.ADD], [], schema)
    _write_role(paths[SourceRole.CHANGE], [], schema)
    output = tmp_path / "current.parquet"
    output.write_bytes(b"prior output")

    with pytest.raises(ReconciliationError, match=r"duplicate.*base"):
        reconcile_table(TableKind.MASTER, paths, output)

    assert output.read_bytes() == b"prior output"
