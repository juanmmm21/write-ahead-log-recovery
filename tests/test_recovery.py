"""Tests de recuperación redo/undo, incluyendo simulación de crash a mitad
de escritura (truncamiento) y de corrupción por checksum."""

from __future__ import annotations

from pathlib import Path

import pytest

from write_ahead_log_recovery.models import ChecksumMismatchError
from write_ahead_log_recovery.pipeline import (
    WriteAheadLog,
    dump_records,
    iter_records,
    recover,
)


class RecordingApplier:
    def __init__(self) -> None:
        self.tables: dict[str, dict[bytes, bytes]] = {}

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.tables.setdefault(table, {})[key] = value

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.tables.setdefault(table, {})[key] = value

    def apply_delete(self, table: str, key: bytes) -> None:
        self.tables.setdefault(table, {}).pop(key, None)


def test_redo_applies_committed_transaction(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"alice", b"v1")
        wal.append_update(txn, "users", b"alice", b"v1", b"v2")
        wal.commit(txn)

    applier = RecordingApplier()
    report = recover(wal_path, applier)

    assert applier.tables["users"][b"alice"] == b"v2"
    assert report.committed_transactions == frozenset({txn})
    assert report.rolled_back_transactions == frozenset()
    assert report.redone_operations == 2
    assert not report.stopped_due_to_truncation
    assert not report.stopped_due_to_corruption


def test_undo_reverts_uncommitted_transaction(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        base_txn = wal.begin_transaction()
        wal.append_insert(base_txn, "users", b"alice", b"v0")
        wal.commit(base_txn)

        dangling_txn = wal.begin_transaction()
        wal.append_update(dangling_txn, "users", b"alice", b"v0", b"v1")
        # Sin COMMIT: simula que el proceso murió antes de confirmar.

    applier = RecordingApplier()
    report = recover(wal_path, applier)

    assert applier.tables["users"][b"alice"] == b"v0"
    assert report.rolled_back_transactions == frozenset({dangling_txn})
    assert report.undone_operations == 1


def test_undo_of_insert_removes_the_key(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"ghost", b"v")
        # Sin COMMIT.

    applier = RecordingApplier()
    recover(wal_path, applier)

    assert b"ghost" not in applier.tables.get("users", {})


def test_explicit_abort_is_also_undone(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"alice", b"v1")
        wal.abort(txn)

    applier = RecordingApplier()
    report = recover(wal_path, applier)

    assert b"alice" not in applier.tables.get("users", {})
    assert report.rolled_back_transactions == frozenset({txn})


def test_recovery_survives_truncated_tail_from_a_simulated_crash(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn1 = wal.begin_transaction()
        wal.append_insert(txn1, "users", b"alice", b"v1")
        wal.commit(txn1)

        txn2 = wal.begin_transaction()
        wal.append_insert(txn2, "users", b"bob", b"v2")
        wal.commit(txn2)

    original = wal_path.read_bytes()
    # Simula un crash a mitad del último `fsync`: se corta el fichero
    # dentro del último registro (el COMMIT de txn2), dejándolo incompleto.
    truncated = original[:-3]
    wal_path.write_bytes(truncated)

    applier = RecordingApplier()
    report = recover(wal_path, applier)

    assert report.stopped_due_to_truncation
    assert applier.tables["users"][b"alice"] == b"v1"
    # txn2 se insertó pero su COMMIT se perdió en el crash: no debe quedar
    # confirmada, y como la caída ocurrió tras aplicar el insert antes del
    # commit, se deshace igual que cualquier transacción sin confirmar.
    assert txn2 not in report.committed_transactions


def test_recovery_reports_corruption_without_crashing_the_process(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn1 = wal.begin_transaction()
        wal.append_insert(txn1, "users", b"alice", b"v1")
        wal.commit(txn1)

        txn2 = wal.begin_transaction()
        wal.append_insert(txn2, "users", b"bob", b"v2")
        wal.commit(txn2)

    data = bytearray(wal_path.read_bytes())
    # Corrompe un byte dentro del último registro (el COMMIT de txn2) sin
    # cambiar la longitud del fichero: no es un truncamiento, es corrupción
    # real de un registro ya completo. Se deja intacto todo lo anterior
    # (el INSERT+COMMIT de txn1) para comprobar que sigue siendo recuperable.
    data[-2] ^= 0xFF
    wal_path.write_bytes(bytes(data))

    applier = RecordingApplier()
    report = recover(wal_path, applier)  # No debe lanzar excepción ni matar el proceso.

    assert report.stopped_due_to_corruption
    assert applier.tables["users"][b"alice"] == b"v1"


def test_iter_records_directly_raises_on_corruption(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"alice", b"v1")
        wal.commit(txn)

    data = bytearray(wal_path.read_bytes())
    data[-1] ^= 0xFF
    wal_path.write_bytes(bytes(data))

    with open(wal_path, "rb") as reader, pytest.raises(ChecksumMismatchError):
        list(iter_records(reader))


def test_dump_records_ignores_truncated_tail(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"alice", b"v1")
        wal.commit(txn)

    original = wal_path.read_bytes()
    wal_path.write_bytes(original[:-2])

    records = dump_records(wal_path)
    assert len(records) == 1
