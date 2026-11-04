"""Regresiones de los bugs que encontró `test_crash_consistency.py`, en
ficheros reales (sin el modelo de fallos), para que fallen de forma obvia
si vuelven."""

from __future__ import annotations

from pathlib import Path

from write_ahead_log_recovery import RecordType, WriteAheadLog, recover
from write_ahead_log_recovery.faultfs import FaultyFileSystem
from write_ahead_log_recovery.pipeline import dump_records


class DictApplier:
    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value

    def apply_delete(self, table: str, key: bytes) -> None:
        self.data.pop(key, None)


def _recover(path: Path) -> dict[bytes, bytes]:
    applier = DictApplier()
    recover(path, applier)
    return applier.data


def test_aborted_transaction_is_not_undone_over_a_later_commit(tmp_path: Path) -> None:
    """A actualiza k y aborta; B actualiza k y confirma. Antes, la
    recuperación deshacía A (sin COMMIT) DESPUÉS de rehacer B y restauraba
    el valor anterior a A: el commit de B se perdía."""
    path = tmp_path / "wal.log"
    with WriteAheadLog(path) as wal:
        setup = wal.begin_transaction()
        wal.append_insert(setup, "t", b"k", b"v0")
        wal.commit(setup)
        a = wal.begin_transaction()
        wal.append_update(a, "t", b"k", b"v0", b"vA")
        wal.abort(a)
        b = wal.begin_transaction()
        wal.append_update(b, "t", b"k", b"v0", b"vB")
        wal.commit(b)
    assert _recover(path) == {b"k": b"vB"}
    kinds = [r.record_type for r in dump_records(path)]
    assert RecordType.ROLLBACK in kinds  # el abort quedó en la historia con su CLR


def test_second_recovery_does_not_undo_an_interrupted_txn_again(tmp_path: Path) -> None:
    """Crash con la transacción A a medias; recuperar; B escribe la misma
    clave y confirma; segunda recuperación: antes A se volvía a deshacer
    encima de B."""
    path = tmp_path / "wal.log"
    with WriteAheadLog(path) as wal:
        a = wal.begin_transaction()
        wal.append_insert(a, "t", b"k", b"vA")  # crash sin COMMIT
    assert _recover(path) == {}
    with WriteAheadLog(path) as wal:
        b = wal.begin_transaction()
        wal.append_insert(b, "t", b"k", b"vB")
        wal.commit(b)
    assert _recover(path) == {b"k": b"vB"}


def test_reopen_after_a_torn_tail_appends_where_recovery_can_read(tmp_path: Path) -> None:
    """Cola rota por un crash (CRC inválido): antes reabrir el WAL lanzaba
    ChecksumMismatchError, y con una cola sólo truncada los registros nuevos
    se añadían detrás de la basura, fuera del alcance de la recuperación."""
    path = tmp_path / "wal.log"
    with WriteAheadLog(path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "t", b"a", b"1")
        wal.commit(txn)
        torn = wal.begin_transaction()
        wal.append_insert(torn, "t", b"b", b"2")
    data = bytearray(path.read_bytes())
    data[-3] ^= 0xFF  # el último registro queda con CRC inválido
    path.write_bytes(bytes(data))
    expected = {b"a": b"1"}
    for round_number in range(2):  # 2.ª vuelta: cola sólo truncada
        assert _recover(path) == expected
        key = f"c{round_number}".encode()
        with WriteAheadLog(path) as wal:
            txn = wal.begin_transaction()
            wal.append_insert(txn, "t", key, b"3")
            wal.commit(txn)
        expected[key] = b"3"
        assert _recover(path) == expected
        path.write_bytes(path.read_bytes() + b"WLR1\x00\x00")  # cola truncada
    assert list(tmp_path.glob("wal.log.tail-*"))  # la cola descartada se conserva


def test_creating_and_compacting_the_log_fsync_the_directory() -> None:
    """Sin fsync del directorio, ni el fichero recién creado ni el renombrado
    del compactado son durables: un crash podía hacer desaparecer el log con
    commits confirmados dentro."""
    fs = FaultyFileSystem()
    wal_path = Path("/d/wal.log")
    wal = WriteAheadLog(wal_path, fs=fs)
    txn = wal.begin_transaction()
    wal.append_insert(txn, "t", b"k", b"v")
    wal.commit(txn)
    # disco tras un crash en el que NADA no sincronizado sobrevive
    survivor = FaultyFileSystem(fs.crash_image())
    applier = DictApplier()
    recover(wal_path, applier, fs=survivor)
    assert applier.data == {b"k": b"v"}
    info = wal.checkpoint([])
    wal.truncate_before(info.safe_truncation_lsn)
    txn = wal.begin_transaction()
    wal.append_insert(txn, "t", b"k2", b"v2")
    wal.commit(txn)
    survivor = FaultyFileSystem(fs.crash_image())
    applier = DictApplier()
    recover(wal_path, applier, fs=survivor)
    assert applier.data == {b"k2": b"v2"}  # lo anterior al checkpoint vive en el storage
