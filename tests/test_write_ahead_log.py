"""Tests del escritor del WAL: LSN monotónico, fsync, reapertura y checkpoint."""

from __future__ import annotations

from pathlib import Path

from write_ahead_log_recovery.models import RecordType
from write_ahead_log_recovery.pipeline import WriteAheadLog, dump_records


def test_lsn_is_monotonically_increasing(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        lsn1 = wal.append_insert(txn, "users", b"a", b"1")
        lsn2 = wal.append_insert(txn, "users", b"b", b"2")
        lsn3 = wal.commit(txn)

    assert lsn1 < lsn2 < lsn3


def test_reopening_existing_wal_continues_lsn_sequence(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        last_lsn = wal.append_insert(txn, "users", b"a", b"1")
        wal.commit(txn)

    with WriteAheadLog(wal_path) as wal:
        txn2 = wal.begin_transaction()
        new_lsn = wal.append_insert(txn2, "users", b"b", b"2")

    assert new_lsn > last_lsn


def test_reopening_existing_wal_continues_transaction_id_sequence(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn1 = wal.begin_transaction()
        wal.append_insert(txn1, "users", b"a", b"1")
        wal.commit(txn1)

    with WriteAheadLog(wal_path) as wal:
        txn2 = wal.begin_transaction()

    assert txn2 > txn1


def test_records_survive_a_full_close_and_reopen(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"a", b"1")
        wal.commit(txn)

    records = dump_records(wal_path)
    assert [r.record_type for r in records] == [RecordType.INSERT, RecordType.COMMIT]


def test_checkpoint_writes_begin_and_end_records(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn = wal.begin_transaction()
        wal.append_insert(txn, "users", b"a", b"1")
        info = wal.checkpoint(active_transaction_ids=[txn])

    records = dump_records(wal_path)
    types = [r.record_type for r in records]
    assert types[-2:] == [RecordType.CHECKPOINT_BEGIN, RecordType.CHECKPOINT_END]
    checkpoint_record = records[-2]
    assert checkpoint_record.active_transaction_ids == (txn,)
    assert info.begin_lsn < info.end_lsn


def test_checkpoint_safe_lsn_excludes_committed_transactions(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn1 = wal.begin_transaction()
        wal.append_insert(txn1, "users", b"a", b"1")
        wal.commit(txn1)

        txn2 = wal.begin_transaction()
        second_insert_lsn = wal.append_insert(txn2, "users", b"b", b"2")

        info = wal.checkpoint(active_transaction_ids=[txn2])

    # Nada de txn1 (ya confirmada) debe ser necesario conservar: el punto
    # seguro de truncado debe alinearse con el primer registro de txn2,
    # la única transacción todavía activa.
    assert info.safe_truncation_lsn == second_insert_lsn


def test_truncate_before_discards_old_records_but_keeps_active_txn(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn1 = wal.begin_transaction()
        wal.append_insert(txn1, "users", b"a", b"1")
        wal.commit(txn1)

        txn2 = wal.begin_transaction()
        wal.append_insert(txn2, "users", b"b", b"2")

        info = wal.checkpoint(active_transaction_ids=[txn2])
        discarded = wal.truncate_before(info.safe_truncation_lsn)

    assert discarded > 0
    remaining = dump_records(wal_path)
    assert all(r.lsn >= info.safe_truncation_lsn for r in remaining)
    assert any(r.transaction_id == txn2 for r in remaining)
    assert not any(r.transaction_id == txn1 for r in remaining)


def test_truncate_before_keeps_working_after_more_writes(tmp_path: Path) -> None:
    wal_path = tmp_path / "wal.log"
    with WriteAheadLog(wal_path) as wal:
        txn1 = wal.begin_transaction()
        wal.append_insert(txn1, "users", b"a", b"1")
        wal.commit(txn1)
        info = wal.checkpoint(active_transaction_ids=[])
        wal.truncate_before(info.safe_truncation_lsn)

        txn2 = wal.begin_transaction()
        lsn = wal.append_insert(txn2, "users", b"b", b"2")
        wal.commit(txn2)

    assert lsn > info.safe_truncation_lsn
    records = dump_records(wal_path)
    assert any(r.transaction_id == txn2 for r in records)
