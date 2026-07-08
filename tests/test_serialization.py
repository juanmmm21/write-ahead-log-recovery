"""Tests de (de)serialización binaria de registros: round-trip, truncamiento
y detección de corrupción por checksum."""

from __future__ import annotations

import io

import pytest

from write_ahead_log_recovery.models import (
    ChecksumMismatchError,
    InvalidRecordError,
    LogRecord,
    RecordType,
    TruncatedRecordError,
)
from write_ahead_log_recovery.pipeline import decode_one_record, encode_record, iter_records


def _make_record(**overrides: object) -> LogRecord:
    defaults: dict[str, object] = dict(
        lsn=1,
        record_type=RecordType.INSERT,
        transaction_id=7,
        table="users",
        key=b"alice",
        new_value=b"payload",
        old_value=None,
        active_transaction_ids=(),
    )
    defaults.update(overrides)
    return LogRecord(**defaults)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "record",
    [
        _make_record(record_type=RecordType.INSERT, new_value=b"v1", old_value=None),
        _make_record(record_type=RecordType.UPDATE, new_value=b"v2", old_value=b"v1"),
        _make_record(record_type=RecordType.DELETE, new_value=None, old_value=b"v1"),
        _make_record(
            record_type=RecordType.COMMIT, table="", key=b"", new_value=None, old_value=None
        ),
        _make_record(
            record_type=RecordType.ABORT, table="", key=b"", new_value=None, old_value=None
        ),
        _make_record(
            record_type=RecordType.CHECKPOINT_BEGIN,
            table="",
            key=b"",
            new_value=None,
            old_value=None,
            active_transaction_ids=(3, 5, 9),
        ),
        _make_record(table="unicode_ñ_表", key=b"\x00\x01binary\xff"),
    ],
)
def test_encode_decode_round_trip(record: LogRecord) -> None:
    encoded = encode_record(record)
    decoded = decode_one_record(io.BytesIO(encoded))
    assert decoded == record


def test_iter_records_reads_multiple_records_in_order() -> None:
    records = [
        _make_record(lsn=1, transaction_id=1),
        _make_record(lsn=2, transaction_id=1, record_type=RecordType.COMMIT, new_value=None),
        _make_record(lsn=3, transaction_id=2, key=b"bob"),
    ]
    buffer = io.BytesIO(b"".join(encode_record(r) for r in records))
    assert list(iter_records(buffer)) == records


def test_iter_records_clean_eof_yields_nothing_extra() -> None:
    record = _make_record()
    buffer = io.BytesIO(encode_record(record))
    result = list(iter_records(buffer))
    assert result == [record]


@pytest.mark.parametrize("cut_at", [1, 5, 24, 26, 30])
def test_truncated_tail_raises_truncated_record_error(cut_at: int) -> None:
    encoded = encode_record(_make_record())
    truncated = encoded[:cut_at]
    assert 0 < len(truncated) < len(encoded)
    with pytest.raises(TruncatedRecordError):
        list(iter_records(io.BytesIO(truncated)))


def test_wrong_magic_raises_invalid_record_error() -> None:
    encoded = bytearray(encode_record(_make_record()))
    encoded[0:4] = b"XXXX"
    with pytest.raises(InvalidRecordError):
        list(iter_records(io.BytesIO(bytes(encoded))))


def test_flipped_byte_in_payload_raises_checksum_mismatch() -> None:
    encoded = bytearray(encode_record(_make_record(table="users", key=b"alice")))
    # Voltear un bit en mitad del payload sin cambiar la longitud del registro:
    # simula corrupción de bytes ya escritos, no un truncamiento.
    flip_index = len(encoded) - 6
    encoded[flip_index] ^= 0xFF
    with pytest.raises(ChecksumMismatchError):
        list(iter_records(io.BytesIO(bytes(encoded))))


def test_flipped_checksum_byte_raises_checksum_mismatch() -> None:
    encoded = bytearray(encode_record(_make_record()))
    encoded[-1] ^= 0xFF
    with pytest.raises(ChecksumMismatchError):
        list(iter_records(io.BytesIO(bytes(encoded))))


def test_two_records_second_corrupted_first_still_readable() -> None:
    good = encode_record(_make_record(lsn=1))
    bad = bytearray(encode_record(_make_record(lsn=2, transaction_id=2)))
    bad[len(bad) - 6] ^= 0xFF
    buffer = io.BytesIO(good + bytes(bad))

    reader = buffer
    first = decode_one_record(reader)
    assert first is not None
    assert first.lsn == 1
    with pytest.raises(ChecksumMismatchError):
        decode_one_record(reader)
