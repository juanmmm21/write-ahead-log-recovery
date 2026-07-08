"""Lógica core del write-ahead log: serialización binaria, escritura
durable (fsync), checkpointing y recuperación redo/undo.

Formato de registro en disco (todos los enteros en big-endian):

    +----------+---------+--------------+-----------------+----------------+-----------+----------+
    | MAGIC(4) | LSN(8)  | type(1)      | transaction_id  | payload_len(4) | payload(N) | crc32(4) |
    |          |         |              | (8)             |                |            |          |
    +----------+---------+--------------+-----------------+----------------+-----------+----------+

El checksum CRC32 cubre cabecera + payload (todo excepto el propio
checksum), lo que permite detectar corrupción de cualquier campo.
"""

from __future__ import annotations

import io
import os
import struct
import threading
import zlib
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import BinaryIO

from write_ahead_log_recovery.models import (
    MAGIC,
    CheckpointInfo,
    ChecksumMismatchError,
    InvalidRecordError,
    LogRecord,
    RecordType,
    RecoveryReport,
    TruncatedRecordError,
    WalIOError,
)
from write_ahead_log_recovery.protocols import StorageApplier

_HEADER_STRUCT = struct.Struct(">4sQBQI")  # magic, lsn, record_type, transaction_id, payload_len
_CHECKSUM_STRUCT = struct.Struct(">I")
_HEADER_SIZE = _HEADER_STRUCT.size
_CHECKSUM_SIZE = _CHECKSUM_STRUCT.size


# --- Codificación de campos de longitud variable dentro del payload ---------


def _encode_str(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise ValueError(f"cadena demasiado larga para codificar ({len(raw)} bytes)")
    return struct.pack(">H", len(raw)) + raw


def _decode_str(buf: bytes, offset: int) -> tuple[str, int]:
    (length,) = struct.unpack_from(">H", buf, offset)
    offset += 2
    raw = buf[offset : offset + length]
    return raw.decode("utf-8"), offset + length


def _encode_bytes(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def _decode_bytes(buf: bytes, offset: int) -> tuple[bytes, int]:
    (length,) = struct.unpack_from(">I", buf, offset)
    offset += 4
    return buf[offset : offset + length], offset + length


def _encode_optional_bytes(value: bytes | None) -> bytes:
    if value is None:
        return b"\x00"
    return b"\x01" + _encode_bytes(value)


def _decode_optional_bytes(buf: bytes, offset: int) -> tuple[bytes | None, int]:
    flag = buf[offset]
    offset += 1
    if flag == 0:
        return None, offset
    return _decode_bytes(buf, offset)


def _encode_txn_list(ids: Sequence[int]) -> bytes:
    header = struct.pack(">H", len(ids))
    return header + b"".join(struct.pack(">Q", txn_id) for txn_id in ids)


def _decode_txn_list(buf: bytes, offset: int) -> tuple[tuple[int, ...], int]:
    (count,) = struct.unpack_from(">H", buf, offset)
    offset += 2
    ids = []
    for _ in range(count):
        (txn_id,) = struct.unpack_from(">Q", buf, offset)
        ids.append(txn_id)
        offset += 8
    return tuple(ids), offset


# --- Codificación / decodificación de registros completos ------------------


def encode_record(record: LogRecord) -> bytes:
    """Serializa un `LogRecord` al formato binario descrito arriba."""
    payload = (
        _encode_str(record.table)
        + _encode_bytes(record.key)
        + _encode_optional_bytes(record.new_value)
        + _encode_optional_bytes(record.old_value)
        + _encode_txn_list(record.active_transaction_ids)
    )
    header = _HEADER_STRUCT.pack(
        MAGIC, record.lsn, int(record.record_type), record.transaction_id, len(payload)
    )
    body = header + payload
    checksum = zlib.crc32(body) & 0xFFFFFFFF
    return body + _CHECKSUM_STRUCT.pack(checksum)


def _read_exact(reader: BinaryIO, size: int) -> bytes | None:
    """Lee exactamente `size` bytes o devuelve `None` si el stream está
    limpiamente al final (0 bytes disponibles). Si hay bytes pero menos de
    `size`, se considera un registro truncado (crash a mitad de escritura)."""
    chunk = reader.read(size)
    if len(chunk) == 0:
        return None
    if len(chunk) < size:
        raise TruncatedRecordError(
            f"se esperaban {size} bytes y solo había {len(chunk)}: cola del log truncada"
        )
    return chunk


def decode_one_record(reader: BinaryIO) -> LogRecord | None:
    """Lee y decodifica el siguiente registro de `reader`.

    Devuelve `None` si el stream terminó de forma limpia (no hay más
    registros). Lanza `TruncatedRecordError` si el registro quedó a medias
    y `ChecksumMismatchError`/`InvalidRecordError` ante corrupción real.
    """
    header_bytes = _read_exact(reader, _HEADER_SIZE)
    if header_bytes is None:
        return None

    magic, lsn, record_type_raw, transaction_id, payload_len = _HEADER_STRUCT.unpack(header_bytes)
    if magic != MAGIC:
        raise InvalidRecordError(f"magic inválido en LSN candidato {lsn}: {magic!r}")

    payload_bytes = _read_exact(reader, payload_len)
    if payload_bytes is None:
        raise TruncatedRecordError(
            f"registro LSN={lsn} anuncia payload de {payload_len} bytes "
            "pero el archivo termina antes"
        )

    checksum_bytes = _read_exact(reader, _CHECKSUM_SIZE)
    if checksum_bytes is None:
        raise TruncatedRecordError(f"falta el checksum del registro LSN={lsn}")

    body = header_bytes + payload_bytes
    expected_checksum = zlib.crc32(body) & 0xFFFFFFFF
    (stored_checksum,) = _CHECKSUM_STRUCT.unpack(checksum_bytes)
    if stored_checksum != expected_checksum:
        raise ChecksumMismatchError(
            f"checksum inválido en registro LSN={lsn}: "
            f"esperado {expected_checksum:#010x}, leído {stored_checksum:#010x}"
        )

    offset = 0
    table, offset = _decode_str(payload_bytes, offset)
    key, offset = _decode_bytes(payload_bytes, offset)
    new_value, offset = _decode_optional_bytes(payload_bytes, offset)
    old_value, offset = _decode_optional_bytes(payload_bytes, offset)
    active_transaction_ids, offset = _decode_txn_list(payload_bytes, offset)

    return LogRecord(
        lsn=lsn,
        record_type=RecordType(record_type_raw),
        transaction_id=transaction_id,
        table=table,
        key=key,
        new_value=new_value,
        old_value=old_value,
        active_transaction_ids=active_transaction_ids,
    )


def iter_records(reader: BinaryIO) -> Iterator[LogRecord]:
    """Itera los registros de un stream binario en orden, de principio a fin.

    Propaga `TruncatedRecordError` / `ChecksumMismatchError` /
    `InvalidRecordError` en cuanto se detecta el primer problema: a partir
    de ahí el contenido no es fiable y no tiene sentido seguir leyendo.
    """
    while True:
        record = decode_one_record(reader)
        if record is None:
            return
        yield record


def iter_records_from_path(path: Path) -> Iterator[LogRecord]:
    """Como `iter_records` pero abriendo el fichero en modo lectura."""
    with open(path, "rb") as reader:
        yield from iter_records(reader)


# --- Escritor del WAL --------------------------------------------------------


class WriteAheadLog:
    """Escritor/gestor del write-ahead log para un único fichero.

    Estrategia de sincronización: todas las mutaciones (`append_*`,
    `commit`, `abort`, `checkpoint`, `truncate_before`) toman `self._lock`
    porque comparten estado mutable (siguiente LSN, siguiente id de
    transacción y la posición de escritura del fichero) entre hilos. Es un
    lock de exclusión mutua simple: este módulo asume un único escritor
    lógico por fichero de WAL (varias transacciones concurrentes pueden
    llamar a `append_*` desde hilos distintos, pero la serialización real
    a disco es siempre secuencial).
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._next_lsn = 1
        self._next_txn_id = 1
        self._txn_first_lsn: dict[int, int] = {}

        if path.exists() and path.stat().st_size > 0:
            self._bootstrap_from_existing()

        try:
            self._file: BinaryIO = open(path, "a+b")
        except OSError as exc:
            raise WalIOError(f"no se pudo abrir el WAL en {path}: {exc}") from exc

    def _bootstrap_from_existing(self) -> None:
        """Recorre un log ya existente para recalcular el siguiente LSN y
        el siguiente id de transacción disponibles, tolerando una cola
        truncada (crash previo) pero no corrupción real a mitad de fichero."""
        max_lsn = 0
        max_txn_id = 0
        try:
            with open(self._path, "rb") as reader:
                try:
                    for record in iter_records(reader):
                        max_lsn = max(max_lsn, record.lsn)
                        max_txn_id = max(max_txn_id, record.transaction_id)
                        if record.transaction_id not in self._txn_first_lsn and (
                            record.record_type
                            in (RecordType.INSERT, RecordType.UPDATE, RecordType.DELETE)
                        ):
                            self._txn_first_lsn[record.transaction_id] = record.lsn
                except TruncatedRecordError:
                    # Cola cortada por una caída anterior: se ignora, el resto
                    # del log leído hasta aquí sigue siendo válido.
                    pass
        except OSError as exc:
            raise WalIOError(f"no se pudo leer el WAL existente en {self._path}: {exc}") from exc

        self._next_lsn = max_lsn + 1
        self._next_txn_id = max_txn_id + 1

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> WriteAheadLog:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def begin_transaction(self) -> int:
        """Asigna un nuevo id de transacción monotónico. No escribe nada
        en el log por sí mismo: la transacción "empieza" formalmente en su
        primer registro de operación."""
        with self._lock:
            txn_id = self._next_txn_id
            self._next_txn_id += 1
            return txn_id

    def _append_record(self, partial: LogRecord) -> int:
        with self._lock:
            lsn = self._next_lsn
            record = LogRecord(
                lsn=lsn,
                record_type=partial.record_type,
                transaction_id=partial.transaction_id,
                table=partial.table,
                key=partial.key,
                new_value=partial.new_value,
                old_value=partial.old_value,
                active_transaction_ids=partial.active_transaction_ids,
            )
            encoded = encode_record(record)
            try:
                self._file.write(encoded)
                self._file.flush()
                os.fsync(self._file.fileno())
            except OSError as exc:
                raise WalIOError(
                    f"fallo de E/S escribiendo el registro LSN={lsn} en {self._path}: {exc}"
                ) from exc

            self._next_lsn += 1
            if record.record_type in (
                RecordType.INSERT,
                RecordType.UPDATE,
                RecordType.DELETE,
            ):
                self._txn_first_lsn.setdefault(record.transaction_id, lsn)
            return lsn

    def append_insert(self, transaction_id: int, table: str, key: bytes, value: bytes) -> int:
        placeholder = LogRecord(
            lsn=1,
            record_type=RecordType.INSERT,
            transaction_id=transaction_id,
            table=table,
            key=key,
            new_value=value,
            old_value=None,
        )
        return self._append_record(placeholder)

    def append_update(
        self,
        transaction_id: int,
        table: str,
        key: bytes,
        old_value: bytes,
        new_value: bytes,
    ) -> int:
        placeholder = LogRecord(
            lsn=1,
            record_type=RecordType.UPDATE,
            transaction_id=transaction_id,
            table=table,
            key=key,
            new_value=new_value,
            old_value=old_value,
        )
        return self._append_record(placeholder)

    def append_delete(self, transaction_id: int, table: str, key: bytes, old_value: bytes) -> int:
        placeholder = LogRecord(
            lsn=1,
            record_type=RecordType.DELETE,
            transaction_id=transaction_id,
            table=table,
            key=key,
            new_value=None,
            old_value=old_value,
        )
        return self._append_record(placeholder)

    def commit(self, transaction_id: int) -> int:
        placeholder = LogRecord(lsn=1, record_type=RecordType.COMMIT, transaction_id=transaction_id)
        return self._append_record(placeholder)

    def abort(self, transaction_id: int) -> int:
        placeholder = LogRecord(lsn=1, record_type=RecordType.ABORT, transaction_id=transaction_id)
        return self._append_record(placeholder)

    def checkpoint(self, active_transaction_ids: Sequence[int]) -> CheckpointInfo:
        """Escribe un par CHECKPOINT_BEGIN/CHECKPOINT_END y calcula el LSN
        a partir del cual sería seguro truncar (`truncate_before`): el
        mínimo entre el propio checkpoint y el primer registro de cada
        transacción todavía activa, para no perder información de undo."""
        begin_placeholder = LogRecord(
            lsn=1,
            record_type=RecordType.CHECKPOINT_BEGIN,
            transaction_id=0,
            active_transaction_ids=tuple(active_transaction_ids),
        )
        begin_lsn = self._append_record(begin_placeholder)

        end_placeholder = LogRecord(lsn=1, record_type=RecordType.CHECKPOINT_END, transaction_id=0)
        end_lsn = self._append_record(end_placeholder)

        with self._lock:
            candidate_lsns = [begin_lsn]
            for txn_id in active_transaction_ids:
                first_lsn = self._txn_first_lsn.get(txn_id)
                if first_lsn is not None:
                    candidate_lsns.append(first_lsn)
            safe_lsn = min(candidate_lsns)

        return CheckpointInfo(begin_lsn=begin_lsn, end_lsn=end_lsn, safe_truncation_lsn=safe_lsn)

    def truncate_before(self, safe_lsn: int) -> int:
        """Reescribe el fichero del WAL conservando solo los registros con
        `lsn >= safe_lsn`. Devuelve cuántos registros se descartaron.

        La escritura se hace en un fichero temporal en el mismo directorio
        y se sustituye de forma atómica (`os.replace`), para que un crash
        a mitad de la operación deje el WAL original intacto en vez de un
        fichero a medias.
        """
        with self._lock:
            self._file.flush()
            os.fsync(self._file.fileno())

            tmp_path = self._path.with_suffix(self._path.suffix + ".compact.tmp")
            kept = 0
            discarded = 0
            try:
                with open(self._path, "rb") as reader, open(tmp_path, "wb") as writer:
                    for record in iter_records(reader):
                        if record.lsn >= safe_lsn:
                            writer.write(encode_record(record))
                            kept += 1
                        else:
                            discarded += 1
                    writer.flush()
                    os.fsync(writer.fileno())
                os.replace(tmp_path, self._path)
            except OSError as exc:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                raise WalIOError(f"fallo truncando el WAL en {self._path}: {exc}") from exc
            except TruncatedRecordError:
                # El log activo (no cerrado) puede terminar en un registro a
                # medio escribir si se llama justo tras un fallo previo sin
                # recuperar; no se trunca en ese caso para no perder datos.
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
                raise

            self._file.close()
            try:
                self._file = open(self._path, "a+b")
            except OSError as exc:
                raise WalIOError(
                    f"no se pudo reabrir el WAL tras truncar en {self._path}: {exc}"
                ) from exc

            return discarded


# --- Recuperación redo/undo --------------------------------------------------


def recover(path: Path, applier: StorageApplier) -> RecoveryReport:
    """Recupera el estado de `applier` reproduciendo el WAL en `path`.

    Algoritmo (ARIES: análisis + redo + undo, sin páginas físicas):

    1. Análisis: recorre todo el log en orden, agrupando las operaciones
       de cada transacción y anotando cuáles llegaron a tener un registro
       `COMMIT`.
    2. Redo: reaplica en orden de LSN **todas** las operaciones, sean o no
       de una transacción confirmada (`new_value`). Esto es intencional y
       no una simplificación: hay que reconstruir primero el estado exacto
       que tenía el storage justo antes de la caída (que ya reflejaba los
       cambios de transacciones todavía no confirmadas) para que el
       `old_value` de cada operación sea válido en el paso siguiente. Si
       el redo se limitase a las transacciones confirmadas, una
       transacción no confirmada que quedara "por debajo" de una
       confirmada posterior sobre la misma clave desordenaría el
       resultado final al deshacerla.
    3. Undo: para las transacciones sin `COMMIT` (abortadas explícitamente
       o interrumpidas por la caída), deshace sus operaciones en orden
       inverso de LSN usando `old_value`, sobre el estado ya reconstruido
       en el paso anterior.

    Un truncamiento de cola (`TruncatedRecordError`) o una corrupción real
    (`ChecksumMismatchError`/`InvalidRecordError`) detienen la lectura en
    ese punto: todo lo anterior, ya confirmado con `fsync`, se recupera
    igualmente. El proceso nunca se cae por esto — el problema se refleja
    en los flags del `RecoveryReport`.
    """
    committed: set[int] = set()
    operations_in_lsn_order: list[LogRecord] = []
    txn_ids_with_ops: set[int] = set()
    records_scanned = 0
    stopped_due_to_truncation = False
    stopped_due_to_corruption = False

    try:
        with open(path, "rb") as reader:
            for record in iter_records(reader):
                records_scanned += 1
                if record.record_type == RecordType.COMMIT:
                    committed.add(record.transaction_id)
                elif record.record_type in (
                    RecordType.INSERT,
                    RecordType.UPDATE,
                    RecordType.DELETE,
                ):
                    operations_in_lsn_order.append(record)
                    txn_ids_with_ops.add(record.transaction_id)
                # ABORT es solo informativo: una transacción sin COMMIT ya se
                # deshace en la fase de undo, la haya abortado alguien
                # explícitamente o la haya interrumpido la caída del proceso.
                # CHECKPOINT_BEGIN / CHECKPOINT_END no tienen efecto en el
                # redo/undo en sí: solo sirven para acotar qué se puede truncar.
    except TruncatedRecordError:
        stopped_due_to_truncation = True
    except (ChecksumMismatchError, InvalidRecordError):
        stopped_due_to_corruption = True

    # Redo: se reaplica TODO el historial en orden de LSN (ver docstring),
    # no solo las operaciones de transacciones confirmadas.
    redone = 0
    for record in operations_in_lsn_order:
        _apply_forward(applier, record)
        redone += 1

    # Undo: se deshacen, en orden inverso de LSN, solo los efectos de las
    # transacciones que nunca llegaron a confirmarse.
    rolled_back = txn_ids_with_ops - committed
    undone = 0
    for record in reversed(operations_in_lsn_order):
        if record.transaction_id in rolled_back:
            _apply_undo(applier, record)
            undone += 1

    return RecoveryReport(
        records_scanned=records_scanned,
        redone_operations=redone,
        undone_operations=undone,
        committed_transactions=frozenset(committed),
        rolled_back_transactions=frozenset(rolled_back),
        stopped_due_to_truncation=stopped_due_to_truncation,
        stopped_due_to_corruption=stopped_due_to_corruption,
    )


def _apply_forward(applier: StorageApplier, record: LogRecord) -> None:
    if record.record_type == RecordType.INSERT:
        assert record.new_value is not None
        applier.apply_insert(record.table, record.key, record.new_value)
    elif record.record_type == RecordType.UPDATE:
        assert record.new_value is not None
        applier.apply_update(record.table, record.key, record.new_value)
    elif record.record_type == RecordType.DELETE:
        applier.apply_delete(record.table, record.key)


def _apply_undo(applier: StorageApplier, record: LogRecord) -> None:
    if record.record_type == RecordType.INSERT:
        # Deshacer un insert es borrar la clave que se había insertado.
        applier.apply_delete(record.table, record.key)
    elif record.record_type == RecordType.UPDATE:
        assert record.old_value is not None
        applier.apply_update(record.table, record.key, record.old_value)
    elif record.record_type == RecordType.DELETE:
        assert record.old_value is not None
        applier.apply_insert(record.table, record.key, record.old_value)


def open_reader(path: Path) -> BinaryIO:
    """Abre el WAL solo para lectura, sin crear el fichero si no existe."""
    try:
        return open(path, "rb")
    except OSError as exc:
        raise WalIOError(f"no se pudo abrir el WAL para lectura en {path}: {exc}") from exc


def dump_records(path: Path) -> list[LogRecord]:
    """Lee todos los registros legibles de `path`, ignorando una posible
    cola truncada (uso pensado para inspección/depuración, p.ej. desde la CLI)."""
    records: list[LogRecord] = []
    buffer = io.BytesIO()
    with open(path, "rb") as reader:
        buffer.write(reader.read())
    buffer.seek(0)
    try:
        for record in iter_records(buffer):
            records.append(record)
    except TruncatedRecordError:
        pass
    return records
