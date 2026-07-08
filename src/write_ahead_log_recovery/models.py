"""Tipos de datos del formato de registro binario del write-ahead log.

El WAL es lógico (no atado a páginas físicas): cada registro describe una
operación (insert/update/delete) sobre un par (tabla, clave) más las
imágenes "antes"/"después" necesarias para redo y undo, siguiendo el
esquema clásico de logging de ARIES pero simplificado para un motor
educativo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum

# Tamaño de página no aplica aquí (el WAL es un log lógico de registros de
# longitud variable, no páginas de tamaño fijo). El invariante de este
# módulo es el de CLAUDE.md: ningún registro se considera durable hasta
# pasar por fsync.
MAGIC = b"WLR1"
"""Marca de formato + versión al inicio de cada registro, para detectar
registros corruptos o de un formato incompatible."""


class RecordType(IntEnum):
    """Tipo de operación registrada en el WAL."""

    INSERT = 1
    UPDATE = 2
    DELETE = 3
    COMMIT = 4
    ABORT = 5
    CHECKPOINT_BEGIN = 6
    CHECKPOINT_END = 7


@dataclass(frozen=True, slots=True)
class LogRecord:
    """Un registro individual del write-ahead log, ya decodificado.

    `new_value` es la imagen "después" de la operación (usada para redo).
    `old_value` es la imagen "antes" (usada para undo). `active_transaction_ids`
    solo tiene contenido en registros `CHECKPOINT_BEGIN`.
    """

    lsn: int
    record_type: RecordType
    transaction_id: int
    table: str = ""
    key: bytes = b""
    new_value: bytes | None = None
    old_value: bytes | None = None
    active_transaction_ids: tuple[int, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.lsn < 1:
            raise ValueError(f"LSN debe ser >= 1, recibido {self.lsn}")
        if self.transaction_id < 0:
            raise ValueError(f"transaction_id debe ser >= 0, recibido {self.transaction_id}")


@dataclass(frozen=True, slots=True)
class CheckpointInfo:
    """Resultado de crear un checkpoint: LSNs de interés para truncar el log."""

    begin_lsn: int
    end_lsn: int
    safe_truncation_lsn: int
    """LSN a partir del cual es seguro truncar: ningún registro anterior
    a este LSN pertenece a una transacción todavía activa."""


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """Resumen de una pasada de recuperación redo/undo."""

    records_scanned: int
    redone_operations: int
    undone_operations: int
    committed_transactions: frozenset[int]
    rolled_back_transactions: frozenset[int]
    stopped_due_to_truncation: bool
    stopped_due_to_corruption: bool


class WalError(Exception):
    """Error base de todo el módulo."""


class WalIOError(WalError):
    """Error de E/S al persistir o truncar el log (disco lleno, permisos...).

    Envuelve el `OSError` original para no perder el contexto pero exponer
    un tipo propio del módulo, tal como exige CLAUDE.md: nunca un
    `except Exception: pass` genérico ante fallos de escritura a disco.
    """


class LogCorruptionError(WalError):
    """Clase base para cualquier problema de integridad detectado al leer."""


class TruncatedRecordError(LogCorruptionError):
    """Un registro quedó incompleto en disco: la cola del log fue cortada
    a mitad de escritura (el escenario típico de un proceso matado durante
    un `append`). No implica pérdida de registros anteriores, ya
    confirmados y con `fsync`."""


class ChecksumMismatchError(LogCorruptionError):
    """Un registro está completo en tamaño pero su checksum no coincide:
    corrupción real de bytes ya escritos (bit flip, sector dañado...),
    distinta de un truncamiento por caída a mitad de escritura."""


class InvalidRecordError(LogCorruptionError):
    """La cabecera de un registro no empieza por el `MAGIC` esperado."""
