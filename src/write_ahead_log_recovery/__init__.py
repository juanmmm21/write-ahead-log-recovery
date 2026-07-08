"""write-ahead-log-recovery: WAL binario con checkpointing y recuperación redo/undo."""

from write_ahead_log_recovery.models import (
    CheckpointInfo,
    ChecksumMismatchError,
    InvalidRecordError,
    LogCorruptionError,
    LogRecord,
    RecordType,
    RecoveryReport,
    TruncatedRecordError,
    WalError,
    WalIOError,
)
from write_ahead_log_recovery.pipeline import WriteAheadLog, iter_records, recover
from write_ahead_log_recovery.protocols import StorageApplier

__all__ = [
    "ChecksumMismatchError",
    "CheckpointInfo",
    "InvalidRecordError",
    "LogCorruptionError",
    "LogRecord",
    "RecordType",
    "RecoveryReport",
    "StorageApplier",
    "TruncatedRecordError",
    "WalError",
    "WalIOError",
    "WriteAheadLog",
    "iter_records",
    "recover",
]

__version__ = "0.1.0"
