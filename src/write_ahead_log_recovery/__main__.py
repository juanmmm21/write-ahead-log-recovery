"""CLI de demostración para write-ahead-log-recovery.

Subcomandos:

    begin       --path <wal>
    insert      --path <wal> --txn <id> --table <t> --key <k> --value <v>
    update      --path <wal> --txn <id> --table <t> --key <k> --old-value <v> --new-value <v>
    delete      --path <wal> --txn <id> --table <t> --key <k> --old-value <v>
    commit      --path <wal> --txn <id>
    abort       --path <wal> --txn <id>
    checkpoint  --path <wal> --active <id1,id2,...>
    dump        --path <wal>
    recover     --path <wal>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from write_ahead_log_recovery.models import RecordType, WalError
from write_ahead_log_recovery.pipeline import WriteAheadLog, dump_records, recover


class InMemoryApplier:
    """`StorageApplier` mínimo en memoria, usado para demostrar `recover`
    desde la CLI sin depender de ningún motor de storage real."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[bytes, bytes]] = {}

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.tables.setdefault(table, {})[key] = value

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.tables.setdefault(table, {})[key] = value

    def apply_delete(self, table: str, key: bytes) -> None:
        self.tables.setdefault(table, {}).pop(key, None)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="write-ahead-log-recovery")
    subparsers = parser.add_subparsers(dest="command", required=True)

    begin = subparsers.add_parser("begin", help="asigna un nuevo id de transacción")
    begin.add_argument("--path", required=True, type=Path)

    insert = subparsers.add_parser("insert", help="registra un INSERT")
    insert.add_argument("--path", required=True, type=Path)
    insert.add_argument("--txn", required=True, type=int)
    insert.add_argument("--table", required=True)
    insert.add_argument("--key", required=True)
    insert.add_argument("--value", required=True)

    update = subparsers.add_parser("update", help="registra un UPDATE")
    update.add_argument("--path", required=True, type=Path)
    update.add_argument("--txn", required=True, type=int)
    update.add_argument("--table", required=True)
    update.add_argument("--key", required=True)
    update.add_argument("--old-value", required=True)
    update.add_argument("--new-value", required=True)

    delete = subparsers.add_parser("delete", help="registra un DELETE")
    delete.add_argument("--path", required=True, type=Path)
    delete.add_argument("--txn", required=True, type=int)
    delete.add_argument("--table", required=True)
    delete.add_argument("--key", required=True)
    delete.add_argument("--old-value", required=True)

    commit = subparsers.add_parser("commit", help="confirma una transacción")
    commit.add_argument("--path", required=True, type=Path)
    commit.add_argument("--txn", required=True, type=int)

    abort = subparsers.add_parser("abort", help="marca una transacción como abortada")
    abort.add_argument("--path", required=True, type=Path)
    abort.add_argument("--txn", required=True, type=int)

    checkpoint = subparsers.add_parser("checkpoint", help="escribe un checkpoint y trunca")
    checkpoint.add_argument("--path", required=True, type=Path)
    checkpoint.add_argument(
        "--active", default="", help="ids de transacción activas separados por comas"
    )
    checkpoint.add_argument(
        "--truncate", action="store_true", help="truncar el log tras el checkpoint"
    )

    dump = subparsers.add_parser("dump", help="lista los registros del log")
    dump.add_argument("--path", required=True, type=Path)

    recover_cmd = subparsers.add_parser("recover", help="ejecuta recuperación redo/undo")
    recover_cmd.add_argument("--path", required=True, type=Path)

    return parser


def _cmd_begin(args: argparse.Namespace) -> int:
    with WriteAheadLog(args.path) as wal:
        txn_id = wal.begin_transaction()
    print(txn_id)
    return 0


def _cmd_insert(args: argparse.Namespace) -> int:
    with WriteAheadLog(args.path) as wal:
        lsn = wal.append_insert(args.txn, args.table, args.key.encode(), args.value.encode())
    print(f"LSN={lsn}")
    return 0


def _cmd_update(args: argparse.Namespace) -> int:
    with WriteAheadLog(args.path) as wal:
        lsn = wal.append_update(
            args.txn,
            args.table,
            args.key.encode(),
            args.old_value.encode(),
            args.new_value.encode(),
        )
    print(f"LSN={lsn}")
    return 0


def _cmd_delete(args: argparse.Namespace) -> int:
    with WriteAheadLog(args.path) as wal:
        lsn = wal.append_delete(args.txn, args.table, args.key.encode(), args.old_value.encode())
    print(f"LSN={lsn}")
    return 0


def _cmd_commit(args: argparse.Namespace) -> int:
    with WriteAheadLog(args.path) as wal:
        lsn = wal.commit(args.txn)
    print(f"LSN={lsn}")
    return 0


def _cmd_abort(args: argparse.Namespace) -> int:
    with WriteAheadLog(args.path) as wal:
        lsn = wal.abort(args.txn)
    print(f"LSN={lsn}")
    return 0


def _cmd_checkpoint(args: argparse.Namespace) -> int:
    active_ids = [int(x) for x in args.active.split(",") if x.strip()]
    with WriteAheadLog(args.path) as wal:
        info = wal.checkpoint(active_ids)
        print(
            f"begin_lsn={info.begin_lsn} end_lsn={info.end_lsn} "
            f"safe_truncation_lsn={info.safe_truncation_lsn}"
        )
        if args.truncate:
            discarded = wal.truncate_before(info.safe_truncation_lsn)
            print(f"discarded={discarded}")
    return 0


def _cmd_dump(args: argparse.Namespace) -> int:
    for record in dump_records(args.path):
        print(
            f"lsn={record.lsn} type={RecordType(record.record_type).name} "
            f"txn={record.transaction_id} table={record.table!r} key={record.key!r} "
            f"new_value={record.new_value!r} old_value={record.old_value!r} "
            f"active_txns={record.active_transaction_ids}"
        )
    return 0


def _cmd_recover(args: argparse.Namespace) -> int:
    applier = InMemoryApplier()
    report = recover(args.path, applier)
    print(
        f"records_scanned={report.records_scanned} "
        f"redone={report.redone_operations} undone={report.undone_operations} "
        f"committed={sorted(report.committed_transactions)} "
        f"rolled_back={sorted(report.rolled_back_transactions)} "
        f"stopped_due_to_truncation={report.stopped_due_to_truncation} "
        f"stopped_due_to_corruption={report.stopped_due_to_corruption}"
    )
    for table, rows in applier.tables.items():
        for key, value in rows.items():
            print(f"state: {table}[{key!r}] = {value!r}")
    return 0


_HANDLERS = {
    "begin": _cmd_begin,
    "insert": _cmd_insert,
    "update": _cmd_update,
    "delete": _cmd_delete,
    "commit": _cmd_commit,
    "abort": _cmd_abort,
    "checkpoint": _cmd_checkpoint,
    "dump": _cmd_dump,
    "recover": _cmd_recover,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _HANDLERS[args.command]
    try:
        return handler(args)
    except WalError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
