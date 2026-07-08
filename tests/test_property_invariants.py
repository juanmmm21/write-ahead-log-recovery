"""Tests de propiedades: secuencias aleatorias de operaciones (semilla fija)
verificando invariantes estructurales del log y de la recuperación."""

from __future__ import annotations

import random
from pathlib import Path

from write_ahead_log_recovery.models import RecordType
from write_ahead_log_recovery.pipeline import WriteAheadLog, dump_records, recover

_KEYS = [f"key-{i}".encode() for i in range(30)]
_SEEDS = [1, 2, 3, 17, 42, 1000]


class _OracleApplier:
    """Espejo en memoria de `InMemoryApplier`, usado como resultado esperado."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[bytes, bytes]] = {}

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.tables.setdefault(table, {})[key] = value

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.tables.setdefault(table, {})[key] = value

    def apply_delete(self, table: str, key: bytes) -> None:
        self.tables.setdefault(table, {}).pop(key, None)


def _run_random_workload(wal_path: Path, seed: int, num_transactions: int = 40) -> _OracleApplier:
    """Genera una carga aleatoria válida (con semilla fija) de transacciones.

    Una transacción que no llega a confirmarse simula quedar activa hasta
    que el proceso se cae al final del todo: en un sistema real, un gestor
    de locks (`lock-manager-deadlock-detector`) o MVCC le garantizarían
    exclusividad sobre las claves que toca hasta que confirme o aborte. Por
    eso, una vez que una clave queda "colgada" de una transacción sin
    confirmar, se excluye del resto de la carga: ninguna otra transacción
    puede tocarla, igual que no podría hacerlo con locking real de por
    medio. Sin esta exclusión, la historia generada no sería serializable
    y ni siquiera un motor de recuperación correcto podría reconstruirla.
    """
    rng = random.Random(seed)
    oracle = _OracleApplier()
    live_state: dict[bytes, bytes] = {}  # estado "real" según se van escribiendo los registros
    locked_keys: set[bytes] = set()  # claves de transacciones que quedaron sin confirmar

    with WriteAheadLog(wal_path) as wal:
        for _ in range(num_transactions):
            available_keys = [k for k in _KEYS if k not in locked_keys]
            if not available_keys:
                break

            txn = wal.begin_transaction()
            num_ops = rng.randint(1, min(4, len(available_keys)))
            txn_ops: list[tuple[str, bytes, bytes | None]] = []
            touched_keys: set[bytes] = set()
            for _ in range(num_ops):
                candidates = [k for k in available_keys if k not in touched_keys]
                if not candidates:
                    break
                key = rng.choice(candidates)
                touched_keys.add(key)
                existing = live_state.get(key)
                if existing is None:
                    value = f"v-{rng.randint(0, 1_000_000)}".encode()
                    wal.append_insert(txn, "t", key, value)
                    live_state[key] = value
                    txn_ops.append(("insert", key, value))
                elif rng.random() < 0.5:
                    value = f"v-{rng.randint(0, 1_000_000)}".encode()
                    wal.append_update(txn, "t", key, existing, value)
                    live_state[key] = value
                    txn_ops.append(("update", key, value))
                else:
                    wal.append_delete(txn, "t", key, existing)
                    del live_state[key]
                    txn_ops.append(("delete", key, None))

            will_commit = rng.random() < 0.7
            if will_commit:
                wal.commit(txn)
                for kind, key, value in txn_ops:
                    if kind == "insert" or kind == "update":
                        assert value is not None
                        oracle.tables.setdefault("t", {})[key] = value
                    else:
                        oracle.tables.setdefault("t", {}).pop(key, None)
            else:
                locked_keys.update(touched_keys)

    return oracle


def test_random_workloads_recover_to_the_committed_only_state(tmp_path: Path) -> None:
    for seed in _SEEDS:
        wal_path = tmp_path / f"wal-{seed}.log"
        oracle = _run_random_workload(wal_path, seed)

        applier = _OracleApplier()
        report = recover(wal_path, applier)

        assert applier.tables == oracle.tables, f"seed={seed}"
        assert not report.stopped_due_to_truncation
        assert not report.stopped_due_to_corruption


def test_random_workloads_have_strictly_increasing_lsns(tmp_path: Path) -> None:
    for seed in _SEEDS:
        wal_path = tmp_path / f"wal-{seed}.log"
        _run_random_workload(wal_path, seed)

        records = dump_records(wal_path)
        lsns = [r.lsn for r in records]
        assert lsns == sorted(lsns), f"seed={seed}"
        assert len(set(lsns)) == len(lsns), f"seed={seed}: LSNs duplicados"


def test_random_workloads_never_lose_a_committed_write(tmp_path: Path) -> None:
    """Invariante de durabilidad: toda operación de una transacción
    confirmada se recupera correctamente y ninguna transacción confirmada
    queda pendiente de deshacer."""
    for seed in _SEEDS:
        wal_path = tmp_path / f"wal-{seed}.log"
        _run_random_workload(wal_path, seed)

        records = dump_records(wal_path)
        committed_txns = {r.transaction_id for r in records if r.record_type == RecordType.COMMIT}
        total_ops = sum(
            1
            for r in records
            if r.record_type in (RecordType.INSERT, RecordType.UPDATE, RecordType.DELETE)
        )

        applier = _OracleApplier()
        report = recover(wal_path, applier)

        # El redo reaplica absolutamente todo el historial (ver docstring de
        # `recover`), así que su recuento coincide con el total de operaciones.
        assert report.redone_operations == total_ops, f"seed={seed}"
        assert report.committed_transactions == committed_txns, f"seed={seed}"
        assert report.rolled_back_transactions.isdisjoint(committed_txns), f"seed={seed}"
