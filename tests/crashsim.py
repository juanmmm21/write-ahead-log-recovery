"""Simulador de crash compartido por los tests de consistencia (no es un
`test_*.py`): un workload determinista sobre `WriteAheadLog` con un
`FaultyFileSystem`, el protocolo de checkpoint de un motor real (persistir
el storage de forma atómica y sólo después truncar el log) y la
reconstrucción tras el crash."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path

from write_ahead_log_recovery import WriteAheadLog, recover
from write_ahead_log_recovery.faultfs import FaultyFileSystem, FileSystem, SimulatedCrash

ROOT = Path("/db")
WAL = ROOT / "wal.log"
STORAGE = ROOT / "storage.json"
KEYS = [f"k{i}".encode() for i in range(6)]

State = dict[bytes, bytes]


class DictApplier:
    def __init__(self, base: State | None = None) -> None:
        self.data: State = dict(base or {})

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value

    def apply_delete(self, table: str, key: bytes) -> None:
        self.data.pop(key, None)


@dataclass
class Trace:
    """Lo que el workload consiguió antes del crash."""

    commits: list[list[tuple[str, bytes, bytes | None]]] = field(default_factory=list)
    acknowledged: int = 0  # commits cuyo `commit()` devolvió
    in_flight: bool = False  # crash DENTRO de un `commit()`
    in_flight_ops: list[tuple[str, bytes, bytes | None]] = field(default_factory=list)
    crashed: bool = False


def _persist_storage(fs: FileSystem, state: State) -> None:
    """Flush atómico del "motor de storage": temporal + fsync + rename +
    fsync del directorio. Un motor real que lo haga peor puede perder datos
    en el truncado posterior del WAL; eso lo prueban los tests de nanosql."""
    tmp = STORAGE.with_suffix(".tmp")
    handle = fs.open_write(tmp)
    handle.write(json.dumps({k.hex(): v.hex() for k, v in sorted(state.items())}).encode())
    handle.sync()
    handle.close()
    fs.replace(tmp, STORAGE)
    fs.fsync_dir(ROOT)


def load_storage(fs: FileSystem) -> State:
    if not fs.exists(STORAGE):
        return {}
    raw = json.loads(fs.read_bytes(STORAGE))
    return {bytes.fromhex(k): bytes.fromhex(v) for k, v in raw.items()}


def apply_commits(commits: list[list[tuple[str, bytes, bytes | None]]]) -> State:
    state: State = {}
    for ops in commits:
        for kind, key, value in ops:
            if kind == "delete":
                state.pop(key, None)
            else:
                assert value is not None
                state[key] = value
    return state


def run_workload(
    fs: FileSystem,
    *,
    seed: int,
    transactions: int,
    checkpoint_every: int = 3,
    trace: Trace | None = None,
    start_state: State | None = None,
) -> Trace:
    """Ejecuta `transactions` transacciones (a veces dos solapadas sobre
    claves disjuntas, a veces abortadas) y un checkpoint cada
    `checkpoint_every` commits. Se detiene en el primer `SimulatedCrash`."""
    rng = random.Random(seed)
    trace = trace or Trace()
    committed: State = dict(start_state or {})
    try:
        wal = WriteAheadLog(WAL, fs=fs)
        since_checkpoint = 0
        done = 0
        while done < transactions:
            group = 2 if rng.random() < 0.3 and transactions - done >= 2 else 1
            free = list(KEYS)
            rng.shuffle(free)
            active = []
            for _ in range(group):
                keys = [free.pop() for _ in range(rng.randint(1, 2))]
                active.append((wal.begin_transaction(), keys, dict(committed), []))
            # operaciones intercaladas entre las transacciones activas
            steps = [(i, key) for i, (_, keys, _, _) in enumerate(active) for key in keys]
            rng.shuffle(steps)
            for i, key in steps:
                txn, _, view, ops = active[i]
                value = f"{seed}:{done}:{i}:{key.decode()}:{rng.randrange(10**6)}".encode()
                if key not in view:
                    wal.append_insert(txn, "t", key, value)
                    ops.append(("insert", key, value))
                    view[key] = value
                elif rng.random() < 0.3:
                    wal.append_delete(txn, "t", key, view[key])
                    ops.append(("delete", key, None))
                    del view[key]
                else:
                    wal.append_update(txn, "t", key, view[key], value)
                    ops.append(("update", key, value))
                    view[key] = value
            for txn, _, _view, ops in active:
                done += 1
                if rng.random() < 0.15:
                    wal.abort(txn)
                    continue
                trace.in_flight = True
                trace.in_flight_ops = ops
                wal.commit(txn)
                trace.in_flight = False
                trace.commits.append(ops)
                trace.acknowledged = len(trace.commits)
                for kind, key, value in ops:
                    if kind == "delete":
                        committed.pop(key, None)
                    else:
                        assert value is not None
                        committed[key] = value
                since_checkpoint += 1
            if since_checkpoint >= checkpoint_every:
                since_checkpoint = 0
                _persist_storage(fs, committed)
                info = wal.checkpoint([])
                wal.truncate_before(info.safe_truncation_lsn)
        wal.close()
    except SimulatedCrash:
        trace.crashed = True
    return trace


def recover_image(image: dict[str, bytes]) -> tuple[FaultyFileSystem, State]:
    """Arranque tras el crash, como un motor real: storage persistido +
    redo/undo del WAL encima; luego el resultado se persiste y el log se
    reabre para seguir escribiendo."""
    fs = FaultyFileSystem(image)
    applier = DictApplier(load_storage(fs))
    if fs.exists(WAL):
        recover(WAL, applier, fs=fs)
    return fs, applier.data


def acceptable_states(trace: Trace, *, durable: bool) -> list[State]:
    """Estados válidos tras el crash: los de los prefijos de la historia de
    commits. Con fsync honesto el prefijo debe incluir todo lo reconocido;
    el commit que estaba en vuelo puede aparecer entero o no aparecer."""
    lowest = trace.acknowledged if durable else 0
    states = [
        apply_commits(trace.commits[:count]) for count in range(lowest, len(trace.commits) + 1)
    ]
    if trace.in_flight:
        states.append(apply_commits([*trace.commits, trace.in_flight_ops]))
    return states
