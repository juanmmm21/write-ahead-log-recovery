"""Consistencia ante crash con fallos de disco inyectados.

Para cada punto de E/S del workload (cada write, fsync, creación, rename,
fsync de directorio) se simula un crash justo antes y se reconstruye el
disco con cada combinación posible de lo no sincronizado: sectores de
escrituras pendientes que persisten o no (escrituras a medias y
reordenadas), truncados y operaciones de directorio. Tras recuperar:

* **lo confirmado sobrevive**: el estado contiene todas las transacciones
  cuyo `commit()` devolvió (con fsync honesto);
* **nunca aparece nada a medias**: el estado es EXACTAMENTE el de un
  prefijo de la historia de commits — ni transacciones parciales, ni
  huecos, ni efectos de abortadas;
* **se puede seguir escribiendo**: reabrir el log tras el crash, confirmar
  más transacciones y volver a caer conserva también lo nuevo.

Con un fsync que "miente" no hay durabilidad que exigir, pero el estado
debe seguir siendo un prefijo consistente.
"""

from __future__ import annotations

import itertools
import random

import pytest
from crashsim import (
    WAL,
    Trace,
    acceptable_states,
    apply_commits,
    recover_image,
    run_workload,
)

from write_ahead_log_recovery.faultfs import FaultyFileSystem, PendingUnit

EXHAUSTIVE_UNIT_LIMIT = 10


def _operation_count(seed: int, transactions: int) -> int:
    fs = FaultyFileSystem()
    trace = run_workload(fs, seed=seed, transactions=transactions)
    assert not trace.crashed
    return fs.operations


def _images(fs: FaultyFileSystem, rng: random.Random, samples: int) -> list[dict[str, bytes]]:
    units = fs.pending_units()
    if len(units) <= EXHAUSTIVE_UNIT_LIMIT:
        images = []
        for mask in itertools.product((False, True), repeat=len(units)):
            chosen = {u for u, keep in zip(units, mask, strict=True) if keep}
            images.append(fs.crash_image(lambda unit, chosen=chosen: unit in chosen))
        return images
    return [fs.random_crash_image(rng) for _ in range(samples)]


def _check(trace: Trace, state: dict[bytes, bytes], *, durable: bool, context: str) -> None:
    assert state in acceptable_states(trace, durable=durable), (
        f"{context}: estado recuperado fuera de los prefijos válidos "
        f"(reconocidos={trace.acknowledged}, en vuelo={trace.in_flight})"
    )


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_every_crash_point_and_every_pending_subset_small_workload(seed: int) -> None:
    rng = random.Random(seed)
    total = _operation_count(seed, transactions=6)
    checked = 0
    for crash_at in range(total + 1):
        fs = FaultyFileSystem(crash_at=crash_at)
        trace = run_workload(fs, seed=seed, transactions=6)
        for image in _images(fs, rng, samples=64):
            _, state = recover_image(image)
            _check(trace, state, durable=True, context=f"crash_at={crash_at}")
            checked += 1
    assert checked > total  # se recorrieron de verdad imágenes alternativas


@pytest.mark.parametrize("seed", [4, 5])
def test_restart_after_crash_keeps_old_and_new_commits(seed: int) -> None:
    """Crash, recuperar, REABRIR el log y seguir confirmando, segundo
    crash: debe sobrevivir lo reconocido de ambas sesiones."""
    rng = random.Random(seed)
    total = _operation_count(seed, transactions=5)
    for crash_at in range(total + 1):
        fs = FaultyFileSystem(crash_at=crash_at)
        first = run_workload(fs, seed=seed, transactions=5)
        image = fs.random_crash_image(rng)
        fs2, recovered = recover_image(image)
        _check(first, recovered, durable=True, context=f"primer crash en {crash_at}")
        # el estado recuperado pasa a ser la base de la segunda sesión
        base_commits = next(
            first.commits[:count]
            if count <= len(first.commits)
            else [*first.commits, first.in_flight_ops]
            for count in range(len(first.commits) + 2)
            if apply_commits(
                first.commits[:count]
                if count <= len(first.commits)
                else [*first.commits, first.in_flight_ops]
            )
            == recovered
        )
        fs2.crash_at = fs2.operations + rng.randint(0, 40)
        second = Trace(commits=list(base_commits), acknowledged=len(base_commits))
        run_workload(fs2, seed=seed + 100, transactions=4, trace=second, start_state=recovered)
        _, final = recover_image(fs2.random_crash_image(rng))
        _check(second, final, durable=True, context=f"segundo crash tras {crash_at}")


@pytest.mark.parametrize("lying", [0.0, 0.4])
def test_random_crashes_on_large_workloads(lying: float) -> None:
    """Con fsync honesto, workloads de 60 transacciones con checkpoints y
    truncados. Con un fsync que miente ninguna durabilidad es exigible — ni
    siquiera la del checkpoint (persistir storage y truncar el log puede
    perder datos si el storage nunca llegó a disco) —, así que ahí se prueba
    el log solo, sin checkpoints: el estado debe seguir siendo un prefijo
    exacto, sin nada a medias ni huecos."""
    rng = random.Random(int(lying * 10) + 7)
    checkpoint_every = 7 if lying == 0.0 else 10**9
    for iteration in range(120):
        seed = rng.randrange(10**6)
        fs = FaultyFileSystem(crash_at=rng.randint(0, 900), lying_fsync=lying, seed=seed)
        trace = run_workload(fs, seed=seed, transactions=60, checkpoint_every=checkpoint_every)
        image = fs.random_crash_image(rng, keep_probability=rng.random())
        _, state = recover_image(image)
        _check(trace, state, durable=lying == 0.0, context=f"iteración {iteration} seed {seed}")


def test_fault_model_produces_torn_and_reordered_sectors() -> None:
    fs = FaultyFileSystem()
    handle = fs.open_append(WAL)
    fs.fsync_dir(WAL.parent)
    handle.write(b"A" * 1000)  # dos sectores, sin fsync
    units = [u for u in fs.pending_units() if u.kind == "sector"]
    assert [u.sector for u in units] == [0, 1]
    only_second = fs.crash_image(lambda u: u == PendingUnit(units[1].inode, 0, 1, "sector"))
    assert only_second[str(WAL)] == b"\x00" * 512 + b"A" * 488  # hueco a ceros
    assert fs.crash_image()[str(WAL)] == b""
