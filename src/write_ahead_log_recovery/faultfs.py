"""Capa de ficheros del WAL: la real (`OsFileSystem`) y un modelo de disco
con fallos inyectables (`FaultyFileSystem`) para probar la recuperación en
cada punto de escritura.

`WriteAheadLog` y `recover` hacen TODA su E/S a través de un `FileSystem`.
El modelo de fallos sigue a la literatura sobre consistencia ante crash
(Pillai et al., "All File Systems Are Not Created Equal", OSDI 2014):

* **Caché de página volátil.** Cada `write` queda pendiente hasta el
  `sync` (fsync) de su fichero; un crash pierde lo pendiente, salvo lo que
  el disco ya hubiera escrito por su cuenta.
* **Escrituras a medias por sector.** Una escritura pendiente se parte en
  sectores de 512 bytes (alineados al offset absoluto); en el crash cada
  sector persiste o no de forma independiente (un sector sí es atómico).
* **Reordenación de escrituras no sincronizadas.** Como cada sector decide
  por separado, puede sobrevivir uno posterior y perderse uno anterior; los
  huecos se leen como ceros.
* **fsync que miente.** Con probabilidad `lying_fsync`, `sync` devuelve
  éxito sin hacer nada durable (caché de escritura del disco que ignora los
  flush): esas escrituras quedan pendientes y pueden perderse.
* **Metadatos de directorio.** Crear, renombrar o borrar sólo es durable
  tras `fsync_dir` del directorio; en el crash sobrevive un prefijo de las
  operaciones de espacio de nombres no sincronizadas.

Un `FaultyFileSystem(crash_at=k)` lanza `SimulatedCrash` justo antes de su
k-ésima operación mutante; `crash_image()` construye entonces el disco que
vería un reinicio, y `pending_units()` enumera las decisiones posibles para
recorrerlas de forma exhaustiva en workloads pequeños.
"""

from __future__ import annotations

import os
import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Protocol

SECTOR_SIZE = 512


class FileHandle(Protocol):
    def write(self, data: bytes) -> None: ...

    def sync(self) -> None:
        """flush + fsync del contenido del fichero."""
        ...

    def truncate(self, size: int) -> None: ...

    def size(self) -> int: ...

    def close(self) -> None: ...


class FileSystem(Protocol):
    def exists(self, path: Path) -> bool: ...

    def read_bytes(self, path: Path) -> bytes: ...

    def open_append(self, path: Path) -> FileHandle:
        """Abre para añadir al final, creando el fichero si no existe."""
        ...

    def open_write(self, path: Path) -> FileHandle:
        """Crea (o vacía) un fichero para escribirlo desde el principio."""
        ...

    def replace(self, source: Path, target: Path) -> None: ...

    def unlink(self, path: Path) -> None: ...

    def fsync_dir(self, directory: Path) -> None: ...


# ---------------------------------------------------------------------------
# Sistema de ficheros real
# ---------------------------------------------------------------------------


class _OsHandle:
    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle

    def write(self, data: bytes) -> None:
        self._handle.write(data)

    def sync(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def truncate(self, size: int) -> None:
        self._handle.flush()
        self._handle.truncate(size)
        self._handle.seek(0, os.SEEK_END)

    def size(self) -> int:
        self._handle.flush()
        return os.fstat(self._handle.fileno()).st_size

    def close(self) -> None:
        self._handle.close()


class OsFileSystem:
    def exists(self, path: Path) -> bool:
        return path.exists()

    def read_bytes(self, path: Path) -> bytes:
        return path.read_bytes()

    def open_append(self, path: Path) -> FileHandle:
        return _OsHandle(open(path, "a+b"))  # noqa: SIM115 — lo cierra el WAL

    def open_write(self, path: Path) -> FileHandle:
        return _OsHandle(open(path, "wb"))  # noqa: SIM115

    def replace(self, source: Path, target: Path) -> None:
        os.replace(source, target)

    def unlink(self, path: Path) -> None:
        path.unlink(missing_ok=True)

    def fsync_dir(self, directory: Path) -> None:
        try:
            fd = os.open(directory, os.O_RDONLY)
        except OSError:
            return  # plataformas sin fsync de directorios (Windows)
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# Modelo de disco con fallos
# ---------------------------------------------------------------------------


class SimulatedCrash(Exception):  # noqa: N818 — es un evento, no un error del WAL
    """El "proceso" murió justo antes de esta operación."""


@dataclass(slots=True)
class _PendingOp:
    kind: str  # "write" | "truncate"
    offset: int = 0
    data: bytes = b""


@dataclass(slots=True)
class _Inode:
    durable: bytearray = field(default_factory=bytearray)
    pending: list[_PendingOp] = field(default_factory=list)

    def visible(self) -> bytearray:
        buf = bytearray(self.durable)
        for op in self.pending:
            _apply(buf, op)
        return buf


def _apply(buf: bytearray, op: _PendingOp) -> None:
    if op.kind == "truncate":
        del buf[op.offset :]
        return
    end = op.offset + len(op.data)
    if len(buf) < op.offset:
        buf.extend(b"\x00" * (op.offset - len(buf)))
    buf[op.offset : end] = op.data


@dataclass(frozen=True, slots=True)
class PendingUnit:
    """Una decisión independiente del crash: si persiste un sector concreto
    de una escritura pendiente, un truncado pendiente, o la n-ésima
    operación de espacio de nombres no sincronizada."""

    inode: int
    op_index: int
    sector: int  # -1 para truncados y operaciones de directorio
    kind: str  # "sector" | "truncate" | "namespace"


class _FaultyHandle:
    def __init__(self, fs: FaultyFileSystem, inode: int) -> None:
        self._fs = fs
        self._inode = inode
        self._closed = False

    def write(self, data: bytes) -> None:
        self._fs.tick("write")
        inode = self._fs.inodes[self._inode]
        offset = len(inode.visible())
        inode.pending.append(_PendingOp("write", offset, bytes(data)))

    def sync(self) -> None:
        self._fs.tick("fsync")
        if self._fs.rng.random() < self._fs.lying_fsync:
            self._fs.lied += 1
            return  # fsync que miente: nada pasa a ser durable
        inode = self._fs.inodes[self._inode]
        inode.durable = inode.visible()
        inode.pending.clear()

    def truncate(self, size: int) -> None:
        self._fs.tick("truncate")
        self._fs.inodes[self._inode].pending.append(_PendingOp("truncate", size))

    def size(self) -> int:
        return len(self._fs.inodes[self._inode].visible())

    def close(self) -> None:
        self._closed = True


class FaultyFileSystem:
    def __init__(
        self,
        image: Mapping[str, bytes] | None = None,
        *,
        crash_at: int | None = None,
        lying_fsync: float = 0.0,
        seed: int = 0,
    ) -> None:
        self.inodes: dict[int, _Inode] = {}
        self.namespace: dict[str, int] = {}
        self._durable_namespace: dict[str, int] = {}
        # operaciones de directorio desde el último fsync_dir, en orden
        self._namespace_log: list[tuple[str, str, int | None]] = []
        self._next_inode = 1
        for path, content in (image or {}).items():
            inode = self._new_inode(bytearray(content))
            self.namespace[path] = inode
        self._durable_namespace = dict(self.namespace)
        self.crash_at = crash_at
        self.operations = 0
        self.lying_fsync = lying_fsync
        self.lied = 0
        self.rng = random.Random(seed)

    # -- contabilidad de puntos de crash -----------------------------------

    def tick(self, _kind: str) -> None:
        if self.crash_at is not None and self.operations >= self.crash_at:
            raise SimulatedCrash(f"crash antes de la operación #{self.operations}")
        self.operations += 1

    def _new_inode(self, content: bytearray | None = None) -> int:
        number = self._next_inode
        self._next_inode += 1
        self.inodes[number] = _Inode(durable=content if content is not None else bytearray())
        return number

    def _bind(self, path: str, inode: int | None) -> None:
        if inode is None:
            self.namespace.pop(path, None)
        else:
            self.namespace[path] = inode
        self._namespace_log.append(("bind", path, inode))

    # -- FileSystem ----------------------------------------------------------

    def exists(self, path: Path) -> bool:
        return str(path) in self.namespace

    def read_bytes(self, path: Path) -> bytes:
        try:
            return bytes(self.inodes[self.namespace[str(path)]].visible())
        except KeyError as exc:
            raise FileNotFoundError(str(path)) from exc

    def open_append(self, path: Path) -> FileHandle:
        key = str(path)
        if key not in self.namespace:
            self.tick("create")
            self._bind(key, self._new_inode())
        return _FaultyHandle(self, self.namespace[key])

    def open_write(self, path: Path) -> FileHandle:
        self.tick("create")
        self._bind(str(path), self._new_inode())
        return _FaultyHandle(self, self.namespace[str(path)])

    def replace(self, source: Path, target: Path) -> None:
        self.tick("rename")
        inode = self.namespace[str(source)]
        self._bind(str(source), None)
        self._bind(str(target), inode)

    def unlink(self, path: Path) -> None:
        if str(path) in self.namespace:
            self.tick("unlink")
            self._bind(str(path), None)

    def fsync_dir(self, directory: Path) -> None:
        self.tick("fsync_dir")
        self._durable_namespace = dict(self.namespace)
        self._namespace_log.clear()

    # -- imagen tras el crash -----------------------------------------------

    def pending_units(self) -> list[PendingUnit]:
        units: list[PendingUnit] = []
        for number, inode in sorted(self.inodes.items()):
            for index, op in enumerate(inode.pending):
                if op.kind == "truncate":
                    units.append(PendingUnit(number, index, -1, "truncate"))
                    continue
                first = op.offset // SECTOR_SIZE
                last = (op.offset + max(len(op.data), 1) - 1) // SECTOR_SIZE
                for sector in range(first, last + 1):
                    units.append(PendingUnit(number, index, sector, "sector"))
        for position in range(len(self._namespace_log)):
            units.append(PendingUnit(0, position, -1, "namespace"))
        return units

    def crash_image(self, keep: Callable[[PendingUnit], bool] | None = None) -> dict[str, bytes]:
        """Disco tras el crash. `keep(unidad)` decide qué persiste de lo no
        sincronizado; por defecto, nada (el caso más destructivo). Para el
        espacio de nombres sobrevive el prefijo de operaciones hasta la
        primera que `keep` rechace (los metadatos se escriben en orden)."""
        decide = keep or (lambda _unit: False)
        namespace = dict(self._durable_namespace)
        for position, (_, path, bound) in enumerate(self._namespace_log):
            if not decide(PendingUnit(0, position, -1, "namespace")):
                break
            if bound is None:
                namespace.pop(path, None)
            else:
                namespace[path] = bound
        image: dict[str, bytes] = {}
        for path, number in namespace.items():
            inode = self.inodes[number]
            buf = bytearray(inode.durable)
            for index, op in enumerate(inode.pending):
                if op.kind == "truncate":
                    if decide(PendingUnit(number, index, -1, "truncate")):
                        _apply(buf, op)
                    continue
                first = op.offset // SECTOR_SIZE
                last = (op.offset + max(len(op.data), 1) - 1) // SECTOR_SIZE
                for sector in range(first, last + 1):
                    if not decide(PendingUnit(number, index, sector, "sector")):
                        continue
                    start = max(op.offset, sector * SECTOR_SIZE)
                    end = min(op.offset + len(op.data), (sector + 1) * SECTOR_SIZE)
                    _apply(
                        buf,
                        _PendingOp("write", start, op.data[start - op.offset : end - op.offset]),
                    )
            image[path] = bytes(buf)
        return image

    def random_crash_image(
        self, rng: random.Random, keep_probability: float = 0.5
    ) -> dict[str, bytes]:
        decisions: dict[PendingUnit, bool] = {}

        def keep(unit: PendingUnit) -> bool:
            if unit not in decisions:
                decisions[unit] = rng.random() < keep_probability
            return decisions[unit]

        return self.crash_image(keep)
