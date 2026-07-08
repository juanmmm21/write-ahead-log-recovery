"""Interfaces que los motores de storage (consumidores del WAL) implementan.

`bplus-tree-storage-engine` y `lsm-tree-engine` no se importan directamente
desde este repositorio (la integración real ocurre dentro de `nanosql`);
en su lugar, cualquier motor que quiera reproducir el efecto de un registro
del WAL durante la recuperación implementa este `Protocol`.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class StorageApplier(Protocol):
    """Destino de la fase de redo/undo: aplica el efecto de un registro
    del WAL sobre el estado real de un motor de almacenamiento."""

    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        """Inserta `value` bajo `key` en `table`, sobrescribiendo si existe."""
        ...

    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        """Actualiza el valor de `key` en `table` a `value`."""
        ...

    def apply_delete(self, table: str, key: bytes) -> None:
        """Elimina `key` de `table` si existe (idempotente)."""
        ...
