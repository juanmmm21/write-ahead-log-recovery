# AGENTS.md — write-ahead-log-recovery

Hereda reglas globales de `../AGENTS.md` y `../CLAUDE.md`.

## Posición en el ecosistema

| Campo | Valor |
|---|---|
| Categoría | 1. Almacenamiento y Persistencia |
| Repo (cuando exista) | `https://github.com/juanmmm21/write-ahead-log-recovery.git` |

Log de escritura anticipada (WAL) binario con checkpointing periódico y recuperación redo/undo tras la caída del proceso. Es el cimiento de durabilidad de todo el ecosistema: ningún otro subproyecto de storage se considera correcto si no respeta el invariante WAL definido en `../CLAUDE.md`.

## Integración con el resto del ecosistema

- **Consume:** nada — es la base de la que dependen los demás.
- **Produce:** un log binario de registros con LSN (log sequence number) monotónico, `fsync`eado antes de confirmar cualquier escritura, que `bplus-tree-storage-engine` y `lsm-tree-engine` usan para garantizar durabilidad, y que `raft-replication-log` usa como unidad de replicación entre nodos.
- La integración real (que otro subproyecto importe y use este WAL) ocurre dentro de `nanosql`, no mediante imports cruzados directos entre estos repos.

## Stack

- Python >=3.11, `pyproject.toml` + `hatchling`
- `mypy --strict`, `ruff` (format + lint), `pytest`
- Estructura: `src/write_ahead_log_recovery/{models,protocols,pipeline,__main__}.py`, `tests/`

## Definition of Done

- [ ] Formato de registro binario con LSN monotónico, checksum y tipo de operación (insert/update/delete/checkpoint)
- [ ] `fsync` explícito antes de confirmar cualquier escritura como durable
- [ ] Checkpointing periódico que trunca el log de forma segura sin perder registros no aplicados
- [ ] Recuperación redo (reaplica registros confirmados no reflejados en el storage) y undo (revierte cambios de transacciones no confirmadas)
- [ ] Tests que simulan un crash a mitad de escritura (truncamiento del archivo) y verifican recuperación parcial correcta
- [ ] Tests que simulan corrupción de un registro (checksum inválido) y verifican detección sin crash del proceso

## Git

Carpeta y repo deben compartir nombre (`write-ahead-log-recovery`). No se ejecuta ningún comando git hasta que el repo remoto exista y Juan lo indique.
</content>
