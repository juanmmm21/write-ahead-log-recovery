# write-ahead-log-recovery

**Componente 1 de 3 en Almacenamiento y Persistencia** del ecosistema [`strata-database-engine`](https://github.com/juanmmm21/strata-database-engine).
Repo: [`github.com/juanmmm21/write-ahead-log-recovery`](https://github.com/juanmmm21/write-ahead-log-recovery)

Un write-ahead log (WAL) binario, escrito desde cero en Python, con checkpointing periódico y recuperación redo/undo tras la caída de un proceso. Es el cimiento de durabilidad del que dependen los dos motores de almacenamiento del ecosistema ([`bplus-tree-storage-engine`](https://github.com/juanmmm21/bplus-tree-storage-engine) y [`lsm-tree-engine`](https://github.com/juanmmm21/lsm-tree-engine)) y la unidad de replicación que usará [`raft-replication-log`](https://github.com/juanmmm21/raft-replication-log).

---

## Qué es y qué problema resuelve

Ningún motor de base de datos puede garantizar que un dato confirmado sobrevive a una caída del proceso si no escribe antes una descripción de ese cambio en un log secuencial y la fuerza a disco (`fsync`) antes de responder "confirmado" al cliente. Esa es la regla de *write-ahead logging*: **ninguna página o fila se considera durable hasta que su registro correspondiente está en el WAL y ha pasado por `fsync`.**

Este proyecto implementa esa pieza de forma aislada y verificable: un formato de registro binario con checksum, un escritor que asigna LSNs (*log sequence numbers*) monotónicos y sincroniza cada escritura a disco, y un procedimiento de recuperación que reconstruye el estado correcto de un motor de almacenamiento externo tras un crash — ya sea a mitad de una escritura (truncamiento) o por corrupción de bytes ya escritos.

## Rol en `strata-database-engine`

```text
                         ┌────────────────────────────┐
                         │  write-ahead-log-recovery   │   (este repo)
                         │  LSN · fsync · redo/undo     │
                         └──────────────┬───────────────┘
                                        │ implementa StorageApplier
                         ┌──────────────┼───────────────┐
                         ▼                              ▼
          ┌──────────────────────────┐    ┌──────────────────────────┐
          │ bplus-tree-storage-engine │    │     lsm-tree-engine      │
          └──────────────────────────┘    └──────────────────────────┘
                         │                              │
                         └──────────────┬───────────────┘
                                        ▼
                              mvcc-transaction-manager /
                              lock-manager-deadlock-detector
                                        │
                                        ▼
                                     nanosql
```

Este repo no importa ni depende de ningún otro subproyecto del ecosistema: expone su formato binario y el `Protocol` `StorageApplier` como contrato de integración. La integración real (un motor de storage que reproduce los registros del WAL para reconstruir su estado) ocurre dentro de `nanosql`.

## Objetivo / skills demostradas

- Diseño de un formato de registro binario versionado, con checksum de integridad y campos de longitud variable.
- Invariante de durabilidad WAL-antes-que-datos y `fsync` explícito.
- Algoritmo de recuperación **redo/undo estilo ARIES** (análisis, redo completo del historial, undo de transacciones perdedoras) sin páginas físicas.
- Checkpointing con cálculo del LSN seguro de truncado y compactación atómica del fichero (`os.replace`).
- Manejo explícito de fallos de E/S (disco lleno, permisos, truncamiento a mitad de escritura) con excepciones tipadas propias, nunca genéricas.
- Tests de propiedades con secuencias aleatorias de semilla fija, y tests dedicados de simulación de crash (truncamiento) y de corrupción (checksum inválido).

## Cómo funciona

### Formato de registro

Todos los enteros se codifican en big-endian:

```text
+----------+---------+------+-----------------+----------------+------------+----------+
| MAGIC(4) | LSN(8)  | tipo | transaction_id  | payload_len(4) | payload(N) | crc32(4) |
|  "WLR1"  |         | (1)  |       (8)        |                |            |          |
+----------+---------+------+-----------------+----------------+------------+----------+
```

El `payload` codifica, con longitud explícita por campo: la tabla (`str`), la clave (`bytes`), el valor "después" (`new_value`, opcional), el valor "antes" (`old_value`, opcional) y — solo en registros de checkpoint — la lista de ids de transacciones activas. El checksum CRC32 cubre cabecera + payload completos, por lo que cualquier bit corrompido en cualquier campo se detecta.

Tipos de registro (`RecordType`): `INSERT`, `UPDATE`, `DELETE`, `COMMIT`, `ABORT`, `CHECKPOINT_BEGIN`, `CHECKPOINT_END`.

### Escritura durable

Cada llamada a `append_insert` / `append_update` / `append_delete` / `commit` / `abort` asigna el siguiente LSN de forma atómica, serializa el registro, escribe los bytes, hace `flush()` y `os.fsync()` **antes de devolver el LSN al llamador** — el registro no se considera confirmado hasta ese punto.

### Recuperación (redo/undo tipo ARIES)

1. **Análisis:** se recorre el log completo, anotando qué transacciones llegaron a tener un registro `COMMIT`.
2. **Redo:** se reaplica en orden de LSN **todo** el historial de operaciones — no solo el de las transacciones confirmadas. Esto reconstruye el estado exacto que tenía el storage justo antes de la caída (que ya reflejaba escrituras de transacciones todavía no confirmadas), condición necesaria para que el `old_value` de cada operación sea válido en el siguiente paso. Limitar el redo a las transacciones confirmadas rompe la reconstrucción cuando una transacción no confirmada y una confirmada posterior tocan la misma clave.
3. **Undo:** las transacciones sin `COMMIT` (abortadas explícitamente o interrumpidas por la caída) se deshacen en orden inverso de LSN usando el `old_value` de cada una de sus operaciones, sobre el estado ya reconstruido en el paso anterior.

Un truncamiento de cola (`TruncatedRecordError`, registro a medio escribir) o una corrupción real (`ChecksumMismatchError` / `InvalidRecordError`) detienen la lectura en ese punto exacto; todo lo anterior — ya confirmado con `fsync` — se recupera igualmente. El proceso nunca se cae por esto: el problema queda reflejado en los flags `stopped_due_to_truncation` / `stopped_due_to_corruption` del `RecoveryReport`.

### Checkpointing y truncado seguro

`checkpoint(active_transaction_ids)` escribe un par `CHECKPOINT_BEGIN`/`CHECKPOINT_END` y calcula `safe_truncation_lsn`: el mínimo entre el propio checkpoint y el LSN del primer registro de cada transacción todavía activa. `truncate_before(safe_lsn)` reescribe el fichero conservando solo los registros con `lsn >= safe_lsn`, escribiendo a un fichero temporal y sustituyéndolo de forma atómica (`os.replace`) — un crash a mitad de la compactación deja el WAL original intacto.

## Arquitectura

```text
src/write_ahead_log_recovery/
├── __init__.py     # API pública reexportada
├── models.py       # RecordType, LogRecord, CheckpointInfo, RecoveryReport, excepciones
├── protocols.py     # StorageApplier: contrato que implementan los motores de storage
├── pipeline.py       # (de)serialización binaria, WriteAheadLog, recover()
└── __main__.py       # CLI de demostración
```

- **`models.py`** — tipos de datos inmutables (`dataclass(frozen=True, slots=True)`) y la jerarquía de excepciones (`WalError` → `WalIOError` / `LogCorruptionError` → `TruncatedRecordError` / `ChecksumMismatchError` / `InvalidRecordError`).
- **`protocols.py`** — `StorageApplier`, el único punto de acoplamiento con un motor de storage real.
- **`pipeline.py`** — toda la lógica: codificación/decodificación binaria, `WriteAheadLog` (escritor con `fsync`, checkpoint y truncado) y `recover()` (función pura de recuperación).
- **`__main__.py`** — CLI de demostración con subcomandos, más un `StorageApplier` en memoria (`InMemoryApplier`) para ilustrar `recover` sin depender de ningún motor real.

**Concurrencia:** `WriteAheadLog` protege con un único `threading.Lock` el LSN siguiente, el id de transacción siguiente y la posición de escritura del fichero — el diseño asume un único escritor lógico por fichero de WAL (varios hilos pueden invocar `append_*` concurrentemente, pero la serialización a disco es siempre secuencial).

## Requisitos e instalación

- Python `>=3.11`

```bash
git clone https://github.com/juanmmm21/write-ahead-log-recovery.git
cd write-ahead-log-recovery
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"  # o: pip install -e . pytest mypy ruff
```

## Uso

### CLI

```bash
# Empezar una transacción, insertar, confirmar
txn=$(python -m write_ahead_log_recovery begin --path /tmp/demo.wal)
python -m write_ahead_log_recovery insert --path /tmp/demo.wal --txn "$txn" \
    --table users --key alice --value v1
python -m write_ahead_log_recovery commit --path /tmp/demo.wal --txn "$txn"

# Inspeccionar el log
python -m write_ahead_log_recovery dump --path /tmp/demo.wal

# Recuperar (redo/undo) sobre un StorageApplier en memoria de demostración
python -m write_ahead_log_recovery recover --path /tmp/demo.wal

# Checkpoint + truncado
python -m write_ahead_log_recovery checkpoint --path /tmp/demo.wal --active "" --truncate
```

### Uso programático

```python
from pathlib import Path
from write_ahead_log_recovery import WriteAheadLog, recover

class DictApplier:
    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}
    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value
    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value
    def apply_delete(self, table: str, key: bytes) -> None:
        self.data.pop(key, None)

wal_path = Path("demo.wal")
with WriteAheadLog(wal_path) as wal:
    txn = wal.begin_transaction()
    wal.append_insert(txn, "users", b"alice", b"v1")
    wal.commit(txn)

applier = DictApplier()
report = recover(wal_path, applier)
print(applier.data, report)
```

## Formato de datos / interfaz expuesta a `nanosql`

Cualquier motor de storage que quiera reproducir los efectos de este WAL implementa el `Protocol` `StorageApplier` (`apply_insert`, `apply_update`, `apply_delete`) y se lo pasa a `recover(path, applier)`. El formato binario del fichero (`MAGIC = b"WLR1"`) está versionado en la cabecera de cada registro para poder evolucionarlo sin romper logs existentes.

## Desarrollo

```bash
pytest
ruff check .
ruff format --check .
mypy --strict src/
```

La suite de tests cubre: round-trip de serialización binaria para los 7 tipos de registro, detección de truncamiento en distintos puntos de corte, detección de corrupción por checksum sin interrumpir el proceso, redo/undo básico, checkpoint + truncado seguro, y tests de propiedades con cargas aleatorias de semilla fija que comparan el estado recuperado contra un oráculo de referencia.

## Benchmarks

No aplica en esta fase: el objetivo de este subproyecto es la correctness del invariante de durabilidad y de la recuperación, no el rendimiento. `bplus-tree-storage-engine` y `lsm-tree-engine`, que sí tienen presión de rendimiento real, incluirán sus propios benchmarks.

## Troubleshooting

- **`ChecksumMismatchError` al leer un WAL existente:** hay corrupción real de bytes ya escritos (no un truncamiento). `recover()` no lanza esta excepción — la refleja en `RecoveryReport.stopped_due_to_corruption` y detiene la lectura en ese punto, conservando todo lo anterior. Si se necesita el detalle exacto, se puede invocar `iter_records` directamente, que sí la propaga.
- **`WalIOError` al hacer `append_*` o `truncate_before`:** fallo real de E/S (disco lleno, permisos). El mensaje incluye la ruta y el `OSError` original.
- **El WAL no crece tras varios `checkpoint`:** `checkpoint()` por sí solo no trunca nada — solo calcula `safe_truncation_lsn`. Hay que llamar explícitamente a `truncate_before(info.safe_truncation_lsn)` (o usar `--truncate` en la CLI).

## Roadmap

- [ ] Publicar en `nanosql` un adaptador `StorageApplier` real sobre `bplus-tree-storage-engine` y `lsm-tree-engine`.
- [ ] Checkpointing automático por umbral de tamaño de log, en vez de solo bajo demanda.
- [ ] Métricas de tamaño de log / tasa de truncado expuestas por la CLI.

## Licencia

MIT — ver [`LICENSE`](./LICENSE).
