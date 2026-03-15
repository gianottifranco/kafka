# Almacenamiento segmentado, índices y recuperación tras fallo

Actúa como arquitecto del motor de almacenamiento del broker. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Implementar en Rust la capa de log segmentado de Kafka, incluyendo layout en disco, índices, rolling, recovery, validación, checkpoints y soporte para alta tasa de escritura/lectura con semántica equivalente a Kafka.

## Tareas
- [ ] Diseña el layout en disco de topics/partitions y directorios, incluyendo directorios futuros, IDs de directorio y metadatos de clean shutdown.
- [ ] Implementa `UnifiedLog` o un equivalente que orqueste segmentos, índices, checkpoints y producer state.
- [ ] Reescribe el concepto de `LogSegment` con `.log`, `.index`, `.timeindex`, índices transaccionales y checkpoints necesarios.
- [ ] Diseña recuperación tras crash: re-scan de segmentos activos, truncado, reconstrucción de índices, validación de CRC y reconciliación con metadata.
- [ ] Decide cuándo usar `mmap`, cuándo `pread/pwrite` y cómo evitar page faults patológicos.
- [ ] Implementa segment roll por tiempo/tamaño y políticas de flush/fsync configurables.
- [ ] Añade soporte para límites por tenant: espacio reservado, presupuesto de segmentos activos, control de fragmentación y accounting exacto.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- `LogManager.scala` es el orquestador del subsistema; `LogSegment.java` describe el contrato del segmento e índices.
- Define claramente qué operaciones están protegidas por locks locales por partición y cuáles pueden ser lock-free o copy-on-write.
- Asegura que los índices toleren reconstrucción y que el sistema pueda arrancar aunque un índice auxiliar esté corrupto.
- Diseña la relación entre page cache, buffers temporales y budgets de memoria del tenant.
- Prepara desde el inicio hooks para tiered/remote storage aunque la primera versión los deje desactivados.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/log/LogManager.scala`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java`
- `core/src/main/scala/kafka/server/BrokerServer.scala`

### Multi-tenencia y límites de recursos
- El disco debe presupuestarse por tenant y por namespace, con hard limits y reservas opcionales.
- No permitas que un tenant agote descriptores, page cache caliente o espacio de índices de todo el nodo.
- Considera colas de IO por tenant para recuperación, roll y flush, especialmente durante reinicios con alta densidad de particiones.
- Expón métricas de bytes en disco, segmentos, flush latency, recovery time y corrupción detectada por tenant.

### Crates y utilidades sugeridas
- `memmap2`, `crc32c`, `bytes`, `nix`/`libc` solo si necesitas control fino de fsync/fadvise.
- `tokio::task::spawn_blocking` para tareas de disco y compresión pesadas.
- `tempfile` para escrituras atómicas de índices y checkpoints.

## Entregables mínimos
- Diseño del layout en disco y naming de archivos.
- Especificación de invariantes de segmentos e índices.
- Plan de recuperación y reconstrucción.
- Modelo de accounting de almacenamiento por tenant.

## Posibles dificultades
- Hacer demasiado `mmap` y perder control sobre memoria efectiva.
- Asumir que los índices siempre están sanos y no diseñar reconstrucción robusta.
- No separar IO foreground (produce/fetch) de IO background (cleanup/recovery).

## Referencias
- [LogManager.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/log/LogManager.scala)
- [LogSegment.java](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java)
- [LogCleaner.java](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java)
- [memmap2](https://docs.rs/memmap2/latest/memmap2/)
- [crc32c](https://docs.rs/crc32c/latest/crc32c/)
