# Limpieza de logs, retención y compaction

Actúa como arquitecto de mantenimiento de logs y políticas de retención. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar las políticas de limpieza de logs, compaction, delete retention, truncado y borrado de segmentos, respetando semántica de Kafka y evitando que el mantenimiento de fondo afecte injustamente a otros tenants.

## Tareas
- [ ] Implementa retención por tiempo, tamaño y políticas híbridas.
- [ ] Reescribe el limpiador de logs compactados, incluyendo mapas de offsets y tratamiento especial de tombstones.
- [ ] Define las reglas de secciones limpia, sucia, cleanable y uncleanable del log.
- [ ] Diseña throttling para compaction, borrado y truncado.
- [ ] Soporta configuración por topic, por namespace y por tenant para retención, compactación y delete retention.
- [ ] Asegura que las tareas de limpieza puedan abortarse o replanificarse ante truncados, cambios de liderazgo o presión de recursos.
- [ ] Diseña un scheduler que combine urgencia de limpieza, fairness entre tenants y salud del nodo.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El `LogCleaner.java` documenta la semántica de obsolescencia por clave y las sutilezas con productores idempotentes/transactional.
- Separa claramente metadata del scheduler de limpieza de la ejecución pesada de IO.
- Usa snapshots de configuración para evitar que cambios dinámicos corrompan decisiones a mitad de una limpieza.
- Considera que la limpieza es intensiva en CPU, memoria y disco; trata sus budgets como ciudadanos de primera clase.

### Código original de Kafka a revisar
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java`
- `core/src/main/scala/kafka/log/LogManager.scala`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`

### Multi-tenencia y límites de recursos
- Cada tenant debe tener presupuesto independiente de compaction/retention IO y CPU.
- Evita que un tenant con grandes topics compactados monopolice los threads o el ancho de banda de disco.
- Permite clases de servicio: tenants premium con throughput mínimo de limpieza, tenants best-effort con ventanas más amplias.
- Expón métricas de backlog de limpieza, `dirty ratio`, bytes limpiados y tiempo en cola por tenant.

### Crates y utilidades sugeridas
- `governor` para throttling lógico, `tokio` + semáforos para concurrency control.
- `spawn_blocking` limitado para checksum, compresión y merges costosos.

## Entregables mínimos
- Diseño del scheduler de limpieza y sus prioridades.
- Modelo de fairness multi-tenant para mantenimiento.
- Criterios de abort/retry/replanificación.
- Matriz de configuraciones de retención y compaction.

## Posibles dificultades
- Subestimar el coste de compaction en page cache y CPU.
- Borrar segmentos demasiado agresivamente y comprometer recuperación o clientes lentos.
- No incorporar awareness de liderazgo/truncado y limpiar sobre datos ya invalidados.

## Referencias
- [LogCleaner.java](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java)
- [LogManager.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/log/LogManager.scala)
- [governor](https://docs.rs/governor/latest/governor/)
