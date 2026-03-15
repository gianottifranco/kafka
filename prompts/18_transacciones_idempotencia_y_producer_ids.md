# Transacciones, idempotencia y gestión de producer IDs

Actúa como arquitecto del subsistema transaccional. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Reescribir la lógica de idempotencia, transacciones, producer IDs, fencing, markers y coordinación transaccional sin perder compatibilidad semántica con Kafka.

## Tareas
- [ ] Define el módulo de `ProducerIdManager` y la asignación/renovación segura de producer IDs.
- [ ] Implementa el coordinator transaccional y el state log asociado.
- [ ] Soporta `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`, `WriteTxnMarkers` y caminos de error/fencing.
- [ ] Preserva semántica de productores idempotentes y de transacciones exactamente-una-vez.
- [ ] Diseña recovery del estado transaccional y de los markers tras fallo.
- [ ] Define límites por tenant para número de transactional IDs, sesiones abiertas, timeouts y fanout de markers.
- [ ] Documenta interacción con compaction, fetch read_committed y coordinadores de grupos.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El estado transaccional es especialmente sensible a errores de concurrencia y recuperación.
- Piensa en `fencing` como una semántica central, no como un error accesorio.
- Modela claramente los logs internos y la relación entre coordinator, storage y replica manager.
- Aísla el coste de markers y validaciones transaccionales de tenants no transaccionales.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala`
- `core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala`
- `clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`

### Multi-tenencia y límites de recursos
- Establece cuotas sobre cantidad de transactional IDs, duración máxima efectiva y fanout de particiones por transacción por tenant.
- Evita que tenants con uso intensivo de transacciones degraden latencia global de produce.
- Expón métricas de fencing, aborts, commit latency y markers pendientes por tenant.

### Crates y utilidades sugeridas
- `tokio`, timers, persistencia estructurada y pruebas de recuperación/consistencia muy agresivas.

## Entregables mínimos
- Máquina de estados transaccional.
- Diseño del log interno y recovery.
- Matriz de compatibilidad con productores idempotentes/transaccionales.
- Plan de cuotas y protección multi-tenant.

## Posibles dificultades
- Modelar mal los epochs de producer y generar duplicados o pérdida semántica.
- No probar recovery tras abortos parciales, fencing y fallos de red.
- Permitir fanout transaccional no acotado por tenant.

## Referencias
- [TransactionCoordinator.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala)
- [TransactionStateManager.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala)
- [TransactionManager.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
