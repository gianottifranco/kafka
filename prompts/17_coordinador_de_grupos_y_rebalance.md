# Coordinador de grupos, heartbeats y protocolos de rebalance

Actúa como arquitecto del group coordinator y del protocolo de consumo. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Reescribir el subsistema de coordinación de grupos, offsets y heartbeats, preservando compatibilidad con los protocolos de grupos de Kafka y preparando el camino para el protocolo moderno de rebalance.

## Tareas
- [ ] Diseña el equivalente en Rust de `GroupCoordinatorService` y su runtime interno.
- [ ] Implementa gestión de grupos clásicos, consumer groups modernos, offsets y heartbeats.
- [ ] Modela el log interno de offsets/estado de grupos y su integración con metadata y storage.
- [ ] Define el pipeline de validación, autorización, balanceo y persistencia para `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetCommit`, `OffsetFetch`, `ConsumerGroupHeartbeat`, `Describe*`.
- [ ] Asegura operación correcta durante failover, cambio de coordinator y recovery.
- [ ] Explicita cómo se publicará y aplicará metadata de topics/subscriptions necesaria para el coordinator.
- [ ] Añade cuotas y fairness por tenant, por grupo y por namespace.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El `group-coordinator` actual ya desacopla parte del coordinator en un runtime dedicado: úsalo como guía conceptual.
- Debes decidir si el estado del coordinator vive en topics internos, shards dedicados o una abstracción equivalente alineada con Kafka.
- Distingue claramente grupos clásicos, grupos modernos y offsets transaccionales.
- Piensa en reparto de shards del coordinator entre brokers y en reubicación tras cambios de leadership.

### Código original de Kafka a revisar
- `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java`
- `core/src/main/scala/kafka/server/KafkaApis.scala`
- `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java`
- `clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java`

### Multi-tenencia y límites de recursos
- Permite cuotas por número de grupos, tamaño de grupos, tasa de heartbeats y bytes de commits por tenant.
- Evita que un tenant con miles de grupos genere tormentas de rebalance que afecten a todos.
- Aísla métricas y diagnósticos de grupos por tenant y namespace.

### Crates y utilidades sugeridas
- `tokio`, timers, colas internas por shard, snapshots de metadata, persistencia estructurada.

## Entregables mínimos
- Modelo de shard del coordinator.
- Máquinas de estado de grupos y heartbeats.
- Plan de compatibilidad con grupos clásicos y modernos.
- Pruebas de rebalance y failover.

## Posibles dificultades
- No distinguir suficientemente entre el log del coordinator y el log de datos del usuario.
- Sobreacoplar el coordinator al broker local y dificultar reubicación/failover.
- Ignorar tormentas de heartbeats/rebalances multi-tenant.

## Referencias
- [GroupCoordinatorService.java](https://github.com/apache/kafka/blob/trunk/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java)
- [KafkaApis.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala)
- [KafkaConsumer.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java)
