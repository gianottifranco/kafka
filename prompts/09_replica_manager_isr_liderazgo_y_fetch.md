# ReplicaManager, ISR, liderazgo de particiones y fetch de replicación

Actúa como arquitecto del plano de datos replicado. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Reescribir la lógica de `ReplicaManager`, estado de particiones, ISR, high watermark, leader epochs, produce/fetch, delayed operations y transición de roles leader/follower.

## Tareas
- [ ] Diseña el estado en memoria de cada partición: líder, réplicas, ISR, HW, LEO, leader epoch, partition epoch y cambios pendientes.
- [ ] Implementa flujos de `Produce` y `Fetch` equivalentes a Kafka, incluyendo `acks`, aislamiento read_committed y errores de liderazgo.
- [ ] Modela follower fetch y avance de ISR/high watermark.
- [ ] Decide cómo reimplementar delayed operations o purgatories para produce/fetch/delete/list-offsets.
- [ ] Asegura que cambios de metadata y liderazgo se apliquen atómicamente respecto a las rutas de IO.
- [ ] Define interacción con remote/tiered storage si se habilita después.
- [ ] Añade quotas y fairness específicos para replicación frente a tráfico de clientes.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- `ReplicaManager.scala` concentra gran parte de la semántica operativa del broker; debes preservar sus invariantes aunque cambie la implementación.
- Los cambios de metadata deben materializarse como transiciones explícitas del estado de partición para evitar carreras.
- Distingue entre request path de clientes, path de replicación inter-broker y path de coordinadores.
- El cálculo de ISR y HW debe mantenerse correcto incluso bajo reinicios, lag transitorio y cambios frecuentes de leadership.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/server/ReplicaManager.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`
- `core/src/main/scala/kafka/server/KafkaApis.scala`

### Multi-tenencia y límites de recursos
- Reserva ancho de banda y slots de IO para replicación, de modo que un tenant ruidoso no provoque under-replication sistémica.
- Permite cuotas de replicación por tenant/namespace cuando se pueda atribuir un topic a un tenant.
- Mide backlog de replicación, tiempo fuera de ISR, bytes retrasados y duración de delayed operations por tenant.

### Crates y utilidades sugeridas
- `tokio`, `futures`, estructuras lock-sharded o actors por partición cuando simplifiquen invariantes.
- `arc-swap` para publicar snapshots de metadata consumibles por workers de replicación.

## Entregables mínimos
- Máquina de estados de partición y documento de invariantes.
- Secuencias de liderazgo/cambio de ISR.
- Plan de implementación de purgatories o equivalente async.
- Pruebas de consistencia y lag.

## Posibles dificultades
- Romper invariantes de HW/ISR bajo alta concurrencia.
- Modelar liderazgo solo como flags y no como máquina de estados robusta.
- No reservar recursos para replicación y degradar disponibilidad ante tenants ruidosos.

## Referencias
- [ReplicaManager.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ReplicaManager.scala)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [KafkaApis.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala)
