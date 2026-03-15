# Controller, gestión del quorum y publicación de cambios al clúster

Actúa como arquitecto del controller del clúster. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Reescribir el controller del clúster, incluyendo registro de brokers/controllers, heartbeats, reassignments, cambios de features, aplicación de ACLs/cuotas/configs y coordinación con el metadata quorum.

## Tareas
- [ ] Diseña el `ControllerServer` en Rust y su relación con el módulo Raft.
- [ ] Reimplementa la cola/event loop del controller con prioridades, deadlines y ejecución determinista cuando sea posible.
- [ ] Implementa broker registration, controller registration, heartbeats, fencing/unfencing y control de epochs.
- [ ] Define el pipeline para create/alter/delete topics, repartición de particiones, reassignment y cambios de config.
- [ ] Modela publicación de ACLs, SCRAM, cuotas y features hacia brokers y coordinadores.
- [ ] Añade herramientas para inspección y operación del quorum.
- [ ] Documenta el contrato entre controller y broker para propagación de cambios y manejo de versiones.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El controller debe ser pequeño, predecible y extremadamente observable.
- La cola de eventos debe distinguir operaciones baratas de operaciones pesadas, evitando head-of-line blocking.
- Piensa en `publishers` especializados: ACLs, SCRAM, quotas, topic configs, broker lifecycle, features.
- Preserva la separación de roles broker/controller recomendada para producción.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/scala/kafka/server/KafkaRaftServer.scala`

### Multi-tenencia y límites de recursos
- El controller debe soportar metadata y políticas multi-tenant como entidades de primer nivel.
- Las operaciones de tenants con gran volumen administrativo no deben impedir heartbeats/fencing del clúster.
- Aplica cuotas también al plano administrativo: tasa de mutaciones de metadata por tenant/namespace y tamaños máximos de lotes.

### Crates y utilidades sugeridas
- `tokio`, `tracing`, colas explícitas, timers de precisión razonable, snapshots inmutables de metadata.

## Entregables mínimos
- Diseño del event loop y colas del controller.
- Tabla de comandos administrativos y sus efectos de metadata.
- Plan de publicación a brokers y validación de applied state.
- Pruebas de failover y fencing.

## Posibles dificultades
- Construir un controller demasiado generalista y con rutas críticas largas.
- Procesar mutaciones administrativas pesadas en el mismo carril que heartbeats o cambios de liderazgo.
- No separar mutación, persistencia y publicación como fases explícitas.

## Referencias
- [ControllerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ControllerServer.scala)
- [QuorumController.java](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java)
- [KRaft docs](https://kafka.apache.org/41/operations/kraft/)
