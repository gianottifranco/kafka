# Raft/KRaft nativo en Rust para el metadata quorum

Actúa como arquitecto del consenso y del metadata log. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar e implementar un módulo Raft en Rust que replique la funcionalidad esencial de KRaft: metadata log, elección de líder, replicación, snapshots, cambios de miembros y publicación segura al controller.

## Tareas
- [ ] Define el modelo del metadata log, términos/epochs, voters/observers, snapshotting y checkpoints.
- [ ] Implementa RPCs y estados de Raft equivalentes a los necesarios para KRaft.
- [ ] Define el contrato exacto entre el módulo Raft y la máquina de estados de metadata.
- [ ] Soporta arranque limpio, recovery desde disco, rejoin al quorum y cambios de membresía.
- [ ] Decide si vas a construir un módulo propio o a evaluar bibliotecas como `openraft` solo como referencia; documenta por qué.
- [ ] Asegura que el controller pueda operar sobre snapshots e incrementales sin inconsistencias.
- [ ] Define mecanismos de fencing y de aislamiento entre tráfico de metadatos y tráfico de datos.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El módulo debe ser interno al sistema; no debe introducir un servicio externo equivalente a ZooKeeper o etcd.
- Diseña una interfaz clara: append de registros de metadata, lectura de HWM, instalación de snapshots, notificación de liderazgo y cambios de membresía.
- Piensa desde el inicio en dynamic controller membership y en herramientas equivalentes a `kafka-metadata-quorum.sh`.
- El metadata quorum es un recurso crítico; debe tener reservas de CPU, disco y memoria distintas al data plane.

### Código original de Kafka a revisar
- `raft/src/main/java/org/apache/kafka/raft/RaftClient.java`
- `raft/src/main/java/org/apache/kafka/raft/LeaderState.java`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`
- `core/src/main/scala/kafka/server/KafkaRaftServer.scala`
- `core/src/main/scala/kafka/server/ControllerServer.scala`

### Multi-tenencia y límites de recursos
- Aísla físicamente o lógicamente recursos del quorum de metadatos; no debe competir en igualdad con produce/fetch de tenants.
- Si el sistema soporta varios dominios de metadata o namespaces con fuerte aislamiento, evalúa particionamiento lógico del espacio de metadatos sobre un solo quorum o varios quorums internos.
- Expón métricas de lag, HWM, snapshot install time y cambios de membresía con separación por dominio/control-plane.

### Crates y utilidades sugeridas
- `tokio`, `bytes`, `crc32c`, almacenamiento secuencial propio, `openraft` solo como comparación/benchmark si procede.
- `tracing` para logs de consenso con correlación por epoch/term.

## Entregables mínimos
- Especificación del protocolo interno de Raft.
- Máquina de estados y persistencia de quorum.
- Plan de snapshots e instalación.
- Criterios para cambios de membresía y failover.

## Posibles dificultades
- Intentar generalizar demasiado Raft y perder alineación con necesidades concretas de Kafka.
- No reservar recursos suficientes al metadata quorum.
- Diseñar cambios de miembros sin considerar seguridad operativa ni tooling.

## Referencias
- [RaftClient.java](https://github.com/apache/kafka/blob/trunk/raft/src/main/java/org/apache/kafka/raft/RaftClient.java)
- [LeaderState.java](https://github.com/apache/kafka/blob/trunk/raft/src/main/java/org/apache/kafka/raft/LeaderState.java)
- [KafkaRaftServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaRaftServer.scala)
- [KRaft docs](https://kafka.apache.org/41/operations/kraft/)
- [Metadata quorum tool docs](https://kafka.apache.org/40/operations/kraft/)
- [openraft](https://docs.rs/openraft/latest/openraft/)
