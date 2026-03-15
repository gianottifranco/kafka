# Cuotas, aislamiento de recursos y Quality of Service

Actúa como arquitecto del engine de quotas y resource governance. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar el motor jerárquico de límites y garantías de recursos del sistema, cubriendo CPU, memoria, almacenamiento, IOPS, throughput de red, conexiones, requests y trabajos de fondo por tenant y por clúster.

## Tareas
- [ ] Define el árbol de quotas y budgets: global -> tenant -> namespace -> topic/grupo/cliente.
- [ ] Distingue límites duros, límites blandos, garantías mínimas y clases de prioridad.
- [ ] Diseña algoritmos de enforcement para CPU, memoria, requests por segundo, bytes por segundo, conexiones, IO de disco y trabajos background.
- [ ] Implementa admission control, rate limiting, backpressure y scheduling justo.
- [ ] Evalúa integración con cgroup v2 (`cpu.max`, `memory.high`, `memory.max`, `io.max`) y fallback a enforcement a nivel de aplicación.
- [ ] Define cómo se presupuestan `spawn_blocking`, limpiadores, compaction, recovery, snapshots y replicación.
- [ ] Expón APIs administrativas para cambiar límites por tenant y observar consumo en tiempo real.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- No basta con quotas de produce/fetch; necesitas budgets de memoria en colas, tasks, page cache y storage.
- Modela separately foreground vs background budgets.
- Usa token buckets/hierarchical token buckets para throughput y weighted fair queues para scheduling.
- Define un `resource accounting context` común a red, storage y coordinadores.
- Integra señales de presión (PSI, cgroup stats, disco casi lleno, latencia de flush) para adaptar throttling.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/scala/kafka/server/KafkaApis.scala`
- `core/src/main/scala/kafka/server/KafkaConfig.scala`
- `metadata` (client quotas y config records)

### Multi-tenencia y límites de recursos
- Este prompt es el corazón del diseño multi-tenant. Debe producir un modelo operacional completo, no solo límites por request.
- Asegura que tenants premium puedan tener garantías mínimas sin destruir la eficiencia de agregación del clúster.
- Permite burst controlado y fairness bajo saturación.

### Crates y utilidades sugeridas
- `governor`, semáforos Tokio, métricas PSI/cgroups, `nix` o acceso directo a fs para cgroup v2 si es necesario.

## Entregables mínimos
- Especificación del árbol de quotas y semántica de enforcement.
- Diseño del scheduler y clases de servicio.
- APIs de administración y observabilidad de consumos.
- Plan de pruebas de fairness y noisy-neighbor.

## Posibles dificultades
- Confiar únicamente en cgroups sin accounting a nivel de aplicación.
- Aplicar límites demasiado tarde en el pipeline.
- No separar tráfico foreground de mantenimiento y control plane.

## Referencias
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [KafkaApis.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala)
- [cgroup v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
- [Kubernetes resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Kubernetes QoS](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/)
- [governor](https://docs.rs/governor/latest/governor/)
