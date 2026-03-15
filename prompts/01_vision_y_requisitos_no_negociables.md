# Visión del programa y requisitos no negociables

Actúa como director técnico del programa de reescritura. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Definir el marco del proyecto antes de escribir una sola línea de código. Establece qué comportamientos de Kafka deben preservarse, qué partes pueden rediseñarse y qué garantías operativas, de compatibilidad, de aislamiento multi-tenant y de control de recursos son obligatorias para considerar exitosa la reescritura.

## Tareas
- [ ] Redacta una declaración de objetivos y no objetivos del producto.
- [ ] Define una matriz de compatibilidad: protocolo wire, formato de registros, semántica de `acks`, ISR, idempotencia, transacciones, offsets, ACLs y herramientas administrativas.
- [ ] Fija SLOs iniciales para disponibilidad, latencia p50/p99, durabilidad, tiempos de recuperación y aislamiento entre tenants.
- [ ] Decide si la primera entrega soportará roles separados de broker y controller, y declara explícitamente que el modo combinado es solo para desarrollo o entornos pequeños.
- [ ] Establece principios de diseño para Rust: seguridad de memoria, ownership claro, mínimo `unsafe`, separación estricta entre plano de control, plano de datos y tareas de mantenimiento.
- [ ] Define el contrato multi-tenant mínimo: `TenantId`, `NamespaceId`, cuotas jerárquicas, políticas de retención independientes y aislamiento de observabilidad.
- [ ] Establece criterios para aceptar dependencias Rust y criterios para rechazar dependencias que introduzcan complejidad operativa o lock-in.
- [ ] Fija el formato de salida esperado para los siguientes prompts: ADR, diagrama, invariantes, riesgos, pruebas y plan de migración.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Formula la visión como una arquitectura modular compuesta por un `metadata quorum`, brokers de datos, coordinadores, clientes y herramientas.
- Haz explícito que no se puede sustituir el log segmentado de Kafka por una base de datos genérica ni delegar el consenso a etcd, Consul o servicios gestionados.
- Preserva la semántica de los identificadores y conceptos clave: topic, partition, leader epoch, ISR, HW, LEO, transactional id, consumer group, ACL y quota.
- Define un principio de diseño importante: toda funcionalidad multi-tenant debe ser medible, limitable y observable por tenant.
- Declara que el control de recursos será jerárquico: clúster -> tenant -> namespace -> topic/grupo/cliente.
- Establece que las operaciones que bloqueen CPU o disco se encapsularán con colas limitadas y `spawn_blocking` acotado por semáforos.

### Código original de Kafka a revisar
- `README.md`
- `settings.gradle`
- `core/src/main/scala/kafka/server/KafkaRaftServer.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`

### Multi-tenencia y límites de recursos
- Declara tres niveles de aislamiento soportados desde el diseño: soft multi-tenancy (cuotas y namespaces), hard multi-tenancy (aislamiento operativo dentro de un clúster compartido) y dedicated tenancy (aislamiento por clúster o pool de nodos).
- Exige que toda API administrativa pueda operar con filtros por tenant/namespace, evitando escaneos globales innecesarios.
- Define desde el principio cómo se repartirán presupuestos de CPU, memoria de page cache, memoria heap/off-heap, IOPS y almacenamiento.
- Incluye una política de degradación: ante presión de recursos, primero throttling, luego backpressure, luego rechazo explícito; nunca degradación silenciosa de la durabilidad.

### Crates y utilidades sugeridas
- `tokio` para el runtime principal; no mezclar runtimes.
- `tracing` y `tracing-subscriber` para observabilidad estructurada.
- `thiserror` o `miette` para errores con contexto.
- `arc-swap`, `parking_lot` y `dashmap` solo donde aporten beneficios claros y medidos.

## Entregables mínimos
- Documento de visión del sistema de 3-5 páginas.
- Lista de invariantes del sistema y de compatibilidad.
- Tabla de objetivos/no objetivos.
- Árbol preliminar de cuotas y entidades multi-tenant.
- Criterios de aceptación global del programa.

## Posibles dificultades
- Confundir compatibilidad wire con compatibilidad total de implementación interna.
- Diseñar multi-tenancy demasiado tarde y terminar acoplando cuotas a parches locales.
- Sobreoptimizar muy pronto y comprometer la legibilidad o la seguridad de concurrencia.
- Permitir extensiones incompatibles del protocolo para introducir tenancy; debe evitarse salvo con KIP equivalente y fuerte justificación.

## Referencias
- [Repositorio oficial de Apache Kafka](https://github.com/apache/kafka)
- [README del repositorio](https://github.com/apache/kafka/blob/trunk/README.md)
- [KafkaRaftServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaRaftServer.scala)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [ControllerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ControllerServer.scala)
- [QuorumController.java](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java)
- [KRaft](https://kafka.apache.org/41/operations/kraft/)
