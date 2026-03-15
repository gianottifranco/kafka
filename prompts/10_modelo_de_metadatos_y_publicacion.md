# Modelo de metadatos, imágenes inmutables y publicación a brokers

Actúa como arquitecto del subsistema de metadatos. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Definir el modelo de metadatos del clúster, cómo se materializa en imágenes/snapshots inmutables y cómo se publica a brokers, coordinadores y herramientas sin crear puntos de contención.

## Tareas
- [ ] Diseña el esquema de metadatos para topics, particiones, brokers, controllers, ACLs, SCRAM, cuotas, features, tenants y namespaces.
- [ ] Implementa un modelo `metadata image` + `delta` + `publisher` equivalente o mejor que el actual.
- [ ] Define snapshots inmutables y mecanismos eficientes de publicación/consumo.
- [ ] Modela la separación entre metadata operativa global y metadata específica de tenant.
- [ ] Establece cómo se serializan snapshots y checkpoints del metadata log.
- [ ] Define API interna para consultas rápidas de metadata desde broker, coordinadores, CLI y shell.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Piensa en el modelo de metadata como una base de datos inmutable y versionada, no como mapas mutables compartidos.
- La publicación de metadata debe ser lock-light para lectura y segura para writers del controller.
- Introduce `TenantRecord`, `NamespaceRecord`, `QuotaPlanRecord` o equivalentes como entidades de primer nivel si necesitas multitenencia real.
- Evalúa si conviene particionar lógicamente el espacio de metadatos por tenant, aunque siga existiendo un único quorum físico.

### Código original de Kafka a revisar
- `metadata`
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`
- `shell/src/main/java/org/apache/kafka/shell/MetadataShell.java`

### Multi-tenencia y límites de recursos
- Permite políticas y configuraciones heredables: defaults globales -> tenant -> namespace -> topic.
- Separa metadata sensible y metadata visible por tenant para facilitar RBAC y auditoría.
- Optimiza consultas frecuentes por tenant/namespace para evitar scans globales y contención.

### Crates y utilidades sugeridas
- `arc-swap` para publicación lock-free de snapshots.
- `serde`/formatos binarios propios para snapshots si aporta rendimiento y control.
- `im` o estructuras persistentes si realmente simplifican el modelo, pero mide su coste.

## Entregables mínimos
- Esquema de metadatos versionado.
- Contratos de image/delta/publisher.
- Diseño de snapshots y consultas.
- Plan de evolución del esquema para tenancy.

## Posibles dificultades
- Modelar metadata multi-tenant como simples prefijos de nombres y perder semántica administrativa.
- Hacer copias completas costosas en cada actualización.
- No definir claramente quién puede ver/modificar qué metadata.

## Referencias
- [ControllerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ControllerServer.scala)
- [QuorumController.java](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java)
- [MetadataShell.java](https://github.com/apache/kafka/blob/trunk/shell/src/main/java/org/apache/kafka/shell/MetadataShell.java)
- [arc-swap](https://docs.rs/arc-swap/latest/arc_swap/)
