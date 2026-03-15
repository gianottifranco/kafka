# Inventario del código actual y mapeo a crates de Rust

Actúa como arquitecto de migración y analista del repositorio actual. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Crear un mapa exhaustivo entre los módulos actuales de Kafka y la futura estructura del workspace en Rust, identificando qué se reescribe, qué se replica semánticamente y qué se rediseña por completo.

## Tareas
- [ ] Recorre los módulos activos del repositorio (`clients`, `core`, `metadata`, `raft`, `storage`, `server`, `server-common`, `group-coordinator`, `share-coordinator`, `tools`, `shell`, etc.).
- [ ] Produce una tabla que mapee cada módulo Java/Scala a uno o más crates de Rust.
- [ ] Separa claramente piezas del plano de datos, del plano de metadatos/control y de utilidades de línea de comandos.
- [ ] Identifica las clases/archivos que deben leerse primero para cada gran subsistema.
- [ ] Marca qué código es semánticamente crítico y qué código es solo infraestructura o compatibilidad histórica.
- [ ] Define dependencias internas entre crates y qué fronteras deben imponerse para evitar ciclos.
- [ ] Añade una columna de impacto multi-tenant: dónde vive la política de cuotas, dónde vive el enforcement y dónde solo se consume metadata.
- [ ] Propón un orden de reescritura por capas para reducir riesgos.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Usa `settings.gradle` como inventario vivo de módulos del proyecto.
- Distingue entre componentes que generan mensajes/protocolo, componentes de almacenamiento, controladores, coordinadores y herramientas.
- Propón un workspace con crates como `kafka-protocol`, `kafka-network`, `kafka-storage`, `kafka-raft`, `kafka-metadata`, `kafka-broker`, `kafka-group-coordinator`, `kafka-txn`, `kafka-admin-cli`, etc.
- Evita que el crate de red dependa del crate de storage; usa traits y capas intermedias.
- Identifica límites de ownership para que los readers de metadata puedan publicarse como snapshots inmutables.

### Código original de Kafka a revisar
- `settings.gradle`
- `build.gradle`
- `clients/src/main/resources/common/message/README.md`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`
- `raft/src/main/java/org/apache/kafka/raft/RaftClient.java`
- `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java`
- `shell/src/main/java/org/apache/kafka/shell/MetadataShell.java`

### Multi-tenencia y límites de recursos
- Etiqueta explícitamente qué crates deben ser tenant-aware y cuáles solo deben recibir metadatos ya resueltos.
- Incluye en el mapeo un `quota-engine` o módulo transversal equivalente, para no duplicar enforcement en cada subsistema.
- Define un módulo de `resource accounting` reutilizable por red, storage y coordinadores.
- Aísla la lógica de nombrespacios/tenants de la lógica de protocolo; los clientes clásicos deben seguir funcionando.

### Crates y utilidades sugeridas
- `cargo-workspaces` o un `Cargo.toml` raíz bien segmentado.
- `cargo-deny`, `cargo-audit` y `cargo-nextest` para gobernanza del workspace.
- `prost-build` o generador propio solo si se usa un pipeline de código generado; para el protocolo Kafka probablemente convenga generador específico.

## Entregables mínimos
- Mapa módulo->crate con dependencias internas.
- Diagrama de capas y ownership.
- Lista priorizada de archivos críticos del Kafka original.
- Riesgos de acoplamiento y plan para evitarlos.

## Posibles dificultades
- Copiar ciegamente la estructura del repositorio actual y heredar acoplamientos innecesarios.
- Unificar demasiado pronto crates con ritmos de cambio distintos.
- No reservar un módulo transversal para cuotas, metering y tenancy, obligando a retrofits posteriores.

## Referencias
- [settings.gradle](https://github.com/apache/kafka/blob/trunk/settings.gradle)
- [build.gradle](https://github.com/apache/kafka/blob/trunk/build.gradle)
- [README de message definitions](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/README.md)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [QuorumController.java](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java)
- [RaftClient.java](https://github.com/apache/kafka/blob/trunk/raft/src/main/java/org/apache/kafka/raft/RaftClient.java)
