# Herramientas de administración CLI, shell y diagnósticos

Actúa como arquitecto de tooling y operabilidad. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Reescribir las herramientas administrativas más importantes del ecosistema Kafka para que operen sobre el broker/controller en Rust, incluyendo topics, configs, metadata quorum, storage, dump-log y metadata shell.

## Tareas
- [ ] Inventaria las herramientas que deben existir desde la primera entrega.
- [ ] Diseña equivalentes de `kafka-topics.sh`, `kafka-configs.sh`, `kafka-metadata-quorum.sh`, `kafka-storage.sh`, `kafka-dump-log.sh` y `kafka-metadata-shell.sh`.
- [ ] Decide si el CLI será una sola herramienta con subcomandos o varias herramientas pequeñas.
- [ ] Soporta operaciones tenant-aware: listar, describir y mutar solo el ámbito autorizado.
- [ ] Diseña salida humana y salida máquina (`json`, `yaml`) para automatización.
- [ ] Incluye tooling de inspección de snapshots, metadata log, estados del quorum y budgets por tenant.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El tooling debe ser operacionalmente seguro: dry-run, confirmaciones, timeouts razonables y mensajes claros.
- Las herramientas de quorum y metadata shell son críticas para diagnóstico y soporte; no las dejes para el final.
- El modelo de permisos del CLI debe respetar la misma authz que el plano de control.

### Código original de Kafka a revisar
- `tools/src/main/java/org/apache/kafka/tools/TopicCommand.java`
- `tools/src/main/java/org/apache/kafka/tools/MetadataQuorumCommand.java`
- `shell/src/main/java/org/apache/kafka/shell/MetadataShell.java`
- Documentación `kafka-metadata-quorum.sh` y `metadata shell`

### Multi-tenencia y límites de recursos
- Toda salida que enumere recursos debe permitir filtro por tenant/namespace y ocultar recursos ajenos por defecto.
- Incluye subcomandos para cuotas, budgets y uso por tenant.

### Crates y utilidades sugeridas
- `clap`, `serde_json`, `comfy-table` o equivalente si aporta claridad, `tracing` para diagnósticos.

## Entregables mínimos
- Mapa de comandos y subcomandos.
- Diseño de salida humana y máquina.
- Matriz de permisos administrativos.
- Plan de tooling mínimo viable y extensiones.

## Posibles dificultades
- Diseñar CLIs distintos sin semántica consistente.
- No soportar salidas automatizables.
- Exponer más visibilidad administrativa de la permitida entre tenants.

## Referencias
- [TopicCommand.java](https://github.com/apache/kafka/blob/trunk/tools/src/main/java/org/apache/kafka/tools/TopicCommand.java)
- [MetadataQuorumCommand.java](https://github.com/apache/kafka/blob/trunk/tools/src/main/java/org/apache/kafka/tools/MetadataQuorumCommand.java)
- [MetadataShell.java](https://github.com/apache/kafka/blob/trunk/shell/src/main/java/org/apache/kafka/shell/MetadataShell.java)
- [Metadata quorum tool docs](https://kafka.apache.org/40/operations/kraft/)
- [clap](https://docs.rs/clap/latest/clap/)
