# Migración desde ZooKeeper al nuevo sistema de metadatos Raft en Rust

Actúa como arquitecto de migración y compatibilidad operativa. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar una estrategia de migración desde clústeres históricos basados en ZooKeeper hacia el nuevo subsistema de metadatos Raft en Rust, inspirada en KRaft migration pero adaptada al nuevo sistema y con soporte para tenants.

## Tareas
- [ ] Define si la migración será online, offline o ambas.
- [ ] Modela las fases de migración: lectura inicial, fase híbrida, dual-write opcional, validación y finalización.
- [ ] Especifica cómo importar topics, particiones, configs, ACLs, quotas, SCRAM y estados relevantes desde el mundo ZooKeeper/Kafka antiguo.
- [ ] Define checksums, comparaciones y validaciones previas al cutover.
- [ ] Añade estrategia de rollback o, si no es segura, documenta claramente por qué y cómo minimizar el riesgo.
- [ ] Especifica cómo etiquetar/importar tenants y namespaces si el clúster legado no tenía multi-tenancy explícita.
- [ ] Define tooling para preflight checks, dry run y auditoría.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Aunque Kafka 4.x ya eliminó ZooKeeper, el nuevo sistema puede necesitar absorber clusters anteriores vía bridge release o tooling especializado.
- KRaft migration documenta fases útiles: initial load, hybrid, dual-write, finalize. Usa la idea, no necesariamente la misma implementación.
- No intentes soportar rollback arbitrario si no puedes probarlo; es mejor un camino de migración unidireccional bien validado.
- La importación de metadata tenant-aware puede requerir reglas heurísticas (prefijos, ACLs, cluster mapping, listas externas).

### Código original de Kafka a revisar
- Documentación KRaft migration
- `metadata`
- `core/src/main/scala/kafka/server/KafkaRaftServer.scala`
- `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`

### Multi-tenencia y límites de recursos
- Incluye una fase de normalización donde topics legacy se asignen a tenants/namespaces según reglas auditables.
- Asegura que cuotas y ACLs migradas no otorguen visibilidad cruzada accidental entre tenants.
- Evita migraciones globales monolíticas; soporta oleadas por tenant/namespace cuando sea posible.

### Crates y utilidades sugeridas
- Herramientas CLI dedicadas, formatos de export/import versionados, validadores de consistencia.

## Entregables mínimos
- Plan de migración paso a paso.
- Matriz de objetos de metadata a migrar.
- Plan de validación y auditoría.
- Reglas de asignación tenant-aware para legados.

## Posibles dificultades
- Subestimar la complejidad de ACLs, quotas y SCRAM heredados.
- Migrar sin dry run ni comparación exhaustiva de metadata.
- No planificar cómo se asignarán tenants a recursos legacy.

## Referencias
- [KRaft docs](https://kafka.apache.org/39/operations/kraft/)
- [Upgrade 4.x](https://kafka.apache.org/42/getting-started/upgrade/)
- [KRaft migration note](https://kafka.apache.org/40/operations/kraft/)
- [Broker configs migration flags](https://kafka.apache.org/39/configuration/broker-configs/)
