# Pruebas, compatibilidad, caos y migraciones entre versiones

Actúa como arquitecto de QA, fiabilidad y compatibilidad. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar una estrategia de pruebas que cubra compatibilidad wire, recuperación, failover, rendimiento, fairness multi-tenant, migraciones y escenarios de caos en clúster.

## Tareas
- [ ] Define un test matrix por módulo: unitarias, integración, property-based, fuzzing, soak y chaos tests.
- [ ] Añade pruebas de compatibilidad con clientes Kafka existentes y, si aplica, con brokers Kafka Java/Scala.
- [ ] Diseña pruebas de rolling upgrade, mixed-version y migración desde clústeres legacy.
- [ ] Incluye pruebas de crash/recovery, corrupción de índices, pérdida de quorum, lag de controller, under-replication y truncado.
- [ ] Añade pruebas específicas de multi-tenancy: fairness, noisy-neighbor, enforcement de budgets y visibilidad administrativa.
- [ ] Automatiza benchmarks de regresión y golden tests del protocolo.
- [ ] Define criterios de release gate y de promoción de alpha->beta->GA.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- No trates compatibilidad y fiabilidad como una fase final: debe existir harness desde los primeros módulos.
- Valora entornos similares a Trogdor/ducktape para pruebas de sistema.
- Las pruebas multi-tenant deben medir no solo correctness sino también aislamiento de recursos y latencia bajo contención.
- Incluye fault injection en red, disco y metadata quorum.

### Código original de Kafka a revisar
- `tests/README.md`
- `trogdor/README.md`
- `README.md`
- `clients`
- `core`
- `metadata`

### Multi-tenencia y límites de recursos
- Diseña benchmarks y chaos tests con mezcla de tenants premium y best-effort para validar QoS.
- Incluye validación de límites duros/blandos y degradación controlada.

### Crates y utilidades sugeridas
- `cargo-nextest`, `proptest`, fuzzers, harnesses de integración y cluster tests automatizados.

## Entregables mínimos
- Estrategia de test matrix completa.
- Harness mínimo de cluster tests.
- Plan de chaos/fault injection.
- Criterios de release gate.

## Posibles dificultades
- Probar solo correctness local y no comportamientos distribuidos.
- No incluir pruebas de fairness/noisy-neighbor.
- No probar mixed-version/migraciones hasta demasiado tarde.

## Referencias
- [README del repo](https://github.com/apache/kafka/blob/trunk/README.md)
- [tests/README.md](https://github.com/apache/kafka/blob/trunk/tests/README.md)
- [trogdor/README.md](https://github.com/apache/kafka/blob/trunk/trogdor/README.md)
- [proptest](https://docs.rs/proptest/latest/proptest/)
