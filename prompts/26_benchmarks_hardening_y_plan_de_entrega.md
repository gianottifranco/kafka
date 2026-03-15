# Benchmarks, hardening, seguridad operativa y plan de entrega

Actúa como arquitecto de rendimiento y release management. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Cerrar el programa con un plan de benchmarking, hardening y entrega por fases que permita llevar el sistema desde prototipo hasta producción sin perder control técnico ni operativo.

## Tareas
- [ ] Define un baseline de rendimiento frente a Kafka actual para produce, fetch, latencia de metadata, failover y recuperación.
- [ ] Crea benchmarks de micro y macro rendimiento por subsistema.
- [ ] Establece presupuestos de regresión máximos y alarmas de performance.
- [ ] Diseña un programa de hardening: seguridad, saturación, disco lleno, filesystems, límites del SO, upgrade/downgrade, disaster recovery.
- [ ] Propón un roadmap por fases: foundation, broker mínimo, quorum estable, coordinadores, tooling, multi-tenancy avanzada, GA.
- [ ] Añade criterios de salida de cada fase y riesgos abiertos.
- [ ] Documenta qué features pueden ir detrás de feature flags y qué requiere paridad antes de producción.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El objetivo no es solo igualar throughput bruto; también debes medir aislamiento multi-tenant y eficiencia bajo contención.
- Incluye hardening del kernel/OS, sockets, filesystems, límites de FD, ulimits y configuración recomendada para contenedores.
- El roadmap debe reflejar dependencias reales entre módulos, no una simple lista lineal.
- Relaciona cada fase con tooling, pruebas, migración y observabilidad requeridos.

### Código original de Kafka a revisar
- `README.md`
- `jmh-benchmarks`
- `tests`
- `trogdor`
- `core`
- `storage`
- `metadata`

### Multi-tenencia y límites de recursos
- Mide explícitamente fairness y cumplimiento de QoS por tenant en benchmarks y gates.
- Incluye escenarios de tenants bursty, tenants premium y mezcla de cargas administrativas + data plane.

### Crates y utilidades sugeridas
- `criterion` para microbenchmarks, harnesses de cluster benchmarks, profiling de CPU/heap/page cache, eBPF si procede.

## Entregables mínimos
- Plan de benchmarks con KPIs y metodología.
- Checklist de hardening operativo.
- Roadmap por fases con exit criteria.
- Registro de riesgos residuales.

## Posibles dificultades
- Comparar solo throughput medio e ignorar p99, recovery o fairness multi-tenant.
- No fijar budgets de regresión medibles.
- Prometer GA sin runbooks, tooling y chaos coverage suficientes.

## Referencias
- [README del repo](https://github.com/apache/kafka/blob/trunk/README.md)
- [jmh-benchmarks/README.md](https://github.com/apache/kafka/blob/trunk/jmh-benchmarks/README.md)
- [tests/README.md](https://github.com/apache/kafka/blob/trunk/tests/README.md)
- [criterion](https://docs.rs/criterion/latest/criterion/)
