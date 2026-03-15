# Workspace de Rust, estándares de implementación y reglas de concurrencia

Actúa como staff engineer responsable del foundation layer en Rust. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Definir el esqueleto del workspace de Rust, las reglas de codificación, el contrato de errores, la estrategia de concurrencia y las políticas de seguridad para todo el programa.

## Tareas
- [ ] Diseña la estructura del workspace y el conjunto inicial de crates.
- [ ] Decide cómo se expondrán traits, tipos compartidos y contratos entre módulos.
- [ ] Define reglas de oro para usar `async`, `spawn_blocking`, locks, channels y pools de memoria.
- [ ] Especifica el manejo estándar de errores: errores de dominio, errores recuperables, errores fatales y errores de protocolo.
- [ ] Establece una política explícita para `unsafe`: cuándo se permite, cómo se revisa y qué pruebas requiere.
- [ ] Decide la estrategia de configuración (`serde`, `figment`, `config`, YAML/TOML/ENV`) y la forma de versionarla.
- [ ] Define las guías de testing para crates base: unit, property-based, loom/miri donde aplique, fuzzing y benchmarks.
- [ ] Integra desde el diseño hooks para accounting de recursos por tenant.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Tokio debe ser el runtime principal salvo justificación excepcional y documentada.
- Las tareas de IO de archivo, compresión o checksum intensivo deben pasar por `spawn_blocking` o pools dedicados con semáforos; no bloquear el runtime.
- La metadata publicada a lectores de alto volumen debería exponerse como snapshots inmutables (`Arc`, `ArcSwap`, RCU-like patterns).
- Las fronteras entre crates deben minimizar el uso de genéricos complejos a través de la API pública si afectan tiempos de compilación o legibilidad.
- Define un estilo homogéneo para `Result<T, E>`, tipos de error y telemetría estructurada.

### Código original de Kafka a revisar
- `build.gradle`
- `README.md`
- `server-common`
- `server`
- `clients`

### Multi-tenencia y límites de recursos
- Incluye un crate o módulo transversal para `tenant_context`, `quota_key`, `resource_budget` y `work_class`.
- Toda API interna que pueda consumir CPU/IO relevante debe aceptar contexto de tenant o contexto resoluble a tenant.
- Evita estados globales implícitos; favorece objetos de contexto explícitos para poder aplicar límites y trazas por tenant.

### Crates y utilidades sugeridas
- `tokio`, `tokio-util`, `bytes`, `parking_lot`, `arc-swap`, `dashmap`, `thiserror`, `serde`, `tracing`, `clap`.
- `loom` para pruebas de concurrencia donde haya estructuras lock-free o patrones delicados.
- `miri` y sanitizers para detectar UB si se introduce `unsafe`.

## Entregables mínimos
- Plantilla del workspace con crates y convenciones.
- Guía de estilos de concurrencia y `unsafe`.
- Política de errores y logging.
- Contrato de contexto multi-tenant compartido.

## Posibles dificultades
- Abusar de `Arc<Mutex<...>>` y trasladar contention a producción.
- Permitir `spawn_blocking` sin límites y degradar el runtime bajo carga.
- Acoplar demasiado la configuración a formatos externos y dificultar migraciones de config.

## Referencias
- [README del repo](https://github.com/apache/kafka/blob/trunk/README.md)
- [tokio](https://docs.rs/tokio/latest/tokio/)
- [bytes](https://docs.rs/bytes/latest/bytes/)
- [tracing](https://docs.rs/tracing/latest/tracing/)
- [arc-swap](https://docs.rs/arc-swap/latest/arc_swap/)
- [parking_lot](https://docs.rs/parking_lot/latest/parking_lot/)
- [dashmap](https://docs.rs/dashmap/latest/dashmap/)
