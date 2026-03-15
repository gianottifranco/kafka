# Despacho de requests, pipeline del broker y forwarding al controller

Actúa como arquitecto del pipeline de APIs del broker. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar el equivalente en Rust a `KafkaApis`, `ForwardingManager`, validación, autorización, throttling y dispatch hacia storage, coordinadores o controller, sin mezclar responsabilidades ni comprometer throughput.

## Tareas
- [ ] Diseña un dispatcher principal que enrute por `ApiKey` y versión.
- [ ] Separa etapas: autenticación resuelta, autorización, resolución de tenant/namespace, validación de cuota, decode semántico y ejecución.
- [ ] Implementa forwarding transparente al controller para operaciones administrativas y de metadata que no deba resolver el broker local.
- [ ] Modela `RequestLocal` o contexto equivalente con trazas, principal, tenant, presupuesto de recursos y deadlines.
- [ ] Define qué rutas deben ser síncronas, cuáles asíncronas y cuáles se apoyan en delayed operations/purgatories.
- [ ] Añade circuit breakers y mecanismos de rechazo temprano para APIs caras bajo presión.
- [ ] Documenta cómo se propagará el contexto multi-tenant a storage, coordinadores y controller.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- `KafkaApis.scala` es la pieza central: úsala para inventariar el catálogo de APIs y decidir fronteras internas.
- El dispatcher no debe conocer detalles de persistencia binaria de segmentos; debe delegar a servicios internos con contratos claros.
- El forwarding al controller debe preservar contexto de autorización, timeout y correlación.
- Conviene modelar middlewares internos: authn/authz, quotas, tracing, métricas y mapping de errores a `Errors` del protocolo.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/server/KafkaApis.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`

### Multi-tenencia y límites de recursos
- La resolución de tenant debería ocurrir muy pronto en el pipeline para que authz, quotas y métricas la aprovechen.
- Añade prioridades por clase de API: control plane y replication > produce/fetch > admin masivo > background maintenance.
- Diseña throttling diferenciado para produce/fetch/admin por tenant y por namespace, no solo por cliente.

### Crates y utilidades sugeridas
- `tower` para modelar middlewares internos si simplifica el diseño.
- `tracing` para spans por request con `tenant_id`, `api_key`, `correlation_id` y `listener`.
- `governor` o un motor de quotas propio para rate limiting.

## Entregables mínimos
- Mapa `ApiKey` -> servicio interno -> permisos -> cuotas.
- Pipeline de middlewares internos.
- Tabla de errores del dominio a `Errors` del protocolo.
- Diseño de forwarding con preservación de contexto.

## Posibles dificultades
- Acoplar `ApiKey` a implementaciones concretas y perder testabilidad.
- Resolver authz o tenancy demasiado tarde.
- No distinguir entre throttling benigno y fallos que deben cerrar conexión.

## Referencias
- [KafkaApis.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala)
- [ApiKeys.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [tower](https://docs.rs/tower/latest/tower/)
- [tracing](https://docs.rs/tracing/latest/tracing/)
