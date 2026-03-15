# Protocolo binario de Kafka y generación de mensajes

Actúa como arquitecto del wire protocol y del generador de código. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Implementar el protocolo binario de Kafka en Rust conservando compatibilidad con versiones existentes, incluyendo encabezados, cuerpos, versiones flexibles, tagged fields y serialización de record batches.

## Tareas
- [ ] Estudia la especificación JSON de mensajes y decide si crearás un generador de código Rust específico o una capa manual para cada API.
- [ ] Implementa `ApiKeys`, headers, request/response schemas, versionado y negociación de versiones.
- [ ] Soporta versiones flexibles, tagged fields y defaults de campos sin romper compatibilidad.
- [ ] Define un pipeline de generación reproducible desde los JSON del repositorio original o desde una copia versionada interna.
- [ ] Asegura soporte para `ApiVersions`, `Metadata`, `Produce`, `Fetch`, `FindCoordinator`, `DescribeCluster`, `DescribeQuorum` y el resto del catálogo que el broker soporte.
- [ ] Define pruebas de round-trip, golden tests y compatibilidad cruzada con brokers/clientes Kafka existentes.
- [ ] Especifica cómo transportar contexto multi-tenant sin romper el wire protocol: resolución por ACL/principal, listener, SNI, namespace lógico o metadata interna, no por campos incompatibles del protocolo.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El README de `clients/src/main/resources/common/message` explica la semántica de `validVersions`, `taggedVersions`, `flexible versions`, defaults e incompatibilidades permitidas.
- Debes decidir si el generador produce tipos inmutables, builders o structs mutables con validadores.
- Implementa un módulo separado para `record batch` y codificación/decodificación de records, compresión, CRC y timestamps.
- Mantén desacoplada la capa de parsing del pipeline de negocio; la capa de red no debe conocer detalles de storage o metadata.
- Diseña compatibilidad con nuevas APIs de KRaft y con el conjunto moderno de `ApiKeys`.

### Código original de Kafka a revisar
- `clients/src/main/resources/common/message/README.md`
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`
- `core/src/main/scala/kafka/server/KafkaApis.scala`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`

### Multi-tenencia y límites de recursos
- No metas tenancy en el protocolo binario salvo que haya una razón irrenunciable. Prefiere resolver el tenant a partir del principal autenticado, el listener, un namespace del topic o metadatos del plano de control.
- Si introduces extensiones optativas para entornos controlados, deben negociar versión/capacidad y degradar limpiamente con clientes clásicos.
- Incluye métricas de bytes, errores de parseo y latencia por tenant/namespace una vez resuelto el contexto.

### Crates y utilidades sugeridas
- `bytes` para buffers y slices.
- `crc32c`, `lz4_flex`, `snap`, `zstd`, `flate2` según los códecs soportados.
- Un generador propio en Rust para las definiciones JSON, en vez de depender de un generador genérico no alineado con el protocolo de Kafka.

## Entregables mínimos
- Diseño del generador de mensajes o justificación sólida de la alternativa manual.
- Catálogo de APIs soportadas con matriz de versiones.
- Pruebas de compatibilidad binaria y golden files.
- Plan de evolución para nuevas APIs sin romper compatibilidad.

## Posibles dificultades
- Perder compatibilidad por cambiar orden de campos o defaults.
- Sobrecargar la red con asignaciones y copias innecesarias.
- Diseñar un generador que no pueda seguir el ritmo de nuevas versiones del protocolo.

## Referencias
- [README de message definitions](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/README.md)
- [ApiKeys.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java)
- [Protocol guide](https://kafka.apache.org/protocol/)
- [KafkaApis.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala)
- [NetworkClient.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java)
- [bytes](https://docs.rs/bytes/latest/bytes/)
