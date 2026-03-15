# Cliente productor en Rust

Actúa como arquitecto del producer client opcional. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar un cliente productor en Rust compatible con brokers Kafka, incluyendo metadata refresh, batching, compresión, idempotencia, transacciones y telemetría adecuada para entornos multi-tenant.

## Tareas
- [ ] Implementa el equivalente de `KafkaProducer`, `Sender`, `RecordAccumulator` y metadata refresh.
- [ ] Diseña batching, `linger`, `batch.size`, compresión y límites de memoria del cliente.
- [ ] Soporta idempotencia y transacciones si el objetivo del programa incluye cliente completo.
- [ ] Define contratos de serializers, interceptors y hooks de métricas/telemetría.
- [ ] Añade awareness de tenant opcional para entornos donde el producer se use como SDK multi-tenant.
- [ ] Especifica compatibilidad con brokers Kafka existentes y con el nuevo broker en Rust.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El producer es thread-safe en Java; decide la ergonomía equivalente en Rust (`Clone`, handle compartido, background tasks, etc.).
- Separa la API pública ergonómica del runtime interno y del `NetworkClient`.
- Controla memoria total del cliente y budgets por partición/tenant.
- Mantén clara la semántica de expiración, retries y delivery timeout.

### Código original de Kafka a revisar
- `clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
- `clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java`
- `clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`

### Multi-tenencia y límites de recursos
- Si el SDK se usa en aplicaciones multi-tenant, permite budgets separados por tenant o pool de productor lógico.
- Evita que un tenant local agote memoria del proceso cliente cuando se comparte una instancia SDK.

### Crates y utilidades sugeridas
- `tokio`, `bytes`, `parking_lot`/locks ligeros, `flume`/channels si son apropiados, códecs de compresión.

## Entregables mínimos
- Diseño de la API pública del producer.
- Runtime interno y estrategia de batching.
- Matriz de compatibilidad y soporte de idempotencia/transacciones.
- Pruebas de throughput y reintentos.

## Posibles dificultades
- Trasladar demasiado detalle interno a la API pública.
- No acotar memoria y colas internas del cliente.
- Subestimar la complejidad de delivery semantics con retries.

## Referencias
- [KafkaProducer.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java)
- [RecordAccumulator.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java)
- [NetworkClient.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java)
