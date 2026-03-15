# Runtime async, plano de red y gestión de conexiones

Actúa como arquitecto del plano de red del broker. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar el equivalente en Rust a `SocketServer`, `RequestChannel`, acceptors, processors y quotas de conexión, usando Tokio y mecanismos de backpressure para soportar alta concurrencia sin sacrificar aislamiento entre tenants.

## Tareas
- [ ] Reproduce el modelo lógico de listeners, acceptors, processors y handlers del broker actual, pero adaptado a un runtime async moderno.
- [ ] Define la arquitectura de listeners separados para data plane y controller plane.
- [ ] Decide cómo se mapean sockets, conexiones, parsers y request queues a tareas Tokio.
- [ ] Implementa pools de memoria o límites de buffers para impedir agotamiento por conexiones lentas o tenants abusivos.
- [ ] Añade cuotas de conexiones, tasa de creación de conexiones y límites por listener, IP, principal y tenant.
- [ ] Diseña un sistema de colas por clase de trabajo (`WorkClass`) con fairness y prioridades configurables.
- [ ] Integra TLS, SASL y cierre ordenado de conexiones con métricas y trazas estructuradas.
- [ ] Define un mecanismo de backpressure que reaccione a presión de CPU, memoria o IO sin bloquear el runtime global.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El `SocketServer.scala` actual describe un modelo con acceptor threads, processor threads y handler threads; en Rust conviene reinterpretarlo como tareas async y workers especializados.
- Aísla el parseo del protocolo del despacho de negocio para poder aplicar límites por etapa.
- Usa presupuestos explícitos de memoria para request bodies, respuestas en vuelo y colas internas.
- Piensa en conexiones privilegiadas (inter-broker/controller) frente a conexiones de clientes generales.
- Modela la muting/unmuting de canales y el throttling como estados explícitos de la conexión, no como efectos implícitos.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/network/SocketServer.scala`
- `core/src/main/scala/kafka/network/RequestChannel.scala`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`
- `core/src/main/scala/kafka/server/BrokerServer.scala`

### Multi-tenencia y límites de recursos
- Define fairness multinivel: primero reservar capacidad mínima para control plane y replicación; después aplicar pesos por tenant; finalmente colas por cliente o conexión.
- Mide y limita memoria por tenant para requests encoladas, respuestas pendientes y buffers acumulados.
- Considera integración con cgroup v2 para leer presión y adaptar límites en tiempo real; si no está disponible, simúlalo a nivel de aplicación con budgets.
- Protege al sistema contra el ruido entre tenants con `weighted fair queuing`, `token buckets` y límites de conexiones concurrentes.

### Crates y utilidades sugeridas
- `tokio`, `tokio-util`, `bytes`, `tokio-rustls`, `rustls`, `governor`, `dashmap`.
- `mio` o `socket2` solo si necesitas control fino de sockets más allá de Tokio.
- `tower` puede servir para modelar middleware interno de límites y trazas.

## Entregables mínimos
- Diagrama de runtime y colas del plano de red.
- Especificación de estados de conexión y de throttling.
- Modelo de quotas de conexión y memoria.
- Plan de pruebas de carga, fairness y degradación.

## Posibles dificultades
- Hacer `await` largos dentro de rutas críticas y bloquear fairness.
- Recrear 1:1 la topología de hilos de la JVM sin aprovechar async/await.
- Aplicar cuotas solo al final del pipeline, cuando el daño en memoria ya está hecho.

## Referencias
- [SocketServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/network/SocketServer.scala)
- [NetworkClient.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [tokio](https://docs.rs/tokio/latest/tokio/)
- [tokio-rustls](https://docs.rs/tokio-rustls/latest/tokio_rustls/)
- [governor](https://docs.rs/governor/latest/governor/)
- [cgroup v2 docs](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
