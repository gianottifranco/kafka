# Cliente consumidor en Rust

Actúa como arquitecto del consumer client opcional. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar un consumidor en Rust compatible con Kafka, con soporte de fetch, gestión de offsets, grupos de consumo, heartbeats, rebalance y aislamiento de lectura.

## Tareas
- [ ] Implementa el equivalente de `KafkaConsumer` y coordinadores internos relevantes.
- [ ] Diseña el ciclo `poll`, buffers de fetch, heartbeat background, commits síncronos/asíncronos y wakeup/shutdown.
- [ ] Soporta grupos de consumo clásicos y prepara compatibilidad con protocolos modernos.
- [ ] Implementa `read_committed`, fetch sessions y manejo correcto de errores de metadata/coordinator.
- [ ] Añade una API pública idiomática en Rust que no sacrifique compatibilidad conceptual con Kafka.
- [ ] Documenta uso en aplicaciones multi-tenant o con múltiples grupos dentro del mismo proceso.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El consumer de Kafka no es thread-safe; decide cómo reflejar ese contrato en Rust con tipos y ownership.
- Separa claramente buffer de records, coordinación de grupo y networking.
- No mezcles la API pública con la máquina de estados de rebalances/heartbeats.
- Controla memoria del fetch buffer y el número de particiones activas.

### Código original de Kafka a revisar
- `clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java`
- `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`
- `group-coordinator`

### Multi-tenencia y límites de recursos
- Si el SDK atiende múltiples tenants/grupos en un mismo proceso, permite budgets por grupo o por consumer handle.
- Evita starvation entre grupos compartiendo runtime local o conexiones.

### Crates y utilidades sugeridas
- `tokio`, `bytes`, timers y canales; quizá `futures-stream` o iteradores adaptados para ergonomía.

## Entregables mínimos
- Diseño de la API pública del consumer.
- Máquina de estados de heartbeats/rebalance.
- Plan de commits y recovery del cliente.
- Pruebas de grupo y failover.

## Posibles dificultades
- Ocultar semánticas complejas detrás de una API demasiado simple y difícil de razonar.
- No acotar fetch buffers y provocar picos de memoria.
- Manejar incorrectamente wakeup/cancelación y dejar tareas huérfanas.

## Referencias
- [KafkaConsumer.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java)
- [ConsumerCoordinator.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java)
- [NetworkClient.java](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java)
