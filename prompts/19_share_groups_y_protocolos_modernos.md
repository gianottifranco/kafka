# Share groups y protocolos modernos del broker

Actúa como arquitecto responsable de compatibilidad con features modernas de Kafka. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar soporte para `share groups`, estado compartido y APIs modernas del broker, o definir un plan de entrega por fases si no entran en el primer corte funcional.

## Tareas
- [ ] Evalúa el alcance exacto de `share groups` y otras APIs modernas en la versión objetivo.
- [ ] Decide si se implementarán en la primera fase o detrás de feature flags/fases posteriores.
- [ ] Si se implementan, reescribe el `ShareCoordinatorService`, persistencia de estado y APIs asociadas.
- [ ] Documenta su interacción con group coordinator clásico, offsets y metadata.
- [ ] Define límites por tenant para share state, número de grupos y tráfico asociado.
- [ ] Asegura que el pipeline administrativo y de observabilidad también soporta estas entidades.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Kafka 4.x introduce rutas y coordinadores nuevos; si el objetivo es una reescritura completa, no deben ignorarse sin una decisión explícita.
- Puedes optar por un delivery escalonado: primero parity con broker clásico mínimo, luego share groups y protocolos avanzados.
- Las decisiones de fase deben quedar documentadas con impacto en compatibilidad y roadmap.

### Código original de Kafka a revisar
- `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorService.java`
- `core/src/main/java/kafka/server/share/DelayedShareFetch.java`
- `core/src/main/scala/kafka/server/KafkaApis.scala`
- `group-coordinator`

### Multi-tenencia y límites de recursos
- El estado compartido debe presupuestarse por tenant y no convertirse en una nueva fuente de contención global.
- Define métricas y límites específicos para APIs de share state y share fetch.

### Crates y utilidades sugeridas
- `tokio`, timers, runtime shard-aware y estructuras concurrentes contenidas.

## Entregables mínimos
- Decisión explícita de alcance y fases.
- Diseño del share coordinator si aplica.
- Impacto en CLI, observabilidad y quotas.

## Posibles dificultades
- Dejar features modernas fuera del diseño base y descubrir tarde que afectan al protocolo o a coordinadores.
- Subestimar el impacto de share state en almacenamiento y memoria.

## Referencias
- [ShareCoordinatorService.java](https://github.com/apache/kafka/blob/trunk/share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorService.java)
- [KafkaApis.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala)
