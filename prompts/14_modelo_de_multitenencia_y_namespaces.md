# Modelo de multi-tenencia, namespaces y políticas de aislamiento

Actúa como arquitecto de plataforma multi-tenant. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Definir la semántica de multi-tenencia del sistema: qué es un tenant, qué es un namespace, qué recursos están aislados, cómo se heredan políticas y cómo se exponen estas abstracciones sin romper la experiencia Kafka existente.

## Tareas
- [ ] Define las entidades `TenantId`, `NamespaceId`, `ResourceClass`, `IsolationMode` y `QuotaPlan`.
- [ ] Decide si los topics pertenecen siempre a un namespace y si este pertenece siempre a un tenant.
- [ ] Especifica el esquema de nombres y la resolución entre nombres lógicos y nombres físicos si quieres mantener compatibilidad con clientes clásicos.
- [ ] Diseña herencia de políticas: defaults globales -> tenant -> namespace -> topic/grupo/cliente.
- [ ] Define aislamiento de metadatos, aislamiento administrativo, aislamiento de datos y aislamiento de observabilidad.
- [ ] Diseña cómo se crean, actualizan, suspenden y eliminan tenants sin afectar a otros.
- [ ] Añade modelo de facturación/metrado si es relevante para el control de recursos.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Multi-tenancy no es solo prefijar nombres de topics; debe existir un modelo explícito en metadata y en enforcement.
- Evalúa diferentes modos: namespace lógico sobre clúster compartido, clúster virtual por tenant, pools dedicados para tenants críticos.
- Piensa en compatibilidad: un cliente Kafka legacy puede seguir viendo topics convencionales, mientras que el plano de control conoce tenant/namespace.
- Asegura que ACLs y quotas puedan expresarse a nivel de tenant y namespace, además de topic y client-id.

### Código original de Kafka a revisar
- `metadata`
- `core/src/main/scala/kafka/server/KafkaConfig.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `tools/src/main/java/org/apache/kafka/tools/TopicCommand.java`

### Multi-tenencia y límites de recursos
- Define qué significa aislamiento duro y blando para red, CPU, memoria, almacenamiento, metadata y observabilidad.
- Permite que un tenant tenga múltiples namespaces con políticas distintas (por ejemplo, `hot`, `cold`, `compact`, `low-latency`).
- Resuelve el problema de `noisy neighbors` en la semántica del modelo, no solo en la implementación.

### Crates y utilidades sugeridas
- El modelo de tenancy se apoyará en el subsistema de metadata y en el engine de cuotas.

## Entregables mínimos
- Modelo conceptual y esquema de entidades.
- Política de naming/aliasing con compatibilidad Kafka.
- Reglas de herencia de configuración.
- Ciclo de vida de tenants y namespaces.

## Posibles dificultades
- Introducir tenancy solo como etiqueta cosmética sin enforcement real.
- Romper naming o discovery de clientes clásicos.
- No definir visibilidad administrativa por tenant desde el inicio.

## Referencias
- [TopicCommand.java](https://github.com/apache/kafka/blob/trunk/tools/src/main/java/org/apache/kafka/tools/TopicCommand.java)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [Kubernetes ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)
