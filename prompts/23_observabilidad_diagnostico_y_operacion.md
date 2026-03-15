# Observabilidad, diagnóstico y operación diaria

Actúa como arquitecto de observabilidad y soporte de producción. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar la telemetría, logging, métricas, trazas y herramientas de diagnóstico necesarias para operar el sistema en producción, incluyendo visibilidad por tenant y por subsistema.

## Tareas
- [ ] Define métricas mínimas por red, storage, quorum, controller, coordinadores, cuotas y clientes.
- [ ] Incluye métricas de uso y presupuesto por tenant/namespace.
- [ ] Diseña spans/trazas para requests críticas (`Produce`, `Fetch`, heartbeats, mutations de metadata, quorum RPCs).
- [ ] Diseña logs estructurados con claves estables y sin filtración de información sensible.
- [ ] Expón endpoints o exporters para Prometheus/OpenTelemetry.
- [ ] Crea un playbook de diagnóstico: under-replication, lag de quorum, disco lleno, throttling excesivo, tenants ruidosos, leaks de memoria.
- [ ] Incluye herramientas para inspección de segmentos, snapshots y metadata images.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- Las métricas deben permitir distinguir claramente entre foreground, background y control plane.
- Evita cardinalidad explosiva; por tenant/namespace usa agregados y sampling cuando sea necesario.
- Integra eventos de quotas y admission control en la observabilidad para entender rechazo y degradación.
- Piensa en SLOs y alertas desde el principio.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `tools`
- `shell`

### Multi-tenencia y límites de recursos
- La observabilidad multi-tenant debe permitir a operadores globales ver todo y a operadores de tenant solo su ámbito.
- Registra consumos de CPU, memoria, disco e IO por tenant con periodicidad razonable.

### Crates y utilidades sugeridas
- `tracing`, `tracing-subscriber`, `opentelemetry`, `prometheus` o exporter compatible, `metrics` si se adopta como capa interna.

## Entregables mínimos
- Catálogo de métricas y labels permitidos.
- Esquema de logs y spans.
- Mapa de alertas operativas.
- Playbook inicial de diagnóstico.

## Posibles dificultades
- Cardinalidad incontrolada por topic/partition/client-id/tenant.
- Logs ruidosos sin estructura útil.
- No distinguir entre saturación local y throttling intencional.

## Referencias
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [ControllerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ControllerServer.scala)
- [opentelemetry](https://docs.rs/opentelemetry/latest/opentelemetry/)
- [prometheus](https://docs.rs/prometheus/latest/prometheus/)
- [tracing](https://docs.rs/tracing/latest/tracing/)
