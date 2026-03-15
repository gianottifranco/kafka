# Seguridad, autenticación, autorización y políticas por tenant

Actúa como arquitecto de seguridad del broker y del plano de control. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar la capa de seguridad del sistema en Rust: TLS, SASL, SCRAM, ACLs, autorización tenant-aware, tokens delegados y auditoría.

## Tareas
- [ ] Define listeners seguros, TLS/mTLS y rotación de certificados.
- [ ] Diseña soporte SASL al menos para PLAIN/SCRAM; documenta fases para mecanismos más complejos.
- [ ] Modela la resolución `principal -> tenant -> namespace permissions`.
- [ ] Implementa ACLs y políticas con alcance a tenant, namespace, topic, group, cluster y operaciones administrativas.
- [ ] Diseña almacenamiento y publicación de SCRAM, tokens delegados y credenciales efímeras en metadata.
- [ ] Añade auditoría estructurada de accesos y denegaciones.
- [ ] Documenta cómo convivirán autenticación fuerte y compatibilidad con clientes existentes.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- La autorización debe ser barata en el path crítico; usa snapshots de metadata/autz y caches invalidables.
- Piensa en separación entre autenticación (quién es) y pertenencia a tenant (a qué dominio administrativo pertenece).
- No metas información de tenant en secretos si puedes derivarla de políticas o mappings mantenidos en metadata.
- Protege especialmente herramientas administrativas y operaciones de metadatos.

### Código original de Kafka a revisar
- `core/src/main/scala/kafka/server/ControllerServer.scala`
- `core/src/main/scala/kafka/server/BrokerServer.scala`
- `metadata` publishers de ACL/SCRAM/delegation token
- `clients/src/main/java/org/apache/kafka/common/network`

### Multi-tenencia y límites de recursos
- La seguridad debe impedir visibilidad cruzada de tenants por defecto.
- Asegura que métricas, logs y diagnósticos no filtren nombres o recursos de otros tenants.
- Permite políticas por namespace además de por topic para reducir complejidad administrativa.

### Crates y utilidades sugeridas
- `rustls`, `tokio-rustls`, crates SCRAM si son suficientemente maduros, almacenamiento seguro de secretos y rotación.

## Entregables mínimos
- Modelo de seguridad extremo a extremo.
- Matriz de permisos por recurso y por tenant.
- Plan de rotación de secretos/certificados.
- Especificación de auditoría.

## Posibles dificultades
- Hacer authz por consultas síncronas o bloqueantes a metadata mutable.
- Confundir `client.id` con identidad de seguridad real.
- No aislar telemetría y logs por tenant.

## Referencias
- [ControllerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ControllerServer.scala)
- [BrokerServer.scala](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/BrokerServer.scala)
- [rustls](https://docs.rs/rustls/latest/rustls/)
- [tokio-rustls](https://docs.rs/tokio-rustls/latest/tokio_rustls/)
