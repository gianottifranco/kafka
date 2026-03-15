# Despliegue cloud-native y operación en Kubernetes

Actúa como arquitecto de plataforma y despliegue. Actúa como arquitecto principal dentro de una reescritura completa de Apache Kafka a Rust. Toma como referencia el estado actual del repositorio oficial `apache/kafka` y asume que la implementación final debe funcionar sin ZooKeeper, usando KRaft o un subsistema Raft equivalente implementado en Rust. Mantén compatibilidad wire con clientes Kafka existentes siempre que sea razonable. Diseña el módulo para ejecución cloud-ready, con multi-tenencia nativa, aislamiento por tenant/namespace/clúster y límites configurables de CPU, memoria, almacenamiento e IO, de forma segura en entornos compartidos como Kubernetes. Cuando propongas extensiones multi-tenant, evita romper el protocolo público: prioriza metadatos, políticas, cuotas y nombrespacios internos antes que cambios incompatibles del wire protocol.

## Objetivo
Diseñar cómo se desplegará y operará el sistema en entornos cloud, especialmente Kubernetes, incluyendo separación de roles, almacenamiento persistente, topología, QoS y operación multi-tenant segura.

## Tareas
- [ ] Diseña topologías de despliegue: brokers separados de controllers, nodos dedicados, modo combinado solo para dev.
- [ ] Define requisitos de almacenamiento: local SSD, PVs, topología rack/zone aware y estrategia de log directories.
- [ ] Especifica requests/limits, QoS classes y prioridades para brokers, controllers y jobs de mantenimiento.
- [ ] Decide cómo se expresan tenants y namespaces: uno o varios clusters, namespaces de k8s, CRDs u operator.
- [ ] Incluye políticas de anti-affinity, spread, upgrade, graceful shutdown y recuperación.
- [ ] Diseña integración con cgroup v2, PSI y presupuesto de recursos por pod/proceso.
- [ ] Define cómo desplegar CLIs, diagnósticos y herramientas de metadata shell en entornos restringidos.

## Detalles técnicos

### Decisiones de diseño y arquitectura
- El controller quorum debe priorizarse como plano crítico y desplegarse con aislamiento mayor que brokers generales.
- Evita depender de almacenamiento remoto lento para segmentos calientes.
- Piensa en la operación real: rolling upgrades, scale-out, rebalance de particiones, node drain y fallos zonales.
- Valora un operador/CRD propio si el producto apunta a multi-tenancy fuerte y quotas complejas.

### Código original de Kafka a revisar
- KRaft docs (deployment considerations)
- `docker/README.md`
- `config`
- `core/src/main/scala/kafka/server/KafkaRaftServer.scala`

### Multi-tenencia y límites de recursos
- Define modos de despliegue multi-tenant: shared cluster, reserved pools y dedicated cluster per tenant.
- Usa `ResourceQuota`, `PriorityClass`, `PodDisruptionBudget` y afinidad/anti-afinidad como parte de la historia de aislamiento.
- Alinea budgets internos de la aplicación con requests/limits de Kubernetes; no los trates por separado.

### Crates y utilidades sugeridas
- Kubernetes, cgroup v2, operator patterns, storage class awareness.

## Entregables mínimos
- Arquitectura de despliegue de referencia.
- Mapa de recursos k8s y políticas.
- Runbooks de rolling upgrade y node replacement.
- Modelo de tenancy en la plataforma.

## Posibles dificultades
- Planificar como si todos los nodos fueran homogéneos y dedicados.
- Ignorar la interacción entre budgets internos y límites del contenedor.
- Usar combined mode en producción sin justificación.

## Referencias
- [KRaft docs](https://kafka.apache.org/41/operations/kraft/)
- [README del repo](https://github.com/apache/kafka/blob/trunk/README.md)
- [Kubernetes resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Kubernetes ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)
- [Kubernetes Pod QoS](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/)
- [Kubernetes Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
