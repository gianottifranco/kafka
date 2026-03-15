# Paquete de prompts para reescribir Apache Kafka en Rust

Este paquete contiene prompts independientes, escritos en español, para guiar a un equipo de arquitectura e ingeniería en la reescritura completa de Apache Kafka en Rust.

## Nota de base
A fecha de este paquete, el repositorio oficial `apache/kafka` usa la rama `trunk` como cabeza de desarrollo. Por eso, las rutas y referencias del código original apuntan a `trunk`, aunque el objetivo funcional sea “la versión más reciente del repositorio oficial”.

## Principios no negociables
- Eliminar por completo ZooKeeper y cualquier dependencia equivalente externa para consenso o metadatos.
- Implementar KRaft o un módulo Raft equivalente en Rust dentro del propio sistema.
- Mantener compatibilidad wire con el protocolo binario de Kafka y con clientes Kafka existentes.
- Diseñar el broker para entornos cloud-native y multi-tenant.
- Incorporar límites y garantías de CPU, memoria, almacenamiento e IO por tenant, namespace y/o clúster.
- Evitar “noisy neighbors” mediante cuotas jerárquicas, backpressure, priorización, admission control y aislamiento operativo.
- Separar explícitamente el plano de datos del plano de metadatos/control.
- Pensar desde el inicio en operación en Kubernetes y en despliegues con roles separados de broker y controller.

## Orden sugerido
1. 01_vision_y_requisitos_no_negociables.md
2. 02_inventario_y_mapeo_del_codigo_kafka_actual.md
3. 03_workspace_rust_y_normas_de_implementacion.md
4. 04_protocolo_kafka_y_generacion_de_mensajes.md
5. 05_runtime_async_red_y_gestion_de_conexiones.md
6. 06_despacho_de_requests_y_pipeline_del_broker.md
7. 07_almacenamiento_segmentado_indices_y_recuperacion.md
8. 08_limpieza_de_logs_retencion_y_compaction.md
9. 09_replica_manager_isr_liderazgo_y_fetch.md
10. 10_modelo_de_metadatos_y_publicacion.md
11. 11_raft_kraft_nativo_en_rust.md
12. 12_controller_y_gestion_del_quorum.md
13. 13_migracion_de_zookeeper_a_metadatos_raft.md
14. 14_modelo_de_multitenencia_y_namespaces.md
15. 15_cuotas_aislamiento_de_recursos_y_qos.md
16. 16_seguridad_autenticacion_autorizacion_y_politicas.md
17. 17_coordinador_de_grupos_y_rebalance.md
18. 18_transacciones_idempotencia_y_producer_ids.md
19. 19_share_groups_y_protocolos_modernos.md
20. 20_cliente_productor_rust.md
21. 21_cliente_consumidor_rust.md
22. 22_herramientas_admin_cli_y_metadata_shell.md
23. 23_observabilidad_diagnostico_y_operacion.md
24. 24_despliegue_cloud_native_y_kubernetes.md
25. 25_pruebas_compatibilidad_y_migraciones.md
26. 26_benchmarks_hardening_y_plan_de_entrega.md

## Convención recomendada para ejecutar cada prompt
- Producir una propuesta de arquitectura del módulo.
- Incluir modelo de datos, APIs internas, estados, invariantes y estrategia de persistencia.
- Identificar compatibilidad con Kafka clásico.
- Explicitar impactos multi-tenant y de límites de recursos.
- Proponer pruebas, métricas, rollback y criterios de aceptación.
- Acompañar con ADRs, diagramas de secuencia y pseudocódigo cuando corresponda.

## Sugerencia de modelo conceptual transversal
Usad estos conceptos como hilo conductor:
- `TenantId`: dominio de facturación/aislamiento.
- `NamespaceId`: agrupación lógica de topics, cuotas, ACLs y políticas.
- `ResourceClass`: perfil de límites y garantías.
- `IsolationMode`: `shared`, `reserved`, `dedicated`.
- `QuotaPlan`: árbol jerárquico de cuotas.
- `MetadataDomain`: partición lógica del espacio de metadatos.
- `WorkClass`: clase de servicio para colas, CPU e IO.

## Fuentes base
- Repositorio oficial: https://github.com/apache/kafka
- KRaft: https://kafka.apache.org/41/operations/kraft/
- Upgrade 4.x: https://kafka.apache.org/42/getting-started/upgrade/
- Protocolo Kafka: https://kafka.apache.org/protocol/
- Message definitions: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/README.md
