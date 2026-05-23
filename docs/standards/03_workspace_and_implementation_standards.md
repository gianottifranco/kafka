# Kafka-Rust: Workspace, Estándares de Implementación y Reglas de Concurrencia

> **Programa:** Reescritura Apache Kafka → Rust  
> **Fecha:** 2026-03-15  
> **Rama de referencia:** `apache/kafka@trunk` (≈ Kafka 4.1+)  
> **Prerequisitos:** [01_system_vision.md](../vision/01_system_vision.md), [02_code_inventory_and_crate_mapping.md](../inventory/02_code_inventory_and_crate_mapping.md)

---

## 1. Estructura del Workspace

### 1.1 Layout de Directorios

```
kafka-rust/
├── Cargo.toml                        # [workspace] resolver = "2"
├── Cargo.lock
├── rust-toolchain.toml               # Canal y componentes pinneados
├── rustfmt.toml                      # Formateo uniforme
├── clippy.toml                       # Lints custom
├── deny.toml                         # cargo-deny: licencias, bans, advisories
├── .cargo/
│   └── config.toml                   # Flags de compilación, perfiles
│
├── crates/
│   │
│   │  ── Layer 0: Fundación ──
│   ├── kafka-server-types/           # Tipos fundamentales: MetadataVersion, TopicIdPartition, features
│   ├── kafka-protocol/               # Wire protocol: 198 API schemas, codecs, headers
│   ├── kafka-protocol-codegen/       # Build-time: genera structs desde JSON schemas
│   │
│   │  ── Layer 1: Infraestructura ──
│   ├── kafka-network/                # Acceptor, TLS, SASL, dispatch, connection management
│   ├── kafka-storage/                # Log segmentado: segments, índices, append, read, retention
│   ├── kafka-tenant/                 # TenantId, NamespaceId, routing, isolation lógico
│   ├── kafka-quota/                  # Rate limiters, resource accounting jerárquico
│   │
│   │  ── Layer 2: Plano de Control ──
│   ├── kafka-raft/                   # Motor Raft nativo: elecciones, replicación, snapshots
│   ├── kafka-metadata/               # Image/Delta inmutables, publicación, snapshots
│   ├── kafka-controller/             # QuorumController, sub-managers, event queue
│   ├── kafka-controller-server/      # Proceso controller: lifecycle, network, publishers
│   │
│   │  ── Layer 3: Coordinadores ──
│   ├── kafka-coordinator-runtime/    # Framework genérico: runtime, loader, writer, timer
│   ├── kafka-group/                  # Consumer groups, rebalance, offsets
│   ├── kafka-txn/                    # Transacciones, idempotencia, producer IDs
│   │
│   │  ── Layer 4: Broker ──
│   ├── kafka-server-common/          # Configs, metrics, sessions compartidas
│   ├── kafka-broker/                 # BrokerServer, KafkaApis, ReplicaManager, LogManager
│   │
│   │  ── Layer 5: Herramientas ──
│   ├── kafka-admin-cli/              # CLI: topics, groups, ACLs, configs
│   └── kafka-metadata-shell/         # Shell interactiva para metadata
│
├── schemas/                          # 198 JSON message schemas (mirror de Kafka)
│   └── *.json
│
├── docs/
│   ├── adr/                          # Architecture Decision Records
│   ├── vision/                       # 01_system_vision.md
│   ├── inventory/                    # 02_code_inventory_and_crate_mapping.md
│   └── standards/                    # Este documento
│
├── tests/
│   ├── integration/                  # Cross-crate integration tests
│   └── differential/                 # Tests contra Kafka Java
│
├── xtask/                            # cargo xtask: codegen, benchmarks, CI helpers
│   ├── Cargo.toml
│   └── src/main.rs
│
└── benches/                          # Criterion benchmarks compartidos
    └── Cargo.toml
```

### 1.2 Cargo.toml Raíz

```toml
[workspace]
resolver = "2"
members = [
    "crates/kafka-server-types",
    "crates/kafka-protocol",
    "crates/kafka-protocol-codegen",
    "crates/kafka-network",
    "crates/kafka-storage",
    "crates/kafka-tenant",
    "crates/kafka-quota",
    "crates/kafka-raft",
    "crates/kafka-metadata",
    "crates/kafka-controller",
    "crates/kafka-controller-server",
    "crates/kafka-coordinator-runtime",
    "crates/kafka-group",
    "crates/kafka-txn",
    "crates/kafka-server-common",
    "crates/kafka-broker",
    "crates/kafka-admin-cli",
    "crates/kafka-metadata-shell",
    "xtask",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "Apache-2.0"
repository = "https://github.com/kafka-rust/kafka"

[workspace.dependencies]
# Runtime
tokio = { version = "1.43", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec", "time"] }

# Serialización y buffers
bytes = "1.9"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Concurrencia
arc-swap = "1.7"
dashmap = "6.1"
parking_lot = "0.12"
crossbeam-channel = "0.5"

# Errores
thiserror = "2.0"

# Telemetría
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"

# Crypto y compresión
crc32c = "0.6"
lz4_flex = "0.11"
snap = "1.1"
zstd = "0.13"
tokio-rustls = "0.26"

# Configuración
figment = { version = "0.10", features = ["toml", "env", "yaml"] }

# CLI
clap = { version = "4.5", features = ["derive"] }

# Testing
criterion = { version = "0.5", features = ["async_tokio"] }
proptest = "1.5"
tempfile = "3.14"

# Workspace crates (versionados como path)
kafka-server-types = { path = "crates/kafka-server-types" }
kafka-protocol = { path = "crates/kafka-protocol" }
kafka-tenant = { path = "crates/kafka-tenant" }
kafka-quota = { path = "crates/kafka-quota" }
kafka-network = { path = "crates/kafka-network" }
kafka-storage = { path = "crates/kafka-storage" }
kafka-raft = { path = "crates/kafka-raft" }
kafka-metadata = { path = "crates/kafka-metadata" }
kafka-controller = { path = "crates/kafka-controller" }
kafka-coordinator-runtime = { path = "crates/kafka-coordinator-runtime" }
kafka-group = { path = "crates/kafka-group" }
kafka-txn = { path = "crates/kafka-txn" }
kafka-server-common = { path = "crates/kafka-server-common" }
kafka-broker = { path = "crates/kafka-broker" }

[workspace.lints.rust]
unsafe_code = "deny"          # Override explícito por crate si necesario

[workspace.lints.clippy]
all = { level = "deny" }
pedantic = { level = "warn" }
nursery = { level = "warn" }
unwrap_used = { level = "deny" }
expect_used = { level = "warn" }
panic = { level = "deny" }

[profile.release]
lto = "thin"
codegen-units = 4
strip = "symbols"
panic = "abort"

[profile.bench]
inherits = "release"
debug = 1                    # Para perfiles con perf/flamegraph
```

### 1.3 rust-toolchain.toml

```toml
[toolchain]
channel = "1.85.0"
components = ["rustfmt", "clippy", "rust-src", "miri"]
targets = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"]
```

### 1.4 rustfmt.toml

```toml
edition = "2024"
max_width = 100
tab_spaces = 4
use_field_init_shorthand = true
use_try_shorthand = true
imports_granularity = "Crate"
group_imports = "StdExternalCrate"
```

---

## 2. Contratos entre Crates: Traits, Tipos Compartidos y API Pública

### 2.1 Principios de Diseño de API Inter-Crate

| Principio | Regla | Justificación |
|---|---|---|
| **Traits como firewalls** | Cada crate expone traits en su raíz `pub mod traits;`. Las implementaciones viven en módulos internos `pub(crate)`. | Desacopla consumidores de implementaciones concretas. Permite testing con mocks. |
| **Genéricos acotados** | Evitar `fn foo<T: Trait1 + Trait2 + Trait3>()` en APIs públicas si `T` solo tiene una implementación real. Preferir `&dyn Trait` o tipo concreto. | Reduce tiempos de compilación. Mejora mensajes de error. |
| **Newtype idiom** | Identificadores como `TenantId`, `PartitionId`, `TopicId`, `BrokerId` son newtypes (`pub struct TenantId(Arc<str>)`). No raw strings ni enteros. | Type safety en tiempo de compilación. |
| **Re-exports selectivos** | Cada crate re-exporta en su `lib.rs` solo lo que forma parte de su API pública. Usar `#[doc(hidden)]` para internals expuestos por necesidad técnica. | Superficie de API controlada. |
| **Versionado semántico interno** | Aunque son path dependencies, respetar semver en las APIs. Breaking changes requieren ADR. | Gobernanza entre equipos de crate. |

### 2.2 Patrón de Módulos por Crate

```
crates/kafka-{name}/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Re-exports públicos + #![doc = ...]
│   ├── traits.rs        # Traits públicos del crate
│   ├── types.rs         # Tipos públicos (structs, enums)
│   ├── error.rs         # Tipo de error del crate
│   ├── config.rs        # Configuración del crate (si aplica)
│   └── internal/        # Implementaciones privadas
│       ├── mod.rs
│       └── ...
└── tests/
    ├── unit/            # Tests unitarios
    └── integration/     # Tests de integración del crate
```

### 2.3 Tipos Compartidos (`kafka-server-types`)

Este crate es la **raíz del DAG** — no depende de ningún otro crate del workspace. Define:

```rust
// Identificadores semánticos (newtypes)
pub struct BrokerId(pub i32);
pub struct TopicId(pub Uuid);
pub struct TopicName(pub Arc<str>);
pub struct PartitionId(pub i32);
pub struct TopicIdPartition { pub topic_id: TopicId, pub partition: PartitionId }
pub struct LeaderEpoch(pub i32);
pub struct Offset(pub i64);

// Metadata versioning
pub struct MetadataVersion { /* KIP-584 version enum */ }
pub struct KRaftVersion(pub i16);
pub struct FeatureVersion { pub name: String, pub level: i16 }

// Traits fundamentales
pub trait Timestamped { fn timestamp(&self) -> i64; }
pub trait Identifiable<Id> { fn id(&self) -> &Id; }
```

### 2.4 Fronteras Contractuales (Ejemplo de Traits como Firewalls)

```rust
// kafka-coordinator-runtime/src/traits.rs
/// Abstracción de escritura a log de partición.
/// Implementada por kafka-broker; no conoce coordinadores.
pub trait PartitionWriter: Send + Sync + 'static {
    fn append(
        &self,
        tp: &TopicIdPartition,
        records: Vec<CoordinatorRecord>,
        ctx: &RequestContext,
    ) -> impl Future<Output = Result<Offset, CoordinatorError>> + Send;
}

// kafka-quota/src/traits.rs  
/// Motor de enforcement de cuotas. Transversal.
pub trait QuotaEnforcer: Send + Sync + 'static {
    fn check_and_maybe_throttle(
        &self,
        key: &QuotaKey,
        resource: QuotaResource,
        amount: u64,
    ) -> ThrottleResult;

    fn record_usage(
        &self,
        key: &QuotaKey,
        resource: QuotaResource,
        amount: u64,
    );
}

// kafka-tenant/src/traits.rs
/// Resuelve identidad de tenant desde contexto de conexión.
pub trait TenantResolver: Send + Sync + 'static {
    fn resolve(&self, conn: &ConnectionContext) -> Result<TenantId, TenantError>;
    fn map_topic_name(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceId,
        topic: &TopicName,
    ) -> InternalTopicName;
}

// kafka-storage/src/traits.rs
/// Abstracción de append al log. No conoce el protocolo wire.
pub trait LogAppender: Send + Sync + 'static {
    fn append(
        &self,
        records: &RecordBatch,
        ctx: &RequestContext,
    ) -> impl Future<Output = Result<LogAppendInfo, StorageError>> + Send;
}
```

---

## 3. Reglas de Oro de Concurrencia

### 3.1 Tokio como Runtime Principal

| Regla | Detalle |
|---|---|
| **Runtime único por rol** | Cada proceso usa **un** `tokio::runtime::Runtime` para el data plane y **uno** para el control plane. NO compartir runtimes entre planos. |
| **Configuración explícita** | `Runtime::Builder::new_multi_thread().worker_threads(N)`. Nunca `#[tokio::main]` con defaults en producción. |
| **Flavor correcto** | Data plane: `multi_thread`. Control plane controller: `current_thread` (single-threaded event loop, como `QuorumController.java`). |
| **TaskLocal para contexto** | `tokio::task_local!` para propagar `RequestContext` (tenant, trace_id, deadline). No global statics. |

### 3.2 Clasificación de Trabajo

```rust
/// Clasificación de trabajo para scheduling y accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkClass {
    /// Hot path: produce, fetch, metadata. Ejecutar en runtime async.
    DataPlane,
    /// Control plane: raft, controller mutations. Runtime dedicado.
    ControlPlane,
    /// IO-bound: fsync, segment roll, index rebuild. spawn_blocking.
    DiskIo,
    /// CPU-bound: compaction, compression, checksum. Pool dedicado con semáforo.
    CpuIntensive,
    /// Background: cleanup, retention, metrics export. Baja prioridad.
    Maintenance,
}
```

### 3.3 Reglas para `spawn_blocking` y Pools Dedicados

| Regla | Cómo | Por qué |
|---|---|---|
| **Nunca bloquear el runtime async** | Toda operación que haga `fsync`, IO síncrono, compresión pesada o hashing intensivo DEBE ir por `spawn_blocking` o pool dedicado. | Un thread bloqueado en el runtime Tokio degrada a todos los tasks async del worker. |
| **Semáforos obligatorios** | Toda llamada a `spawn_blocking` pasa por `Arc<Semaphore>` con capacidad configurada. Default: `num_cpus * 2` para IO, `num_cpus` para CPU. | Sin límites, `spawn_blocking` lanza threads ilimitados y puede causar OOM o starvation. |
| **Semáforos por tenant** | Para operaciones costosas (compaction, segment roll), el semáforo es **por tenant**, con un techo global. Un tenant no puede monopolizar el pool. | Aislamiento de recursos multi-tenant. |
| **Pool dedicado para compaction** | Compaction usa `rayon::ThreadPool` separado, NO `spawn_blocking`. Capacidad configurable. | Compaction es CPU+IO intensivo y de larga duración; mezclarlo con `spawn_blocking` general contamina el pool. |
| **Timeout en pools** | Toda operación en pool tiene deadline derivado del `RequestContext`. Si expira, se cancela cooperativamente. | Evitar tareas zombie que consuman recursos indefinidamente. |

```rust
/// Pool acotado para operaciones bloqueantes.
pub struct BoundedBlockingPool {
    global_semaphore: Arc<Semaphore>,
    tenant_semaphores: DashMap<TenantId, Arc<Semaphore>>,
    max_per_tenant: usize,
}

impl BoundedBlockingPool {
    pub async fn spawn<F, T>(&self, tenant: &TenantId, f: F) -> Result<T, PoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // 1. Adquirir semáforo global
        let _global = self.global_semaphore.acquire().await?;
        // 2. Adquirir semáforo por tenant
        let tenant_sem = self.get_or_create_tenant_sem(tenant);
        let _tenant = tenant_sem.acquire().await?;
        // 3. Ejecutar en spawn_blocking
        tokio::task::spawn_blocking(f).await.map_err(Into::into)
    }
}
```

### 3.4 Reglas para Locks y Primitivas de Sincronización

| Primitiva | Cuándo Usar | Cuándo NO Usar |
|---|---|---|
| **`parking_lot::Mutex`** | Secciones críticas cortas (<1 µs) en hot paths síncronos. Ejemplo: actualizar un contador atómico compuesto. | Nunca en código async. Nunca proteger IO. |
| **`parking_lot::RwLock`** | Ratio lectura:escritura ≥ 10:1, secciones cortas. Ejemplo: leer metadata de particiones. | Si la sección crítica contiene `.await`. |
| **`tokio::sync::Mutex`** | Sección crítica que contiene `.await` (ejemplo: inicialización lazy de recurso async). | Hot paths; la contention es peor que `parking_lot`. |
| **`tokio::sync::RwLock`** | Ratio lectura:escritura alto CON `.await` dentro. Raro — preferir `ArcSwap`. | Hot paths de lectura (usar `ArcSwap` en su lugar). |
| **`ArcSwap<T>`** | Metadata publicada a muchos lectores (config, metadata image, quota snapshot). Lecturas sin lock. Escrituras atómicas. | Datos que mutan frecuentemente (>100/s) con tamaño grande. |
| **`DashMap<K, V>`** | Mapa concurrente con muchos escritores y lectores. Ejemplo: session cache, fetch session map. | Si el acceso es predominantemente read-only (usar `ArcSwap<HashMap<...>>`). |
| **`crossbeam-channel`** | Comunicación entre threads de `spawn_blocking` y el runtime async. Bounded siempre. | Dentro de código async puro (usar `tokio::sync::mpsc`). |
| **`tokio::sync::mpsc`** | Comunicación async entre tasks. Bounded siempre. | Bounded size = 0 (unbounded oculto). |
| **`tokio::sync::watch`** | Broadcast de valores que cambian infrecuentemente (config updates, shutdown signal). | Streaming de datos de alto volumen. |
| **`tokio::sync::broadcast`** | Fan-out de eventos a múltiples suscriptores (metadata deltas, tenant events). | Si solo hay un consumidor (usar `mpsc`). |

### 3.5 Regla de Prioridad de Selección

```
1. ¿Necesito compartir dato inmutable entre lectores? → Arc<T> o ArcSwap<T>
2. ¿Necesito comunicar entre tasks?                   → channels (mpsc, watch, broadcast)
3. ¿Necesito mutación en sección crítica con .await?   → tokio::sync::Mutex (raro)
4. ¿Necesito mutación en sección crítica sin .await?   → parking_lot::Mutex (medir contention)
5. ¿Necesito mapa concurrente mutable?                 → DashMap (medir vs RwLock<HashMap>)
```

> **Anti-patrón prohibido:** `Arc<Mutex<HashMap<K, V>>>` sin benchmarks que demuestren que `DashMap` o `ArcSwap<HashMap<...>>` no son viables.

### 3.6 Pools de Memoria

| Pool | Uso | Implementación |
|---|---|---|
| **`bytes::BytesMut` pool** | Buffers de red reutilizables para leer/escribir frames del protocolo. | `BytesMut::with_capacity(8192)` por conexión, reutilizado entre requests. |
| **Record batch buffers** | Pre-allocated buffers para serializar `RecordBatch` antes de append. | Pool thread-local de `Vec<u8>` con capacidad típica (16 KiB). |
| **Compaction maps** | Offset maps de compaction pre-allocated. | `Vec<u8>` reutilizado por hilo de compaction. No global. |

---

## 4. Manejo Estándar de Errores

### 4.1 Taxonomía de Errores

```
KafkaError (crate-level)
├── Domain errors   — violaciones de invariantes de negocio
│   ├── TopicNotFound
│   ├── PartitionNotLeader
│   ├── OffsetOutOfRange
│   ├── UnknownMemberId
│   └── QuotaViolation { tenant, resource, limit, usage }
│
├── Protocol errors — mapeados 1:1 con ErrorCode del wire protocol
│   ├── ErrorCode(i16)  — los ~90 error codes de Kafka
│   └── InvalidRequest { api_key, version, detail }
│
├── Recoverable     — reintentar tiene sentido
│   ├── NotLeaderOrFollower
│   ├── CoordinatorNotAvailable
│   ├── NetworkError(io::Error)
│   └── Timeout { operation, duration }
│
├── Fatal           — el proceso debe abortar o el componente debe restartear
│   ├── CorruptedLog { segment, offset, detail }
│   ├── RaftQuorumLost
│   ├── StorageFull
│   └── InvariantViolation(String)
│
└── Internal        — bugs, nunca expuestos al cliente
    ├── Bug(String)                  — .expect() wrappers
    └── Unimplemented(String)        — features pendientes
```

### 4.2 Convenciones

| Regla | Detalle |
|---|---|
| **Tipo de error por crate** | Cada crate define `pub enum {Crate}Error` con `#[derive(thiserror::Error)]`. |
| **Conversión entre crates** | `impl From<StorageError> for BrokerError`. Conversiones explícitas, no genéricas. |
| **`Result<T, E>` siempre** | Toda función fallible retorna `Result`. No panics en código de producción. |
| **`?` operator** | Preferido sobre `match` para propagación. Usar `map_err` para añadir contexto. |
| **Error codes del protocolo** | Cada error de dominio implementa `fn error_code(&self) -> i16` para mapear al wire protocol. |
| **Tracing en errores** | Los errores se loguean al nivel apropiado en el punto de manejo, no en el punto de creación. |
| **No `unwrap()` / `panic!()`** | Prohibidos en producción. Usar `expect("reason")` solo en paths de inicialización (startup). En CI: `#![deny(clippy::unwrap_used, clippy::panic)]`. |

### 4.3 Patrón de FaultHandler (Adaptado de Java)

El Java `FaultHandler` de Kafka distingue entre fallos que deben loguear vs que deben terminar el proceso. En Rust:

```rust
/// Política de manejo de fallos fatales.
pub trait FaultHandler: Send + Sync + 'static {
    /// Maneja un fallo. Retorna un error que el caller puede propagar
    /// o causa terminación si la política lo dicta.
    fn handle_fault(&self, message: &str, cause: Option<&dyn std::error::Error>) -> FatalError;
}

/// FaultHandler que loguea y retorna el error (no termina el proceso).
pub struct LoggingFaultHandler { name: String }

/// FaultHandler que loguea y termina el proceso (para invariantes rotas).
pub struct ProcessTerminatingFaultHandler { name: String, exit_code: i32 }
```

### 4.4 Telemetría Estructurada

```rust
// Toda operación importante emite un span de tracing con campos estandarizados.
#[tracing::instrument(
    skip(self, records),
    fields(
        tenant_id = %ctx.tenant_id,
        topic = %tp.topic_name,
        partition = tp.partition.0,
        otel.kind = "PRODUCER",
    )
)]
pub async fn append_records(
    &self,
    tp: &TopicIdPartition,
    records: &RecordBatch,
    ctx: &RequestContext,
) -> Result<LogAppendInfo, StorageError> { ... }
```

| Campo obligatorio en spans | Tipo | Cuándo |
|---|---|---|
| `tenant_id` | String | Siempre que haya `RequestContext` |
| `topic` / `partition` | String / i32 | Operaciones sobre particiones |
| `api_key` | i16 | Procesamiento de requests del protocolo |
| `correlation_id` | i32 | Trazabilidad request/response |
| `broker_id` | i32 | Operaciones entre brokers |

---

## 5. Política de `unsafe`

### 5.1 Cuándo se Permite

| Caso | Ejemplo | Condiciones |
|---|---|---|
| **Interop FFI** | Bindings a libzstd si `zstd` crate no cumple rendimiento | FFI auditado. Tests con miri. |
| **Optimizaciones de hot path con evidencia** | `SliceIndex::get_unchecked` en decodificación de messages con bounds pre-validados | Benchmark que demuestre ≥15% mejora. |
| **Memory-mapped files** | `mmap` para índices de offset/time (como `AbstractIndex.java` usa `MappedByteBuffer`) | Encapsulado en módulo dedicado. Más detalle abajo. |
| **Lock-free data structures** | Si `dashmap`/`arc-swap` no cubren un caso medido | Tests con `loom`. |

### 5.2 Cuándo NO se Permite

- **Nunca** para evitar lifetime/borrow checker "molesto".
- **Nunca** `unsafe impl Send/Sync` sin auditoría formal.
- **Nunca** raw pointer arithmetic sin `// SAFETY:` que demuestre invariantes.

### 5.3 Proceso de Revisión

1. **Comentario `// SAFETY:`** obligatorio por cada bloque `unsafe`, explicando qué invariantes se mantienen y por qué.
2. **Encapsulación** en módulo `unsafe_impl` con `#[allow(unsafe_code)]` explícito (el workspace tiene `#![deny(unsafe_code)]` por defecto).
3. **Tests con miri** para todo bloque `unsafe`. Ejecutar `cargo +nightly miri test` en CI.
4. **Fuzzing con `cargo-fuzz`** para todo bloque `unsafe` que procese input externo (decodificación de mensajes, parsing de índices).
5. **Revisión por dos** — todo PR con `unsafe` requiere aprobación de 2 reviewers.

### 5.4 Auditoría de Dependencias con Unsafe

```bash
# Contar bloques unsafe en dependencias
cargo geiger --output-format ascii-tree

# Verificar auditorías de crates
cargo vet
```

---

## 6. Estrategia de Configuración

### 6.1 Arquitectura en Capas

```
┌────────────────────────────────────────────────────┐
│  Layer 4: Runtime overrides (admin API / metadata)  │  ← Hot-reload vía ArcSwap
├────────────────────────────────────────────────────┤
│  Layer 3: ENV variables (KAFKA_*)                   │  ← Cloud/K8s friendly
├────────────────────────────────────────────────────┤
│  Layer 2: Config file (TOML)                        │  ← Primary file config
├────────────────────────────────────────────────────┤
│  Layer 1: CLI flags (--broker-id, --log-dir)        │  ← Override puntual
├────────────────────────────────────────────────────┤
│  Layer 0: Compiled defaults                         │  ← Valores por defecto en código
└────────────────────────────────────────────────────┘

Precedencia: Layer 4 > Layer 3 > Layer 2 > Layer 1 > Layer 0
```

### 6.2 Formato y Librería

| Decisión | Valor | Justificación |
|---|---|---|
| **Formato de archivo** | TOML | Nativo en Rust, tipado (a diferencia de YAML), soporte de comentarios. |
| **Librería de fusión** | `figment` | Soporta múltiples providers (TOML, ENV, defaults) con merge y override por capa. |
| **Serialización** | `serde` | Estándar de facto. Derive macros para validación en deserialización. |
| **Hot-reload** | `ArcSwap<Config>` | Lecturas sin lock. El controller publica config updates vía metadata deltas. |
| **ENV prefix** | `KAFKA_` | `KAFKA_BROKER_ID=1`, `KAFKA_LOG_DIRS=/data/kafka`. Separador `_` + lowercase. |

### 6.3 Estructura de Configuración

```rust
/// Configuración raíz del proceso.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub node: NodeConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub raft: RaftConfig,
    pub quotas: QuotaDefaults,
    pub tenancy: TenancyConfig,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub broker_id: BrokerId,
    pub process_roles: Vec<ProcessRole>,   // [broker], [controller], [broker, controller]
    pub cluster_id: String,
    pub advertised_listeners: Vec<Listener>,
    pub controller_quorum_voters: Vec<QuorumVoter>,
}

// Cada sub-config implementa Default con valores de Kafka original.
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            log_dirs: vec![PathBuf::from("/var/kafka/data")],
            log_segment_bytes: 1_073_741_824,       // 1 GiB
            log_retention_hours: 168,                // 7 días
            log_retention_bytes: -1,                 // ilimitado
            log_flush_interval_messages: i64::MAX,   // disable by default
            num_io_threads: 8,
            // ...
        }
    }
}
```

### 6.4 Versionado de Config

```toml
# kafka.toml
_config_version = 1          # Monotónicamente creciente. Permite migraciones.

[node]
broker_id = 1
process_roles = ["broker"]

[storage]
log_dirs = ["/data/kafka-1", "/data/kafka-2"]
```

Cuando `_config_version` cambia, la función `migrate_config(old_version, new_version, raw)` transforma la config automáticamente.

---

## 7. Guía de Testing

### 7.1 Pirámide de Tests por Crate

```
                    ┌──────────┐
                    │ Diff     │  Tests diferenciales contra Kafka Java
                    │ Tests    │  (tests/differential/)
                    ├──────────┤
                 ┌──┤ Integr.  │  Cross-crate, multi-componente
                 │  │ Tests    │  (tests/integration/)
                 │  ├──────────┤
              ┌──┤  │ Fuzzing  │  Inputs adversariales (cargo-fuzz)
              │  │  ├──────────┤
           ┌──┤  │  │ PropTest │  Invariantes con inputs generados (proptest)
           │  │  │  ├──────────┤
           │  │  │  │ Loom     │  Concurrencia (lock-free, reordering)
        ┌──┤  │  │  ├──────────┤
        │  │  │  │  │ Miri     │  UB detection (unsafe blocks)
     ┌──┤  │  │  │  ├──────────┤
     │  │  │  │  │  │ Unit     │  Funciones puras, lógica de dominio
     │  │  │  │  │  │ Tests    │  (crate-level #[test])
     └──┴──┴──┴──┴──┴──────────┘
```

### 7.2 Reglas por Categoría

| Categoría | Runner | Cuándo Aplica | Ejemplo |
|---|---|---|---|
| **Unit tests** | `cargo nextest run` | Todo crate. Lógica pura sin IO. | Serialización de messages, cálculo de CRC, resolución de quota key. |
| **Property-based** | `proptest` | Funciones con dominio amplio de inputs. | "Para cualquier `RecordBatch` válido, `decode(encode(batch)) == batch`". |
| **Loom** | `cargo test --features loom` | Estructuras lock-free o patrones multi-threaded delicados. | Tests de `ArcSwap`-based metadata cache, concurrent segment operations. |
| **Miri** | `cargo +nightly miri test` | Cada módulo que use `unsafe`. | Tests de mmap index wrappers, zero-copy buffer manipulations. |
| **Fuzzing** | `cargo fuzz` | Todo parser de input externo. | Wire protocol decoder, config parser, log segment reader. |
| **Benchmarks** | `criterion` | Hot paths. Mínimo: append, fetch, decode, encode. | `benches/protocol_decode.rs`, `benches/storage_append.rs`. |
| **Integration** | `cargo nextest run -p integration-tests` | Interacción entre 2+ crates. | Broker + Storage: produce→commit→fetch round-trip. |
| **Differential** | Custom harness | Verificar compatibilidad wire. | Enviar mismo request a Kafka Java y Kafka Rust, comparar bytes de response. |

### 7.3 Estructura de Features para Testing

```toml
# Cargo.toml de un crate (ejemplo: kafka-storage)
[features]
default = []
test-utils = []            # Expone builders, fakes y helpers para tests de otros crates
loom = ["dep:loom"]        # Habilita tests de concurrencia con loom

[dev-dependencies]
proptest = { workspace = true }
tempfile = { workspace = true }
criterion = { workspace = true }
```

### 7.4 CI Pipeline (Mínimo)

```yaml
# .github/workflows/ci.yml (pseudocódigo)
steps:
  - cargo fmt --check
  - cargo clippy --all-targets --deny warnings
  - cargo nextest run --workspace
  - cargo nextest run --workspace --features loom    # Loom tests
  - cargo +nightly miri test --workspace             # Miri (solo crates con unsafe)
  - cargo deny check                                 # Licencias, advisories
  - cargo llvm-cov --workspace --lcov > coverage.lcov
```

---

## 8. Contrato de Contexto Multi-Tenant

### 8.1 Tipos Fundamentales (`kafka-tenant`)

```rust
/// Identificador de tenant. Inmutable, barato de clonar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(pub Arc<str>);

/// Identificador de namespace dentro de un tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub Arc<str>);

/// Nombre de topic como lo ve el cliente (sin prefijo de tenant).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalTopicName(pub Arc<str>);

/// Nombre de topic interno con prefijo de tenant/namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InternalTopicName(pub Arc<str>);

/// Tenant especial: el operador del clúster.
pub const SYSTEM_TENANT: TenantId = TenantId(Arc::from("__system"));
/// Namespace por defecto.
pub const DEFAULT_NAMESPACE: NamespaceId = NamespaceId(Arc::from("default"));
```

### 8.2 RequestContext — El Objeto de Contexto Universal

```rust
/// Contexto propagado a través de toda la cadena de procesamiento de un request.
/// NUNCA usar statics globales para obtener esta información.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Identidad del tenant resuelto (de autenticación o header).
    pub tenant_id: TenantId,
    /// Namespace resuelto.
    pub namespace_id: NamespaceId,
    /// Key para resolución de cuotas (puede incluir user, client-id, IP).
    pub quota_key: QuotaKey,
    /// Clasificación de trabajo para scheduling.
    pub work_class: WorkClass,
    /// Deadline absoluto del request.
    pub deadline: Instant,
    /// Span de tracing para correlación.
    pub trace_span: tracing::Span,
    /// Presupuesto de recursos restante para este request.
    pub resource_budget: ResourceBudget,
}
```

### 8.3 QuotaKey — Clave de Resolución de Cuotas

```rust
/// Clave compuesta para lookup de cuotas.
/// Sigue la jerarquía: Cluster > Tenant > Namespace > Topic/Group/ClientId.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuotaKey {
    pub tenant_id: TenantId,
    pub namespace_id: Option<NamespaceId>,
    pub entity: QuotaEntity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuotaEntity {
    /// Cuota a nivel de tenant (agregada).
    Tenant,
    /// Cuota de un topic específico.
    Topic(TopicName),
    /// Cuota de un consumer group.
    ConsumerGroup(GroupId),
    /// Cuota por user + client-id (compatible con Kafka original).
    UserClient { user: Option<String>, client_id: Option<String> },
    /// Cuota por IP (connection rate).
    Ip(IpAddr),
}
```

### 8.4 ResourceBudget — Accounting de Recursos por Request

```rust
/// Presupuesto de recursos asignado a un request individual.
/// Permite accounting granular y early termination si se agotan recursos.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    /// Bytes de produce permitidos en este request.
    pub produce_bytes_remaining: AtomicU64,
    /// Bytes de fetch permitidos en este request.
    pub fetch_bytes_remaining: AtomicU64,
    /// Tiempo de CPU restante (en nanosegundos).
    pub cpu_nanos_remaining: AtomicU64,
}

impl ResourceBudget {
    /// Consume bytes de produce. Retorna error si el presupuesto se agota.
    pub fn consume_produce_bytes(&self, amount: u64) -> Result<(), QuotaViolation> {
        loop {
            let current = self.produce_bytes_remaining.load(Ordering::Relaxed);
            if current < amount {
                return Err(QuotaViolation::ProduceBytesExceeded { limit: current, requested: amount });
            }
            if self.produce_bytes_remaining
                .compare_exchange_weak(current, current - amount, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}
```

### 8.5 Regla de Propagación de Contexto

| Regla | Implementación |
|---|---|
| **Toda API interna que consuma CPU/IO recibe `&RequestContext`** | Es el último o penúltimo parámetro de toda función que haga IO significativo. |
| **El contexto se crea en el boundary de red** | `kafka-network` crea el `RequestContext` al decodificar el request, resolviendo tenant via `TenantResolver`. |
| **El contexto se propaga, no se reconstruye** | Pasar el mismo `&RequestContext` por toda la cadena. No reconstruirlo en capas intermedias. |
| **Operaciones internas (Raft, metadata) usan `SYSTEM_TENANT`** | El contexto de sistema tiene cuotas elevadas y se marca como no-tenant para evitar accounting. |

### 8.6 Diagrama de Flujo: Request con Contexto

```
Cliente → [kafka-network]
           │
           ├── 1. Decode wire frame
           ├── 2. TenantResolver.resolve(&conn) → TenantId
           ├── 3. QuotaEnforcer.check_and_maybe_throttle()
           ├── 4. Construir RequestContext { tenant_id, quota_key, deadline, budget, ... }
           │
           └── [kafka-broker] KafkaApis.handle(request, &ctx)
                │
                ├── [kafka-storage] log.append(records, &ctx)
                │   └── BoundedBlockingPool.spawn(&ctx.tenant_id, fsync)
                │
                ├── [kafka-group] coordinator.join_group(req, &ctx)
                │
                └── Response con throttle_time_ms si quota fue tocada
```

---

## 9. Resumen de Crates y su Stack de Dependencias Externas

| Crate | Deps externas principales | Deps internas |
|---|---|---|
| `kafka-server-types` | `serde`, `uuid` | (ninguna) |
| `kafka-protocol` | `bytes`, `serde`, `crc32c`, compression crates | `kafka-server-types` |
| `kafka-tenant` | `serde`, `tracing` | `kafka-server-types` |
| `kafka-quota` | `parking_lot`, `tokio`, `tracing`, `metrics` | `kafka-server-types`, `kafka-tenant` |
| `kafka-network` | `tokio`, `tokio-rustls`, `bytes`, `tracing` | `kafka-protocol`, `kafka-tenant`, `kafka-quota` |
| `kafka-storage` | `tokio`, `bytes`, `memmap2`, `tracing` | `kafka-server-types`, `kafka-protocol` |
| `kafka-raft` | `tokio`, `bytes`, `tracing`, `rand` | `kafka-protocol`, `kafka-server-types` |
| `kafka-metadata` | `arc-swap`, `tracing` | `kafka-server-types`, `kafka-protocol` |
| `kafka-controller` | `tokio`, `tracing` | `kafka-metadata`, `kafka-raft`, `kafka-quota`, `kafka-tenant` |
| `kafka-coordinator-runtime` | `tokio`, `tracing` | `kafka-protocol`, `kafka-server-types`, `kafka-metadata` |
| `kafka-group` | `tokio`, `tracing` | `kafka-coordinator-runtime`, `kafka-quota`, `kafka-tenant` |
| `kafka-txn` | `tokio`, `tracing` | `kafka-coordinator-runtime`, `kafka-protocol` |
| `kafka-server-common` | `figment`, `serde`, `tracing`, `clap` | `kafka-server-types`, `kafka-protocol`, `kafka-quota` |
| `kafka-broker` | `tokio`, `tracing`, `metrics` | (depende de casi todos los anteriores) |
| `kafka-admin-cli` | `clap`, `tokio` | `kafka-protocol`, `kafka-server-common` |

---

## 10. Herramientas de Gobernanza y CI

| Herramienta | Propósito | Configuración |
|---|---|---|
| `cargo-deny` | Licencias, bans de crates, duplicate deps, advisories | `deny.toml` |
| `cargo-audit` | CVE scanning | CI pipeline |
| `cargo-nextest` | Test runner paralelo con retry y partitioning | `.config/nextest.toml` |
| `cargo-llvm-cov` | Coverage de código | CI: meta ≥ 80% crates core |
| `cargo fmt` | Formateo uniforme | `rustfmt.toml` |
| `cargo clippy` | Linting | `--deny warnings`, `clippy.toml` para lint tuning |
| `cargo-geiger` | Conteo de `unsafe` en dependency tree | Reportes periódicos |
| `cargo-vet` | Auditoría de terceros | Supply chain security |
| `cargo xtask` | Comandos custom: codegen, benchmarks, diff-tests | `xtask/` |

---

## Apéndice A: Ejemplo de Crate Template (`kafka-tenant`)

```toml
# crates/kafka-tenant/Cargo.toml
[package]
name = "kafka-tenant"
version.workspace = true
edition.workspace = true
license.workspace = true
description = "Multi-tenant identity, namespace routing, and isolation for Kafka-Rust"

[dependencies]
kafka-server-types = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }
arc-swap = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }

[features]
default = []
test-utils = []    # Expone TenantId::test("test-tenant") y helpers

[lints]
workspace = true
```

```rust
// crates/kafka-tenant/src/lib.rs
#![doc = include_str!("../README.md")]

pub mod traits;
pub mod types;
pub mod error;

pub use traits::TenantResolver;
pub use types::{TenantId, NamespaceId, ExternalTopicName, InternalTopicName};
pub use types::{SYSTEM_TENANT, DEFAULT_NAMESPACE};
pub use error::TenantError;
```

---

## Apéndice B: Checklist de Revisión para PRs

- [ ] ¿El PR introduce `unsafe`? → Sección 5 (requiere `// SAFETY:`, miri tests, 2 approvals).
- [ ] ¿El PR añade un `Arc<Mutex<…>>`? → Justificar por qué no usar `ArcSwap`, `DashMap` o channels.
- [ ] ¿El PR añade `spawn_blocking`? → ¿Usa semáforo? ¿Semáforo por tenant?
- [ ] ¿La función hace IO significativo? → ¿Recibe `&RequestContext`?
- [ ] ¿El PR añade dependencia externa? → Cumple criterios de sección 8 de la visión.
- [ ] ¿El PR tiene tests para el path feliz y al menos un error path?
- [ ] ¿Los errores implementan `error_code()` si son observables por clientes?
- [ ] ¿Los spans de tracing incluyen `tenant_id`?

---

*Documento generado como parte del programa de reescritura Kafka → Rust.*  
*Prerequisitos: [01_system_vision.md](../vision/01_system_vision.md), [02_code_inventory_and_crate_mapping.md](../inventory/02_code_inventory_and_crate_mapping.md)*  
*Siguiente prompt: `04_protocolo_kafka_y_generacion_de_mensajes.md`*
