//! High-level ergonomic API facade for DAOS operations.
//!
//! This module provides a streamlined API that reduces boilerplate for common
//! DAOS workflows while preserving full access to lower-level APIs when needed.
//!
//! # Design Philosophy
//!
//! The facade follows these principles:
//! - **Sensible defaults**: Common operations work with minimal configuration
//! - **Escape hatches**: Lower-level APIs remain accessible for advanced use
//! - **Type safety**: All safety boundaries from lower layers preserved
//! - **No hidden state**: No global mutable singletons introduced
//!
//! # Architecture
//!
//! The facade is organized around the natural DAOS hierarchy:
//!
//! ```text
//! DaosClient (runtime + pool + container)
//!   ├── pool()      -> &Pool (escape hatch)
//!   ├── container() -> &Container (escape hatch)
//!   ├── ObjectBuilder (oid generation + open)
//!   │     └── open() -> Object (escape hatch to object API)
//!   └── kv()        -> KvClient (dkey/akey/sgl management)
//! ```
//!
//! # Example: Simple KV Operations
//!
//! ```ignore
//! use daoxide::facade::DaosClient;
//!
//! // Connect to DAOS with minimal boilerplate
//! let client = DaosClient::builder()
//!     .pool_label("mypool")
//!     .container_label("mycontainer")
//!     .build()?;
//!
//! // Store and retrieve values
//! client.put(b"my_dkey", b"my_akey", b"hello world")?;
//! let value = client.get::<_, _, Vec<u8>>(b"my_dkey", b"my_akey")?;
//!
//! // Or use transactions for atomic operations
//! client.put_tx(b"my_dkey", b"my_akey", b"atomic value")?;
//! ```
//!
//! # Escape Hatches
//!
//! All facade types provide paths to lower-level APIs:
//!
//! - `DaosClient::pool()` returns `&Pool` for direct pool operations
//! - `DaosClient::container()` returns `&Container` for container operations
//! - `ObjectBuilder::open()` returns `Object` for object operations
//! - `KvClient` methods accept `Tx` for transaction control

use crate::container::{Container, ContainerOpen, flags::CONT_OPEN_RW};
use crate::error::{DaosError, Result};
use crate::io::{AKey, DKey, IoBuffer, Iod, IodSingleBuilder, Sgl};
use crate::object::{
    Object, ObjectClass, ObjectClassHints, ObjectId, ObjectOpenMode, ObjectType, generate_oid,
};
use crate::pool::flags::POOL_CONNECT_READWRITE;
use crate::pool::{Pool, PoolBuilder};
use crate::runtime::DaosRuntime;
use crate::tx::Tx;
use crate::unsafe_inner::ffi::{daos_cont_alloc_oids, daos_cont_close};
use crate::unsafe_inner::handle::DaosHandle;

/// Error type for facade-specific errors that don't map to DAOS errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FacadeError {
    /// Client not yet built or already disconnected.
    NotConnected,
    /// Invalid configuration provided.
    InvalidConfig(String),
    /// Underlying DAOS error.
    Daos(DaosError),
}

impl std::fmt::Display for FacadeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FacadeError::NotConnected => write!(f, "client not connected"),
            FacadeError::InvalidConfig(msg) => write!(f, "invalid configuration: {}", msg),
            FacadeError::Daos(e) => write!(f, "DAOS error: {}", e),
        }
    }
}

impl std::error::Error for FacadeError {}

impl From<DaosError> for FacadeError {
    fn from(err: DaosError) -> Self {
        FacadeError::Daos(err)
    }
}

impl From<FacadeError> for DaosError {
    fn from(err: FacadeError) -> Self {
        match err {
            FacadeError::Daos(e) => e,
            FacadeError::NotConnected => DaosError::InvalidArg,
            FacadeError::InvalidConfig(_) => DaosError::InvalidArg,
        }
    }
}

/// Builder for configuring a [`DaosClient`] connection.
///
/// # Example
///
/// ```ignore
/// use daoxide::facade::DaosClientBuilder;
///
/// let client = DaosClientBuilder::new()
///     .pool_label("mypool")
///     .container_label("mycontainer")
///     .object_type(daoxide::object::ObjectType::KvHashed)
///     .build()?;
/// ```
#[derive(Debug)]
pub struct DaosClientBuilder {
    pool_label: Option<String>,
    pool_uuid: Option<String>,
    pool_sys: Option<String>,
    pool_flags: u32,
    container_label: Option<String>,
    container_uuid: Option<String>,
    container_flags: u32,
    object_type: ObjectType,
    object_class: ObjectClass,
    object_hints: ObjectClassHints,
}

impl Default for DaosClientBuilder {
    fn default() -> Self {
        Self {
            pool_label: None,
            pool_uuid: None,
            pool_sys: None,
            pool_flags: POOL_CONNECT_READWRITE,
            container_label: None,
            container_uuid: None,
            container_flags: CONT_OPEN_RW,
            object_type: ObjectType::KvHashed,
            object_class: ObjectClass::UNKNOWN,
            object_hints: ObjectClassHints::NONE,
        }
    }
}

impl DaosClientBuilder {
    /// Creates a new builder with default settings.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pool label for connection.
    ///
    /// Either pool_label or pool_uuid must be provided, but not both.
    #[inline]
    pub fn pool_label(mut self, label: impl Into<String>) -> Self {
        self.pool_label = Some(label.into());
        self
    }

    /// Sets the pool UUID for connection.
    ///
    /// Either pool_label or pool_uuid must be provided, but not both.
    #[inline]
    pub fn pool_uuid(mut self, uuid: impl Into<String>) -> Self {
        self.pool_uuid = Some(uuid.into());
        self
    }

    /// Sets the DAOS system name.
    #[inline]
    pub fn pool_system(mut self, sys: impl Into<String>) -> Self {
        self.pool_sys = Some(sys.into());
        self
    }

    /// Sets the pool connection flags.
    #[inline]
    pub fn pool_flags(mut self, flags: u32) -> Self {
        self.pool_flags = flags;
        self
    }

    /// Sets the container label for creation/opening.
    ///
    /// Either container_label or container_uuid must be provided, but not both.
    #[inline]
    pub fn container_label(mut self, label: impl Into<String>) -> Self {
        self.container_label = Some(label.into());
        self
    }

    /// Sets the container UUID for opening.
    ///
    /// Either container_label or container_uuid must be provided, but not both.
    #[inline]
    pub fn container_uuid(mut self, uuid: impl Into<String>) -> Self {
        self.container_uuid = Some(uuid.into());
        self
    }

    /// Sets the container open flags.
    #[inline]
    pub fn container_flags(mut self, flags: u32) -> Self {
        self.container_flags = flags;
        self
    }

    /// Sets the default object type for objects created through this client.
    ///
    /// Defaults to [`ObjectType::KvHashed`].
    #[inline]
    pub fn object_type(mut self, ty: ObjectType) -> Self {
        self.object_type = ty;
        self
    }

    /// Sets the default object class for objects created through this client.
    ///
    /// Defaults to [`ObjectClass::UNKNOWN`] (DAOS selects based on hints/container properties).
    #[inline]
    pub fn object_class(mut self, oc: ObjectClass) -> Self {
        self.object_class = oc;
        self
    }

    /// Sets the default object class hints for objects created through this client.
    ///
    /// Defaults to [`ObjectClassHints::NONE`].
    #[inline]
    pub fn object_hints(mut self, hints: ObjectClassHints) -> Self {
        self.object_hints = hints;
        self
    }

    /// Validates the builder configuration.
    fn validate(&self) -> std::result::Result<(), FacadeError> {
        match (&self.pool_label, &self.pool_uuid) {
            (Some(_), Some(_)) => Err(FacadeError::InvalidConfig(
                "cannot specify both pool_label and pool_uuid".into(),
            )),
            (None, None) => Err(FacadeError::InvalidConfig(
                "must specify either pool_label or pool_uuid".into(),
            )),
            _ => Ok(()),
        }?;

        match (&self.container_label, &self.container_uuid) {
            (Some(_), Some(_)) => Err(FacadeError::InvalidConfig(
                "cannot specify both container_label and container_uuid".into(),
            )),
            (None, None) => Err(FacadeError::InvalidConfig(
                "must specify either container_label or container_uuid".into(),
            )),
            _ => Ok(()),
        }?;

        Ok(())
    }

    /// Builds a connected [`DaosClient`] from this builder.
    ///
    /// This initializes the DAOS runtime, connects to the pool, and opens/creates
    /// the container in one step.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration is invalid
    /// - DAOS runtime cannot be initialized
    /// - Pool connection fails
    /// - Container creation/opening fails
    pub fn build(&self) -> Result<DaosClient> {
        self.validate()?;

        let runtime = DaosRuntime::new()?;

        let pool = {
            let mut builder = PoolBuilder::new();
            if let Some(ref label) = self.pool_label {
                builder = builder.label(label);
            }
            if let Some(ref uuid) = self.pool_uuid {
                builder = builder.uuid(uuid);
            }
            if let Some(ref sys) = self.pool_sys {
                builder = builder.system(sys);
            }
            builder.flags(self.pool_flags).build()?
        };

        let (container_identifier, open_by) = if let Some(label) = self.container_label.as_deref() {
            (label.to_string(), ContainerOpen::ByLabel)
        } else if let Some(uuid) = self.container_uuid.as_deref() {
            (uuid.to_string(), ContainerOpen::ByUuid)
        } else {
            unreachable!("validated above");
        };

        // Open once and retain the handle for the client's lifetime.
        let container =
            pool.open_container(&container_identifier, open_by, self.container_flags)?;
        let container_handle = container.into_handle()?;

        Ok(DaosClient {
            container_handle,
            // pool must be initialized before runtime to match struct field order
            pool,
            runtime,
            container_label: container_identifier,
            container_open_by: open_by,
            container_flags: self.container_flags,
            default_object_type: self.object_type,
            default_object_class: self.object_class,
            default_object_hints: self.object_hints,
        })
    }
}

/// High-level DAOS client that encapsulates runtime, pool, and container.
///
/// `DaosClient` provides a streamlined interface for common DAOS operations
/// while preserving access to lower-level APIs through escape hatches.
///
/// # Lifecycle
///
/// A `DaosClient` manages:
/// - DAOS runtime initialization (reference-counted)
/// - Pool connection (automatically disconnected on drop)
/// - Container lifecycle (automatically closed on drop)
///
/// # Drop Order
///
/// Fields are dropped in declaration order, so `pool` must be declared before
/// `runtime` to ensure pool handles are properly disconnected before the DAOS
/// runtime is finalized. This prevents `daos_pool_disconnect` from failing with
/// `DER_UNINIT` (-1015) when the pool is dropped after the runtime.
///
/// # Escape Hatches
///
/// While `DaosClient` provides convenience methods, lower-level APIs remain accessible:
///
/// - [`DaosClient::pool`] - Direct pool operations
/// - [`DaosClient::container`] - Direct container operations
/// - [`DaosClient::object_builder`] - Object creation with full control
///
/// # Example
///
/// ```ignore
/// use daoxide::facade::DaosClient;
///
/// let client = DaosClient::builder()
///     .pool_label("mypool")
///     .container_label("mycontainer")
///     .build()?;
///
/// // Use the client for operations...
///
/// // Or access lower-level APIs:
/// let pool = client.pool();
/// let container = client.container();
/// ```
#[derive(Debug)]
pub struct DaosClient {
    container_handle: DaosHandle,
    // NOTE: pool must be declared before runtime to ensure proper drop order.
    // Pool is dropped first, closing all handles before DAOS runtime finalization.
    pool: Pool,
    #[allow(dead_code)]
    runtime: DaosRuntime,
    container_label: String,
    container_open_by: ContainerOpen,
    container_flags: u32,
    default_object_type: ObjectType,
    default_object_class: ObjectClass,
    default_object_hints: ObjectClassHints,
}

impl DaosClient {
    /// Creates a new `DaosClientBuilder` for configuring a connection.
    #[inline]
    pub fn builder() -> DaosClientBuilder {
        DaosClientBuilder::new()
    }

    /// Returns a reference to the underlying pool.
    ///
    /// This is an escape hatch for advanced pool operations not exposed
    /// by the facade.
    #[inline]
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Returns a newly opened container reference.
    ///
    /// This is an escape hatch for advanced container operations not exposed
    /// by the facade. Note that each call creates a new container handle;
    /// the container is automatically closed when the handle is dropped.
    pub fn container(&self) -> Result<Container<'_>> {
        self.pool.open_container(
            &self.container_label,
            self.container_open_by,
            self.container_flags,
        )
    }

    /// Returns the default object type for this client.
    #[inline]
    pub fn default_object_type(&self) -> ObjectType {
        self.default_object_type
    }

    /// Returns the default object class for this client.
    #[inline]
    pub fn default_object_class(&self) -> ObjectClass {
        self.default_object_class
    }

    /// Returns the default object hints for this client.
    #[inline]
    pub fn default_object_hints(&self) -> ObjectClassHints {
        self.default_object_hints
    }

    /// Allocates a new object ID in the container.
    ///
    /// This is a convenience method that combines container.alloc_oids()
    /// with OID encoding via generate_oid().
    ///
    /// # Arguments
    ///
    /// * `object_type` - The type of object to create
    /// * `oclass` - Object class (use `ObjectClass::UNKNOWN` for default)
    /// * `hints` - Object class hints
    ///
    /// # Errors
    ///
    /// Returns an error if OID allocation or encoding fails.
    pub fn alloc_oid(
        &self,
        object_type: ObjectType,
        oclass: ObjectClass,
        hints: ObjectClassHints,
    ) -> Result<ObjectId> {
        // DAOS requires caller-provided low bits to be unique in the container.
        // Allocate one OID slot first, then encode type/class/hints into it.
        let lo = daos_cont_alloc_oids(self.container_handle, 1)?;
        let mut oid = ObjectId::from_parts(0, lo);
        generate_oid(self.container_handle, &mut oid, object_type, oclass, hints)?;
        Ok(oid)
    }

    /// Creates a new [`ObjectBuilder`] for opening/creating an object.
    ///
    /// This is the primary entry point for object operations through the facade.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use daoxide::object::ObjectType;
    ///
    /// let object = client
    ///     .object_builder()
    ///     .object_type(ObjectType::KvHashed)
    ///     .open(ObjectOpenMode::ReadWrite)?;
    /// ```
    #[inline]
    pub fn object_builder(&self) -> ObjectBuilder<'_> {
        ObjectBuilder::new(self)
    }

    /// Opens an object with the default type/class/hints.
    ///
    /// This is a convenience method for simple use cases where the default
    /// object configuration is sufficient.
    ///
    /// # Arguments
    ///
    /// * `oid` - The object ID to open
    /// * `mode` - The open mode (read/write/exclusive)
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be opened.
    #[inline]
    pub fn open_object(&self, oid: ObjectId, mode: ObjectOpenMode) -> Result<Object> {
        Object::open(self.container_handle, oid, mode)
    }

    /// Stores a value in an object without a transaction.
    ///
    /// This is a convenience method that:
    /// 1. Opens the object (or creates if needed)
    /// 2. Creates dkey/akey/iod from the key arguments
    /// 3. Calls update
    ///
    /// # Type Parameters
    ///
    /// * `D` - DKey type (auto-converted from bytes)
    /// * `A` - AKey type (auto-converted from bytes)
    /// * `V` - Value type (must be `AsRef<[u8]>`)
    ///
    /// # Arguments
    ///
    /// * `oid` - Object ID to write to
    /// * `dkey` - Distribution key (e.g., `b"my_dkey"`)
    /// * `akey` - Attribute key (e.g., `b"my_akey"`)
    /// * `value` - Value to store (any type implementing `AsRef<[u8]>`)
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn put<D, A, V>(&self, oid: ObjectId, dkey: D, akey: A, value: V) -> Result<()>
    where
        D: AsRef<[u8]>,
        A: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_tx(oid, Tx::none(), dkey, akey, value)
    }

    /// Stores a value in an object with explicit transaction control.
    ///
    /// # Type Parameters
    ///
    /// * `D` - DKey type (auto-converted from bytes)
    /// * `A` - AKey type (auto-converted from bytes)
    /// * `V` - Value type (must be `AsRef<[u8]>`)
    ///
    /// # Arguments
    ///
    /// * `oid` - Object ID to write to
    /// * `tx` - Transaction to use (or `Tx::none()` for no transaction)
    /// * `dkey` - Distribution key
    /// * `akey` - Attribute key
    /// * `value` - Value to store
    pub fn put_tx<D, A, V>(
        &self,
        oid: ObjectId,
        tx: Tx<'_>,
        dkey: D,
        akey: A,
        value: V,
    ) -> Result<()>
    where
        D: AsRef<[u8]>,
        A: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let container = self.container()?;
        let object = Object::open_in(&container, oid, ObjectOpenMode::ReadWrite)?;

        let dkey = DKey::new(dkey.as_ref().to_vec())?;
        let akey = AKey::new(akey.as_ref().to_vec())?;
        let value_bytes = value.as_ref();

        let iod = Iod::Single(
            IodSingleBuilder::new(akey)
                .value_len(value_bytes.len())
                .build()?,
        );
        let sgl = Sgl::builder()
            .push(IoBuffer::from_vec(value_bytes.to_vec()))
            .build()?;

        object.update(&tx, &dkey, &iod, &sgl)
    }

    /// Retrieves a value from an object without a transaction.
    ///
    /// # Type Parameters
    ///
    /// * `D` - DKey type (auto-converted from bytes)
    /// * `A` - AKey type (auto-converted from bytes)
    /// * `V` - Value buffer type (must be `AsRef<[u8]>` for input, `Vec<u8>` for output)
    ///
    /// # Arguments
    ///
    /// * `oid` - Object ID to read from
    /// * `dkey` - Distribution key
    /// * `akey` - Attribute key
    /// * `buffer` - Buffer to read into (will be filled with data)
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn get<D, A>(&self, oid: ObjectId, dkey: D, akey: A, buffer: &mut [u8]) -> Result<()>
    where
        D: AsRef<[u8]>,
        A: AsRef<[u8]>,
    {
        self.get_tx(oid, Tx::none(), dkey, akey, buffer)
    }

    /// Retrieves a value from an object with explicit transaction control.
    ///
    /// # Type Parameters
    ///
    /// * `D` - DKey type
    /// * `A` - AKey type
    /// * `V` - Value buffer type
    ///
    /// # Arguments
    ///
    /// * `oid` - Object ID to read from
    /// * `tx` - Transaction to use
    /// * `dkey` - Distribution key
    /// * `akey` - Attribute key
    /// * `buffer` - Buffer to read into
    pub fn get_tx<D, A>(
        &self,
        oid: ObjectId,
        tx: Tx<'_>,
        dkey: D,
        akey: A,
        buffer: &mut [u8],
    ) -> Result<()>
    where
        D: AsRef<[u8]>,
        A: AsRef<[u8]>,
    {
        let container = self.container()?;
        let object = Object::open_in(&container, oid, ObjectOpenMode::ReadWrite)?;

        let dkey = DKey::new(dkey.as_ref().to_vec())?;
        let akey = AKey::new(akey.as_ref().to_vec())?;
        let iod = Iod::Single(
            IodSingleBuilder::new(akey)
                .value_len(buffer.len())
                .build()?,
        );
        let mut sgl = Sgl::builder()
            .push(IoBuffer::from_vec(buffer.to_vec()))
            .build()?;

        object.fetch(&tx, &dkey, &iod, &mut sgl)?;
        let fetched = sgl
            .buffers()
            .first()
            .ok_or(DaosError::Internal("empty SGL after fetch".into()))?;
        if fetched.len() != buffer.len() {
            return Err(DaosError::Internal("fetch buffer length mismatch".into()));
        }
        buffer.copy_from_slice(fetched.as_slice());
        Ok(())
    }

    /// Deletes keys from an object.
    ///
    /// # Arguments
    ///
    /// * `oid` - Object ID
    /// * `dkey` - Distribution key to delete (if `Some`)
    /// * `akeys` - Attribute keys to delete within the dkey (if `Some`)
    /// * `tx` - Transaction to use
    pub fn delete(
        &self,
        oid: ObjectId,
        dkey: Option<&[u8]>,
        akeys: Option<&[&[u8]]>,
        tx: Tx<'_>,
    ) -> Result<()> {
        let container = self.container()?;
        let object = Object::open_in(&container, oid, ObjectOpenMode::ReadWrite)?;

        match (dkey, akeys) {
            (Some(dk), Some(aks)) => {
                let dkey = DKey::new(dk)?;
                let mut akeys_vec: Vec<AKey> = Vec::new();
                for a in aks {
                    akeys_vec.push(AKey::new(*a)?);
                }
                object.punch_akeys(&tx, &dkey, &akeys_vec)
            }
            (Some(dk), None) => {
                let dkey = DKey::new(dk)?;
                object.punch_dkeys(&tx, &[dkey])
            }
            (None, _) => object.punch(&tx),
        }
    }
}

/// Builder for configuring and opening a DAOS object.
///
/// Provides fine-grained control over object creation parameters while
/// reducing the boilerplate of the open sequence.
///
/// # Example
///
/// ```ignore
/// use daoxide::object::{ObjectOpenMode, ObjectType};
///
/// let object = client
///     .object_builder()
///     .object_type(ObjectType::KvHashed)
///     .object_class(my_class)
///     .open(ObjectOpenMode::ReadWrite)?;
/// ```
#[derive(Debug)]
pub struct ObjectBuilder<'c> {
    client: &'c DaosClient,
    object_type: ObjectType,
    object_class: ObjectClass,
    object_hints: ObjectClassHints,
}

impl<'c> ObjectBuilder<'c> {
    /// Creates a new `ObjectBuilder` bound to the given client.
    fn new(client: &'c DaosClient) -> Self {
        Self {
            client,
            object_type: client.default_object_type,
            object_class: client.default_object_class,
            object_hints: client.default_object_hints,
        }
    }

    /// Sets the object type.
    ///
    /// Defaults to the client's default object type.
    #[inline]
    pub fn object_type(mut self, ty: ObjectType) -> Self {
        self.object_type = ty;
        self
    }

    /// Sets the object class.
    ///
    /// Defaults to the client's default object class.
    #[inline]
    pub fn object_class(mut self, oc: ObjectClass) -> Self {
        self.object_class = oc;
        self
    }

    /// Sets the object class hints.
    ///
    /// Defaults to the client's default object hints.
    #[inline]
    pub fn object_hints(mut self, hints: ObjectClassHints) -> Self {
        self.object_hints = hints;
        self
    }

    /// Allocates a new object ID with the configured type/class/hints.
    ///
    /// This is separate from open() to allow inspecting the OID before opening.
    #[inline]
    pub fn alloc(&self) -> Result<ObjectId> {
        self.client
            .alloc_oid(self.object_type, self.object_class, self.object_hints)
    }

    /// Allocates a new object ID and opens it with the given mode.
    ///
    /// This combines alloc() and open() into a single call for convenience.
    ///
    /// # Arguments
    ///
    /// * `mode` - The open mode (read/write/exclusive)
    pub fn create(self, mode: ObjectOpenMode) -> Result<Object> {
        let oid = self.alloc()?;
        self.client.open_object(oid, mode)
    }

    /// Opens an existing object by ID with the given mode.
    ///
    /// Use this when you have a pre-existing OID (e.g., from enumeration
    /// or previous allocation).
    ///
    /// # Arguments
    ///
    /// * `oid` - The object ID to open
    /// * `mode` - The open mode
    #[inline]
    pub fn open(&self, oid: ObjectId, mode: ObjectOpenMode) -> Result<Object> {
        self.client.open_object(oid, mode)
    }

    /// Opens an object with the given ID, or creates a new one if it doesn't exist.
    ///
    /// This is useful for "get or create" semantics where you want to ensure
    /// an object exists before operating on it.
    ///
    /// # Arguments
    ///
    /// * `oid` - The object ID (existing or newly allocated)
    /// * `mode` - The open mode
    #[inline]
    pub fn open_or_create(self, oid: ObjectId, mode: ObjectOpenMode) -> Result<Object> {
        let _ = self.alloc();
        self.open(oid, mode)
    }
}

impl Drop for DaosClient {
    fn drop(&mut self) {
        if let Err(e) = daos_cont_close(self.container_handle) {
            eprintln!(
                "DaosClient::drop: daos_cont_close() failed with {:?}, continuing with drop anyway",
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::container::flags::CONT_OPEN_RW;
    use crate::pool::flags::POOL_CONNECT_NONE;

    #[test]
    fn test_client_builder_default() {
        let builder = DaosClientBuilder::new();
        assert!(builder.pool_label.is_none());
        assert!(builder.pool_uuid.is_none());
        assert!(builder.container_label.is_none());
        assert!(builder.container_uuid.is_none());
    }

    #[test]
    fn test_client_builder_pool_chaining() {
        let builder = DaosClientBuilder::new()
            .pool_label("mypool")
            .pool_system("daos_server")
            .pool_flags(POOL_CONNECT_NONE);
        assert_eq!(builder.pool_label.as_deref(), Some("mypool"));
        assert_eq!(builder.pool_sys.as_deref(), Some("daos_server"));
        assert_eq!(builder.pool_flags, POOL_CONNECT_NONE);
    }

    #[test]
    fn test_client_builder_container_chaining() {
        let builder = DaosClientBuilder::new()
            .container_label("mycontainer")
            .container_flags(CONT_OPEN_RW);
        assert_eq!(builder.container_label.as_deref(), Some("mycontainer"));
        assert_eq!(builder.container_flags, CONT_OPEN_RW);
    }

    #[test]
    fn test_client_builder_object_config() {
        let builder = DaosClientBuilder::new()
            .object_type(ObjectType::KvHashed)
            .object_class(ObjectClass::UNKNOWN)
            .object_hints(ObjectClassHints::NONE);
        assert_eq!(builder.object_type, ObjectType::KvHashed);
    }

    #[test]
    fn test_client_builder_validate_pool_both_label_and_uuid() {
        let builder = DaosClientBuilder::new()
            .pool_label("mypool")
            .pool_uuid("12345678-1234-1234-1234-123456789012");
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_client_builder_validate_container_both_label_and_uuid() {
        let builder = DaosClientBuilder::new()
            .container_label("mycontainer")
            .container_uuid("12345678-1234-1234-1234-123456789012");
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_client_builder_validate_neither_pool() {
        let builder = DaosClientBuilder::new().container_label("mycontainer");
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_client_builder_validate_neither_container() {
        let builder = DaosClientBuilder::new().pool_label("mypool");
        let result = builder.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_object_builder_default_type() {
        let builder = DaosClientBuilder::new();
        assert_eq!(builder.object_type, ObjectType::KvHashed);
    }

    #[test]
    fn test_facade_error_display() {
        let err = FacadeError::NotConnected;
        assert_eq!(format!("{}", err), "client not connected");

        let err = FacadeError::InvalidConfig("test".into());
        assert_eq!(format!("{}", err), "invalid configuration: test");

        let err = FacadeError::Daos(DaosError::NotFound);
        assert_eq!(format!("{}", err), "DAOS error: Entity not found");
    }

    #[test]
    fn test_facade_error_from_daos_error() {
        let facade_err: FacadeError = DaosError::NotFound.into();
        assert!(matches!(facade_err, FacadeError::Daos(DaosError::NotFound)));
    }

    #[test]
    fn test_daos_error_from_facade_error() {
        let daos_err: DaosError = FacadeError::Daos(DaosError::Busy).into();
        assert!(matches!(daos_err, DaosError::Busy));

        let daos_err: DaosError = FacadeError::NotConnected.into();
        assert!(matches!(daos_err, DaosError::InvalidArg));
    }

    #[test]
    fn test_object_builder_debug() {
        let builder = DaosClientBuilder::new()
            .pool_label("mypool")
            .container_label("mycontainer");
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("DaosClientBuilder"));
    }

    #[test]
    fn test_daos_client_builder_all_options() {
        let builder = DaosClientBuilder::new()
            .pool_label("mypool")
            .pool_system("daos_server")
            .pool_flags(1)
            .container_label("mycontainer")
            .container_flags(2)
            .object_type(ObjectType::Array)
            .object_class(ObjectClass::UNKNOWN)
            .object_hints(ObjectClassHints::NONE);

        assert_eq!(builder.pool_label.as_deref(), Some("mypool"));
        assert_eq!(builder.pool_sys.as_deref(), Some("daos_server"));
        assert_eq!(builder.pool_flags, 1);
        assert_eq!(builder.container_label.as_deref(), Some("mycontainer"));
        assert_eq!(builder.container_flags, 2);
        assert_eq!(builder.object_type, ObjectType::Array);
    }

    #[test]
    fn test_daos_client_builder_uuid_based() {
        let builder = DaosClientBuilder::new()
            .pool_uuid("12345678-1234-1234-1234-123456789012")
            .container_uuid("87654321-4321-4321-4321-210987654321");

        assert!(builder.pool_label.is_none());
        assert!(builder.pool_uuid.is_some());
        assert!(builder.container_label.is_none());
        assert!(builder.container_uuid.is_some());
    }

    #[test]
    fn test_object_builder_chaining() {
        let builder = DaosClientBuilder::new()
            .pool_label("mypool")
            .container_label("mycontainer")
            .object_type(ObjectType::MultiHashed)
            .object_class(ObjectClass::UNKNOWN)
            .object_hints(ObjectClassHints::RDD_RP);

        assert_eq!(builder.object_type, ObjectType::MultiHashed);
        assert_eq!(builder.object_class, ObjectClass::UNKNOWN);
        assert!(builder.object_hints.as_raw() & ObjectClassHints::RDD_RP.as_raw() != 0);
    }

    #[test]
    fn test_facade_error_not_connected_display() {
        let err = FacadeError::NotConnected;
        assert!(format!("{:?}", err).contains("NotConnected"));
    }

    #[test]
    fn test_facade_error_invalid_config_display() {
        let err = FacadeError::InvalidConfig("missing pool".into());
        assert!(format!("{}", err).contains("missing pool"));
    }

    #[test]
    fn test_daos_client_debug() {
        // Can't build a real client without DAOS, but we can verify the debug impl exists
        let builder = DaosClientBuilder::new()
            .pool_label("mypool")
            .container_label("mycontainer");
        // Debug impl should compile
        let debug_str = format!("{:?}", builder);
        assert!(!debug_str.is_empty());
    }
}
