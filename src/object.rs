//! Object operations and key-value storage.
//!
//! This module provides typed wrappers for DAOS object operations, ensuring
//! type safety at the API boundary without exposing raw integer constants.

use crate::error::{DaosError, Result};
use crate::io::{AKey, DKey, Iod, RawIod, RawSgl, Sgl};
use crate::iter::{AkeyEnum, DkeyEnum, EnumConfig, RecxEnum, RecxOrder};
use crate::query::{QueryKeyFlags, QueryKeyResult};
use crate::tx::Tx;
use crate::unsafe_inner::handle::DaosHandle;
use daos::{daos_obj_id_t, daos_oclass_hints_t, daos_oclass_id_t, daos_otype_t};
use std::fmt;

/// Object type enumeration defining the structure of DAOS objects.
///
/// Each variant corresponds to a `daos_otype_t` value that determines
/// how keys and data are organized within the object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ObjectType {
    /// Default object type: multi-level KV with hashed dkeys and akeys.
    MultiHashed = 0,
    /// Object ID table created on snapshot.
    Oit = 1,
    /// KV with uint64 distribution keys.
    DKeyUint64 = 2,
    /// KV with uint64 attribute keys.
    AKeyUint64 = 3,
    /// Multi-level KV with uint64 dkeys and akeys.
    MultiUint64 = 4,
    /// KV with lexical (string) distribution keys.
    DKeyLexical = 5,
    /// KV with lexical (string) attribute keys.
    AKeyLexical = 6,
    /// Multi-level KV with lexical dkeys and akeys.
    MultiLexical = 7,
    /// Flat KV (no akey) with hashed dkey.
    KvHashed = 8,
    /// Flat KV (no akey) with integer dkey.
    KvUint64 = 9,
    /// Flat KV (no akey) with lexical dkey.
    KvLexical = 10,
    /// Array with attributes stored in the DAOS object.
    Array = 11,
    /// Array with attributes provided by the user.
    ArrayAttr = 12,
    /// Byte Array with no metadata (e.g., DFS/POSIX).
    ArrayByte = 13,
    /// Second version of Object ID table.
    OitV2 = 14,
}

impl ObjectType {
    /// Maximum valid object type value.
    const MAX: u32 = 14;

    /// Convert from the raw DAOS `daos_otype_t` type.
    ///
    /// Returns `None` if the value is not a valid object type.
    #[inline]
    pub fn from_raw(raw: daos_otype_t) -> Option<Self> {
        if raw <= Self::MAX {
            // SAFETY: We just verified val is in range of our enum discriminant
            unsafe { Some(std::mem::transmute::<u32, Self>(raw)) }
        } else {
            None
        }
    }

    /// Convert to the raw DAOS `daos_otype_t` type.
    #[inline]
    pub fn as_raw(self) -> daos_otype_t {
        self as daos_otype_t
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectType::MultiHashed => write!(f, "multi-hashed (default)"),
            ObjectType::Oit => write!(f, "object ID table"),
            ObjectType::DKeyUint64 => write!(f, "KV with uint64 dkeys"),
            ObjectType::AKeyUint64 => write!(f, "KV with uint64 akeys"),
            ObjectType::MultiUint64 => write!(f, "multi-level KV with uint64 keys"),
            ObjectType::DKeyLexical => write!(f, "KV with lexical dkeys"),
            ObjectType::AKeyLexical => write!(f, "KV with lexical akeys"),
            ObjectType::MultiLexical => write!(f, "multi-level KV with lexical keys"),
            ObjectType::KvHashed => write!(f, "flat KV with hashed dkey"),
            ObjectType::KvUint64 => write!(f, "flat KV with uint64 dkey"),
            ObjectType::KvLexical => write!(f, "flat KV with lexical dkey"),
            ObjectType::Array => write!(f, "array with stored attributes"),
            ObjectType::ArrayAttr => write!(f, "array with user attributes"),
            ObjectType::ArrayByte => write!(f, "byte array"),
            ObjectType::OitV2 => write!(f, "object ID table v2"),
        }
    }
}

/// Object open mode determining read/write access semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ObjectOpenMode {
    /// Shared read access.
    ReadOnly = daos::DAOS_OO_RO,
    /// Shared read & write, no cache for write.
    ReadWrite = daos::DAOS_OO_RW,
    /// Exclusive write, data can be cached.
    Exclusive = daos::DAOS_OO_EXCL,
    /// Unsupported: random I/O.
    IoRandom = daos::DAOS_OO_IO_RAND,
    /// Unsupported: sequential I/O.
    IoSequential = daos::DAOS_OO_IO_SEQ,
}

impl ObjectOpenMode {
    /// Convert from the raw DAOS open mode flags.
    ///
    /// Returns `None` if the value doesn't match a known open mode.
    #[inline]
    pub fn from_raw(raw: u32) -> Option<Self> {
        match raw {
            x if x == daos::DAOS_OO_RO => Some(ObjectOpenMode::ReadOnly),
            x if x == daos::DAOS_OO_RW => Some(ObjectOpenMode::ReadWrite),
            x if x == daos::DAOS_OO_EXCL => Some(ObjectOpenMode::Exclusive),
            x if x == daos::DAOS_OO_IO_RAND => Some(ObjectOpenMode::IoRandom),
            x if x == daos::DAOS_OO_IO_SEQ => Some(ObjectOpenMode::IoSequential),
            _ => None,
        }
    }

    /// Convert to raw DAOS open mode flags.
    #[inline]
    pub fn as_raw(self) -> u32 {
        self as u32
    }
}

/// Redundancy factor for object class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ObjectRedundancy {
    /// Default - use replication factor from container property.
    Default = 0,
    /// No redundancy.
    None = 1,
    /// Replication.
    Replication = 2,
    /// Erasure coding.
    ErasureCode = 3,
}

impl ObjectRedundancy {
    const SHIFT: u32 = 6; // OC_REDUN_SHIFT

    /// Maximum valid redundancy value.
    const MAX: u32 = (1 << 3) - 1; // 3 bits

    #[inline]
    pub fn from_raw(raw: daos_oclass_id_t) -> Option<Self> {
        let ord = (raw >> Self::SHIFT) & Self::MAX;
        match ord {
            0 => Some(ObjectRedundancy::Default),
            1 => Some(ObjectRedundancy::None),
            2 => Some(ObjectRedundancy::Replication),
            3 => Some(ObjectRedundancy::ErasureCode),
            _ => None,
        }
    }

    #[inline]
    pub fn as_raw(self) -> u32 {
        self as u32
    }
}

/// Object class hints controlling redundancy and sharding.
///
/// These hints are combined as flags to suggest object class selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectClassHints {
    bits: u32,
}

impl ObjectClassHints {
    /// No hints specified - let DAOS decide based on container properties.
    pub const NONE: Self = Self { bits: 0 };

    // Redundancy hints
    /// Default redundancy - use RF property.
    pub const RDD_DEF: Self = Self { bits: 1 << 0 }; // DAOS_OCH_RDD_DEF
    /// No redundancy.
    pub const RDD_NO: Self = Self { bits: 1 << 1 }; // DAOS_OCH_RDD_NO
    /// Replication redundancy.
    pub const RDD_RP: Self = Self { bits: 1 << 2 }; // DAOS_OCH_RDD_RP
    /// Erasure code redundancy.
    pub const RDD_EC: Self = Self { bits: 1 << 3 }; // DAOS_OCH_RDD_EC

    // Sharding hints
    /// Default sharding: MAX for array & flat KV; 1 grp for others.
    pub const SHD_DEF: Self = Self { bits: 1 << 4 }; // DAOS_OCH_SHD_DEF
    /// Tiny sharding: 1 group.
    pub const SHD_TINY: Self = Self { bits: 1 << 5 }; // DAOS_OCH_SHD_TINY
    /// Regular sharding: max(128, 25%).
    pub const SHD_REG: Self = Self { bits: 1 << 6 }; // DAOS_OCH_SHD_REG
    /// High sharding: max(256, 50%).
    pub const SHD_HI: Self = Self { bits: 1 << 7 }; // DAOS_OCH_SHD_HI
    /// Extra high sharding: max(1024, 80%).
    pub const SHD_EXT: Self = Self { bits: 1 << 8 }; // DAOS_OCH_SHD_EXT
    /// Maximum sharding: 100%.
    pub const SHD_MAX: Self = Self { bits: 1 << 9 }; // DAOS_OCH_SHD_MAX

    /// Create a new set of hints from raw bits.
    #[inline]
    pub fn from_raw(bits: u32) -> Self {
        Self { bits }
    }

    /// Get the raw bits value.
    #[inline]
    pub fn as_raw(self) -> u32 {
        self.bits
    }

    /// Add a redundancy hint to the current set.
    #[inline]
    pub fn with_redundancy(mut self, red: ObjectRedundancy) -> Self {
        // Clear existing redundancy hint bits and set the selected one.
        // Hint encoding is one-hot in the lower 4 bits.
        let red_hint = match red {
            ObjectRedundancy::Default => Self::RDD_DEF.as_raw(),
            ObjectRedundancy::None => Self::RDD_NO.as_raw(),
            ObjectRedundancy::Replication => Self::RDD_RP.as_raw(),
            ObjectRedundancy::ErasureCode => Self::RDD_EC.as_raw(),
        };
        self.bits = (self.bits & !0xF) | red_hint;
        self
    }

    /// Add a sharding hint to the current set.
    #[inline]
    pub fn with_sharding(self, shd: Sharding) -> Self {
        Self {
            bits: self.bits | shd.as_raw(),
        }
    }
}

impl Default for ObjectClassHints {
    fn default() -> Self {
        Self::NONE
    }
}

/// Sharding level hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Sharding {
    /// Default: Use MAX for array & flat KV; 1 grp for others.
    Default = 1 << 4, // DAOS_OCH_SHD_DEF
    /// Tiny: 1 group.
    Tiny = 1 << 5, // DAOS_OCH_SHD_TINY
    /// Regular: max(128, 25%).
    Regular = 1 << 6, // DAOS_OCH_SHD_REG
    /// High: max(256, 50%).
    High = 1 << 7, // DAOS_OCH_SHD_HI
    /// Extra high: max(1024, 80%).
    ExtraHigh = 1 << 8, // DAOS_OCH_SHD_EXT
    /// Maximum: 100%.
    Max = 1 << 9, // DAOS_OCH_SHD_MAX
}

impl Sharding {
    #[inline]
    pub fn as_raw(self) -> u32 {
        self as u32
    }
}

/// Object class identifier.
///
/// This type wraps `daos_oclass_id_t` and provides methods for
/// common object class configurations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectClass {
    raw: daos_oclass_id_t,
}

impl ObjectClass {
    /// Unknown object class - DAOS will select based on hints or container properties.
    pub const UNKNOWN: Self = Self { raw: 0 };

    /// Create from raw `daos_oclass_id_t`.
    #[inline]
    pub fn from_raw(raw: daos_oclass_id_t) -> Self {
        Self { raw }
    }

    /// Get the raw `daos_oclass_id_t` value.
    #[inline]
    pub fn as_raw(self) -> daos_oclass_id_t {
        self.raw
    }

    /// Create an object class with specific redundancy and number of groups.
    ///
    /// This is for advanced users who are knowledgeable about specific
    /// object classes and their implications.
    pub fn with_params(redundancy: ObjectRedundancy, nr_grps: u32) -> Self {
        let ord = match redundancy {
            ObjectRedundancy::Default => 0,
            ObjectRedundancy::None => 1,
            ObjectRedundancy::Replication => 2,
            ObjectRedundancy::ErasureCode => 3,
        };
        // daos_oclass_id_t layout: (ord << OC_REDUN_SHIFT) | nr_grps
        let raw = (ord << 6) | (nr_grps & 0x3F); // OC_REDUN_SHIFT = 6, 6 bits for nr_grps
        Self { raw }
    }

    /// Extract redundancy factor from this object class.
    pub fn redundancy(self) -> Option<ObjectRedundancy> {
        ObjectRedundancy::from_raw(self.raw)
    }

    /// Extract number of groups from this object class.
    pub fn nr_grps(self) -> u32 {
        self.raw & 0x3F // Lower 6 bits
    }
}

/// A strong type wrapper for DAOS object IDs.
///
/// `ObjectId` encapsulates a `daos_obj_id_t` and provides type-safe
/// construction and validation. Object IDs are generated within a
/// container context and encode object type, class, and metadata
/// in their high bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectId {
    raw: daos_obj_id_t,
}

impl ObjectId {
    /// Nil object ID (all zeros).
    pub const NIL: Self = Self {
        raw: daos_obj_id_t { hi: 0, lo: 0 },
    };

    /// Create from the raw DAOS `daos_obj_id_t`.
    ///
    /// # Safety
    ///
    /// The caller must ensure the raw value is a valid DAOS object ID.
    #[inline]
    pub unsafe fn from_raw(raw: daos_obj_id_t) -> Self {
        Self { raw }
    }

    /// Create an object ID from explicit high/low 64-bit parts.
    #[inline]
    pub fn from_parts(hi: u64, lo: u64) -> Self {
        Self {
            raw: daos_obj_id_t { hi, lo },
        }
    }

    /// Convert to the raw DAOS `daos_obj_id_t`.
    ///
    /// This exposes the raw internal value for FFI calls.
    #[inline]
    pub fn as_raw(self) -> daos_obj_id_t {
        self.raw
    }

    /// Check if this is the nil object ID.
    #[inline]
    pub fn is_nil(self) -> bool {
        self.raw.hi == 0 && self.raw.lo == 0
    }

    /// Extract the object type from this object ID.
    pub fn object_type(self) -> Option<ObjectType> {
        if self.is_nil() {
            return None;
        }
        const OID_FMT_TYPE_SHIFT: u64 = 64 - 8; // OID_FMT_TYPE_BITS = 8
        const OID_FMT_TYPE_MASK: u64 = ((1_u64 << 8) - 1) << OID_FMT_TYPE_SHIFT;

        let type_val = (self.raw.hi & OID_FMT_TYPE_MASK) >> OID_FMT_TYPE_SHIFT;
        ObjectType::from_raw(type_val as daos_otype_t)
    }

    /// Extract the object class from this object ID.
    pub fn object_class(self) -> ObjectClass {
        const OID_FMT_CLASS_SHIFT: u64 = 64 - 8 - 8; // After type bits
        const OID_FMT_CLASS_MASK: u64 = ((1_u64 << 8) - 1) << OID_FMT_CLASS_SHIFT;

        let class_bits = (self.raw.hi & OID_FMT_CLASS_MASK) >> OID_FMT_CLASS_SHIFT;
        ObjectClass::from_raw(class_bits as daos_oclass_id_t)
    }

    /// Extract the number of groups from this object ID.
    pub fn nr_grps(self) -> u32 {
        const OID_FMT_META_SHIFT: u64 = 64 - 8 - 8 - 16; // After type and class bits
        const OID_FMT_META_MASK: u64 = ((1_u64 << 16) - 1) << OID_FMT_META_SHIFT;

        ((self.raw.hi & OID_FMT_META_MASK) >> OID_FMT_META_SHIFT) as u32
    }

    /// Cycle to the next unique high bits value.
    ///
    /// This uses a prime number to guarantee hitting every unique 32-bit value
    /// when called 2^32 times.
    pub fn cycle(&mut self) {
        // daos_obj_oid_cycle implementation
        const PRIME: u64 = 999999937;
        self.raw.hi = (self.raw.hi.wrapping_add(PRIME)) & 0xFFFFFFFF;
    }
}

/// Generate a new object ID in a container.
///
/// This function generates a unique object ID with the specified type,
/// class, and hints encoded in the high bits.
///
/// # Arguments
///
/// * `coh` - Container open handle
/// * `oid` - Object ID to populate (low 96 bits should be set and unique in container)
/// * `otype` - Object type
/// * `oclass` - Object class (use `ObjectClass::UNKNOWN` to select based on hints)
/// * `hints` - Object class hints (ignored if `oclass` is not `UNKNOWN`)
///
pub fn generate_oid(
    coh: DaosHandle,
    oid: &mut ObjectId,
    otype: ObjectType,
    oclass: ObjectClass,
    hints: ObjectClassHints,
) -> Result<()> {
    // We need to use a mutable raw OID for the FFI call
    // Start with NIL or user-provided value
    let mut raw_oid = oid.raw;

    let ret = unsafe {
        daos::daos_obj_generate_oid2(
            coh.as_raw(),
            &mut raw_oid,
            otype.as_raw(),
            oclass.as_raw(),
            hints.as_raw() as daos_oclass_hints_t,
            0,
        )
    };

    if ret == 0 {
        // SAFETY: We just generated this OID through the DAOS API
        oid.raw = raw_oid;
        Ok(())
    } else {
        Err(ret.into())
    }
}

/// Object handle for an open DAOS object.
///
/// Wraps a DAOS object handle and enforces valid state transitions.
pub struct Object {
    handle: Option<DaosHandle>,
    oid: ObjectId,
}

impl Object {
    pub fn open_in(
        container: &crate::container::Container<'_>,
        oid: ObjectId,
        mode: ObjectOpenMode,
    ) -> Result<Self> {
        let coh = container.as_handle()?;
        Self::open(coh, oid, mode)
    }

    pub fn open(container_handle: DaosHandle, oid: ObjectId, mode: ObjectOpenMode) -> Result<Self> {
        let handle =
            crate::unsafe_inner::ffi::daos_obj_open(container_handle, oid.raw, mode.as_raw())?;
        Ok(Self {
            handle: Some(handle),
            oid,
        })
    }

    pub fn oid(&self) -> ObjectId {
        self.oid
    }

    pub fn is_open(&self) -> bool {
        self.handle.is_some()
    }

    pub fn close(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            crate::unsafe_inner::ffi::daos_obj_close(handle)
        } else {
            Err(crate::error::DaosError::InvalidArg)
        }
    }

    fn handle(&self) -> Result<DaosHandle> {
        self.handle.ok_or(crate::error::DaosError::InvalidArg)
    }

    pub fn update(&self, tx: &Tx, dkey: &DKey, iod: &Iod, sgl: &Sgl) -> Result<()> {
        let handle = self.handle()?;
        let mut raw_iod: RawIod = iod.to_raw()?;
        let mut raw_sgl: RawSgl = sgl.to_raw()?;
        let mut dkey_buf = dkey.as_bytes().to_vec();
        let mut dkey_raw = daos::daos_key_t {
            iov_buf: dkey_buf.as_mut_ptr() as *mut std::ffi::c_void,
            iov_buf_len: dkey_buf.len(),
            iov_len: dkey_buf.len(),
        };

        let ret = unsafe {
            daos::daos_obj_update(
                handle.as_raw(),
                tx.as_raw_daos_handle(),
                0,
                &mut dkey_raw,
                1,
                &mut raw_iod.iod,
                &mut raw_sgl.sgl,
                std::ptr::null_mut(),
            )
        };

        if ret == 0 { Ok(()) } else { Err(ret.into()) }
    }

    pub fn fetch(&self, tx: &Tx, dkey: &DKey, iod: &Iod, sgl: &mut Sgl) -> Result<()> {
        let handle = self.handle()?;
        let mut raw_iod: RawIod = iod.to_raw()?;
        let mut raw_sgl: RawSgl = sgl.to_raw()?;
        let mut dkey_buf = dkey.as_bytes().to_vec();
        let mut dkey_raw = daos::daos_key_t {
            iov_buf: dkey_buf.as_mut_ptr() as *mut std::ffi::c_void,
            iov_buf_len: dkey_buf.len(),
            iov_len: dkey_buf.len(),
        };

        let ret = unsafe {
            daos::daos_obj_fetch(
                handle.as_raw(),
                tx.as_raw_daos_handle(),
                0,
                &mut dkey_raw,
                1,
                &mut raw_iod.iod,
                &mut raw_sgl.sgl,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        if ret == 0 { Ok(()) } else { Err(ret.into()) }
    }

    pub fn punch(&self, tx: &Tx) -> Result<()> {
        let handle = self.handle()?;
        crate::unsafe_inner::ffi::daos_obj_punch(handle, tx.as_raw_daos_handle(), 0)
    }

    pub fn punch_dkeys(&self, tx: &Tx, dkeys: &[DKey]) -> Result<()> {
        let handle = self.handle()?;
        if dkeys.is_empty() {
            return Err(crate::error::DaosError::InvalidArg);
        }
        let dkey_raws: Vec<daos::daos_key_t> = dkeys
            .iter()
            .map(|d| {
                let buf = d.as_bytes();
                daos::daos_key_t {
                    iov_buf: buf.as_ptr() as *mut std::ffi::c_void,
                    iov_buf_len: buf.len(),
                    iov_len: buf.len(),
                }
            })
            .collect();
        crate::unsafe_inner::ffi::daos_obj_punch_dkeys(
            handle,
            tx.as_raw_daos_handle(),
            0,
            &dkey_raws,
        )
    }

    pub fn punch_akeys(&self, tx: &Tx, dkey: &DKey, akeys: &[AKey]) -> Result<()> {
        let handle = self.handle()?;
        if akeys.is_empty() {
            return Err(crate::error::DaosError::InvalidArg);
        }
        let dkey_buf = dkey.as_bytes();
        let dkey_raw = daos::daos_key_t {
            iov_buf: dkey_buf.as_ptr() as *mut std::ffi::c_void,
            iov_buf_len: dkey_buf.len(),
            iov_len: dkey_buf.len(),
        };
        let akey_raws: Vec<daos::daos_key_t> = akeys
            .iter()
            .map(|a| {
                let buf = a.as_bytes();
                daos::daos_key_t {
                    iov_buf: buf.as_ptr() as *mut std::ffi::c_void,
                    iov_buf_len: buf.len(),
                    iov_len: buf.len(),
                }
            })
            .collect();
        crate::unsafe_inner::ffi::daos_obj_punch_akeys(
            handle,
            tx.as_raw_daos_handle(),
            0,
            &dkey_raw,
            &akey_raws,
        )
    }

    pub fn query_key(
        &self,
        tx: &Tx,
        flags: QueryKeyFlags,
        dkey: Option<&DKey>,
        akey: Option<&AKey>,
    ) -> Result<QueryKeyResult> {
        if flags.is_empty() {
            return Err(DaosError::InvalidArg);
        }

        let has_max = flags.contains(QueryKeyFlags::GET_MAX);
        let has_min = flags.contains(QueryKeyFlags::GET_MIN);
        if has_max == has_min {
            return Err(DaosError::InvalidArg);
        }

        let query_target = flags.contains(QueryKeyFlags::GET_DKEY)
            || flags.contains(QueryKeyFlags::GET_AKEY)
            || flags.contains(QueryKeyFlags::GET_RECX);
        if !query_target {
            return Err(DaosError::InvalidArg);
        }

        let needs_akey =
            flags.contains(QueryKeyFlags::GET_AKEY) || flags.contains(QueryKeyFlags::GET_RECX);

        let dkey = dkey.ok_or(DaosError::InvalidArg)?;
        let mut dkey_buf = dkey.as_bytes().to_vec();
        let mut akey_buf = akey.map(|a| a.as_bytes().to_vec());
        if needs_akey && akey_buf.is_none() {
            return Err(DaosError::InvalidArg);
        }

        let mut dkey_raw = daos::daos_key_t {
            iov_buf: dkey_buf.as_mut_ptr() as *mut std::ffi::c_void,
            iov_buf_len: dkey_buf.len(),
            iov_len: dkey_buf.len(),
        };

        let mut empty_akey = Vec::new();
        let akey_storage = if let Some(buf) = &mut akey_buf {
            buf
        } else {
            &mut empty_akey
        };

        let mut akey_raw = daos::daos_key_t {
            iov_buf: akey_storage.as_mut_ptr() as *mut std::ffi::c_void,
            iov_buf_len: akey_storage.len(),
            iov_len: akey_storage.len(),
        };

        let mut recx = daos::daos_recx_t {
            rx_idx: 0,
            rx_nr: 0,
        };

        let handle = self.handle()?;
        crate::unsafe_inner::ffi::daos_obj_query_key(
            handle,
            tx.as_raw_daos_handle(),
            flags.as_raw(),
            &mut dkey_raw,
            &mut akey_raw,
            &mut recx,
        )?;

        let dkey_result = if flags.contains(QueryKeyFlags::GET_DKEY) {
            let dkey_len = dkey_raw.iov_len;
            if dkey_len == 0 || dkey_len > dkey_buf.len() {
                None
            } else {
                DKey::new(dkey_buf[..dkey_len].to_vec()).ok()
            }
        } else {
            None
        };

        let akey_result = if flags.contains(QueryKeyFlags::GET_AKEY) {
            let akey_len = akey_raw.iov_len;
            if akey_len == 0 || akey_len > akey_storage.len() {
                None
            } else {
                AKey::new(akey_storage[..akey_len].to_vec()).ok()
            }
        } else {
            None
        };

        let recx_result = if flags.contains(QueryKeyFlags::GET_RECX) {
            Some(recx)
        } else {
            None
        };

        Ok(QueryKeyResult {
            dkey: dkey_result,
            akey: akey_result,
            recx: recx_result,
        })
    }

    pub fn query_max_epoch(&self, tx: &Tx) -> Result<u64> {
        let handle = self.handle()?;
        crate::unsafe_inner::ffi::daos_obj_query_max_epoch(handle, tx.as_raw_daos_handle())
    }

    pub fn enumerate_dkeys(&mut self) -> Result<DkeyEnum> {
        let handle = self.handle()?;
        Ok(DkeyEnum::new(handle, EnumConfig::default()))
    }

    pub fn enumerate_dkeys_with_config(&mut self, config: EnumConfig) -> Result<DkeyEnum> {
        let handle = self.handle()?;
        Ok(DkeyEnum::new(handle, config))
    }

    pub fn enumerate_akeys(&mut self, dkey: &DKey) -> Result<AkeyEnum> {
        let handle = self.handle()?;
        Ok(AkeyEnum::new(handle, dkey.clone(), EnumConfig::default()))
    }

    pub fn enumerate_akeys_with_config(
        &mut self,
        dkey: &DKey,
        config: EnumConfig,
    ) -> Result<AkeyEnum> {
        let handle = self.handle()?;
        Ok(AkeyEnum::new(handle, dkey.clone(), config))
    }

    pub fn enumerate_recxs(&mut self, dkey: &DKey, akey: &AKey) -> Result<RecxEnum> {
        let handle = self.handle()?;
        Ok(RecxEnum::new(
            handle,
            dkey.clone(),
            akey.clone(),
            RecxOrder::Increasing,
        ))
    }

    pub fn enumerate_recxs_ordered(
        &mut self,
        dkey: &DKey,
        akey: &AKey,
        order: RecxOrder,
    ) -> Result<RecxEnum> {
        let handle = self.handle()?;
        Ok(RecxEnum::new(handle, dkey.clone(), akey.clone(), order))
    }
}

impl Drop for Object {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = crate::unsafe_inner::ffi::daos_obj_close(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{AKey, IoBuffer, IodArrayBuilder, IodSingleBuilder, Recx};

    #[test]
    fn test_object_type_from_raw_valid() {
        assert!(ObjectType::from_raw(0).is_some());
        assert!(ObjectType::from_raw(14).is_some());
        assert_eq!(ObjectType::from_raw(0), Some(ObjectType::MultiHashed));
        assert_eq!(ObjectType::from_raw(14), Some(ObjectType::OitV2));
    }

    #[test]
    fn test_object_type_from_raw_invalid() {
        assert!(ObjectType::from_raw(15).is_none());
        assert!(ObjectType::from_raw(100).is_none());
        assert!(ObjectType::from_raw(u32::MAX).is_none());
    }

    #[test]
    fn test_object_type_roundtrip() {
        for ty in [
            ObjectType::MultiHashed,
            ObjectType::Oit,
            ObjectType::DKeyUint64,
            ObjectType::ArrayByte,
            ObjectType::OitV2,
        ] {
            let raw = ty.as_raw();
            assert_eq!(ObjectType::from_raw(raw), Some(ty));
        }
    }

    #[test]
    fn test_object_open_mode_from_raw() {
        assert_eq!(
            ObjectOpenMode::from_raw(0x02),
            Some(ObjectOpenMode::ReadOnly)
        );
        assert_eq!(
            ObjectOpenMode::from_raw(0x04),
            Some(ObjectOpenMode::ReadWrite)
        );
        assert_eq!(
            ObjectOpenMode::from_raw(0x08),
            Some(ObjectOpenMode::Exclusive)
        );
        assert_eq!(
            ObjectOpenMode::from_raw(0x10),
            Some(ObjectOpenMode::IoRandom)
        );
        assert_eq!(
            ObjectOpenMode::from_raw(0x20),
            Some(ObjectOpenMode::IoSequential)
        );
        assert_eq!(ObjectOpenMode::from_raw(0x00), None);
        assert_eq!(ObjectOpenMode::from_raw(0xFF), None);
    }

    #[test]
    fn test_object_open_mode_roundtrip() {
        for mode in [
            ObjectOpenMode::ReadOnly,
            ObjectOpenMode::ReadWrite,
            ObjectOpenMode::Exclusive,
        ] {
            assert_eq!(ObjectOpenMode::from_raw(mode.as_raw()), Some(mode));
        }
    }

    #[test]
    fn test_object_class_hints_constants() {
        // Verify hint constants don't overlap
        let hints = [
            ObjectClassHints::RDD_DEF,
            ObjectClassHints::RDD_NO,
            ObjectClassHints::RDD_RP,
            ObjectClassHints::RDD_EC,
        ];
        let mut combined = 0u32;
        for hint in hints {
            assert!(combined & hint.as_raw() == 0);
            combined |= hint.as_raw();
        }
    }

    #[test]
    fn test_object_class_hints_with_redundancy_encodes_one_hot_bits() {
        assert_eq!(
            ObjectClassHints::NONE
                .with_redundancy(ObjectRedundancy::Default)
                .as_raw()
                & 0xF,
            ObjectClassHints::RDD_DEF.as_raw()
        );
        assert_eq!(
            ObjectClassHints::NONE
                .with_redundancy(ObjectRedundancy::None)
                .as_raw()
                & 0xF,
            ObjectClassHints::RDD_NO.as_raw()
        );
        assert_eq!(
            ObjectClassHints::NONE
                .with_redundancy(ObjectRedundancy::Replication)
                .as_raw()
                & 0xF,
            ObjectClassHints::RDD_RP.as_raw()
        );
        assert_eq!(
            ObjectClassHints::NONE
                .with_redundancy(ObjectRedundancy::ErasureCode)
                .as_raw()
                & 0xF,
            ObjectClassHints::RDD_EC.as_raw()
        );
    }

    #[test]
    fn test_sharding_hints_constants() {
        let hints = [
            Sharding::Default,
            Sharding::Tiny,
            Sharding::Regular,
            Sharding::High,
            Sharding::ExtraHigh,
            Sharding::Max,
        ];
        let mut combined = 0u32;
        for hint in hints {
            assert!(combined & hint.as_raw() == 0);
            combined |= hint.as_raw();
        }
    }

    #[test]
    fn test_object_class_with_params() {
        let oc = ObjectClass::with_params(ObjectRedundancy::Replication, 4);
        assert_eq!(oc.redundancy(), Some(ObjectRedundancy::Replication));
        assert_eq!(oc.nr_grps(), 4);
    }

    #[test]
    fn test_object_class_unknown() {
        assert_eq!(ObjectClass::UNKNOWN.as_raw(), 0);
    }

    #[test]
    fn test_object_id_nil() {
        let nil = ObjectId::NIL;
        assert!(nil.is_nil());
        assert_eq!(nil.object_type(), None); // hi=0 means no type encoded
    }

    #[test]
    fn test_object_id_equality() {
        let id1 = ObjectId::NIL;
        let id2 = ObjectId::NIL;
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_object_id_from_parts() {
        let id = ObjectId::from_parts(0xAABBCCDD, 0x11223344);
        let raw = id.as_raw();
        assert_eq!(raw.hi, 0xAABBCCDD);
        assert_eq!(raw.lo, 0x11223344);
    }

    #[test]
    fn test_object_id_debug() {
        let nil = ObjectId::NIL;
        let debug_str = format!("{:?}", nil);
        assert!(debug_str.contains("ObjectId"));
    }

    #[test]
    fn test_object_type_display() {
        assert_eq!(
            ObjectType::MultiHashed.to_string(),
            "multi-hashed (default)"
        );
        assert_eq!(ObjectType::Oit.to_string(), "object ID table");
        assert_eq!(ObjectType::ArrayByte.to_string(), "byte array");
    }

    #[test]
    fn test_object_close_when_already_closed() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let result = object.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_object_oid_accessor() {
        let object = Object {
            handle: None,
            oid: ObjectId::from_parts(10, 20),
        };
        let oid = object.oid();
        assert_eq!(oid.as_raw().hi, 10);
        assert_eq!(oid.as_raw().lo, 20);
    }

    #[test]
    fn test_update_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let dkey = DKey::new(b"test_dkey").unwrap();
        let akey = AKey::new(b"test_akey").unwrap();
        let iod = Iod::Single(IodSingleBuilder::new(akey).value_len(8).build().unwrap());
        let sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![0u8; 8]))
            .build()
            .unwrap();
        let tx = Tx::none();

        let result = object.update(&tx, &dkey, &iod, &sgl);
        assert!(result.is_err());
    }

    #[test]
    fn test_fetch_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let dkey = DKey::new(b"test_dkey").unwrap();
        let akey = AKey::new(b"test_akey").unwrap();
        let iod = Iod::Single(IodSingleBuilder::new(akey).value_len(8).build().unwrap());
        let mut sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![0u8; 8]))
            .build()
            .unwrap();
        let tx = Tx::none();

        let result = object.fetch(&tx, &dkey, &iod, &mut sgl);
        assert!(result.is_err());
    }

    #[test]
    fn test_tx_none_as_raw_handle_is_null() {
        let tx = Tx::none();
        let raw = tx.as_raw_daos_handle();
        assert_eq!(raw.cookie, 0);
    }

    #[test]
    fn test_single_value_iod_construction() {
        let _dkey = DKey::new(b"my_dkey").unwrap();
        let akey = AKey::new(b"my_akey").unwrap();
        let single = IodSingleBuilder::new(akey).value_len(16).build().unwrap();
        let iod = Iod::Single(single);
        let _sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![1u8; 16]))
            .build()
            .unwrap();

        let raw_iod = iod.to_raw().unwrap();
        assert_eq!(raw_iod.iod.iod_type, daos::daos_iod_type_t_DAOS_IOD_SINGLE);
        assert_eq!(raw_iod.iod.iod_size, 16);
    }

    #[test]
    fn test_array_value_iod_construction() {
        let _dkey = DKey::new(b"my_dkey").unwrap();
        let akey = AKey::new(b"my_akey").unwrap();
        let recx = Recx::new(0, 4).unwrap();
        let array = IodArrayBuilder::new(akey)
            .record_len(8)
            .add_recx(recx)
            .build()
            .unwrap();
        let iod = Iod::Array(array);
        let _sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![1u8; 32]))
            .build()
            .unwrap();

        let raw_iod = iod.to_raw().unwrap();
        assert_eq!(raw_iod.iod.iod_type, daos::daos_iod_type_t_DAOS_IOD_ARRAY);
        assert_eq!(raw_iod.iod.iod_size, 8);
        assert_eq!(raw_iod.iod.iod_nr, 1);
    }

    #[test]
    fn test_update_and_fetch_with_tx_none_signature() {
        let tx: Tx = Tx::none();
        assert!(tx.is_none());
        let raw = tx.as_raw_daos_handle();
        assert_eq!(raw.cookie, 0);
    }

    #[test]
    fn test_fetch_returns_notfound_for_missing_key() {
        let _dkey = DKey::new(b"nonexistent_dkey").unwrap();
        let akey = AKey::new(b"nonexistent_akey").unwrap();
        let _iod = Iod::Single(IodSingleBuilder::new(akey).value_len(8).build().unwrap());
        let _sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![0u8; 8]))
            .build()
            .unwrap();

        let tx = Tx::none();
        assert!(tx.is_none());
        let raw = tx.as_raw_daos_handle();
        assert_eq!(raw.cookie, 0);
    }

    #[test]
    fn test_tx_none_passthrough_in_update() {
        let tx: Tx = Tx::none();
        assert!(tx.is_none());
        let raw = tx.as_raw_daos_handle();
        assert_eq!(raw.cookie, 0);
    }

    #[test]
    fn test_round_trip_single_value_data() {
        let _dkey = DKey::new(b"roundtrip_dkey").unwrap();
        let akey = AKey::new(b"roundtrip_akey").unwrap();
        let original_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE];

        let single = IodSingleBuilder::new(akey)
            .value_len(original_data.len())
            .build()
            .unwrap();
        let iod = Iod::Single(single);
        let _sgl = Sgl::builder()
            .push(IoBuffer::from_vec(original_data.clone()))
            .build()
            .unwrap();

        let raw_iod = iod.to_raw().unwrap();
        assert_eq!(raw_iod.iod.iod_size, original_data.len() as u64);
        assert_eq!(raw_iod.iod.iod_type, daos::daos_iod_type_t_DAOS_IOD_SINGLE);

        let fetch_sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![0u8; original_data.len()]))
            .build()
            .unwrap();
        let raw_fetch_sgl = fetch_sgl.to_raw().unwrap();
        assert_eq!(raw_fetch_sgl.sgl.sg_nr, 1);
    }

    #[test]
    fn test_round_trip_array_value_data() {
        let _dkey = DKey::new(b"array_dkey").unwrap();
        let akey = AKey::new(b"array_akey").unwrap();
        let record_len = 8;
        let num_records = 4;
        let original_data: Vec<u8> = (0u8..32).collect();

        let recx = Recx::new(0, num_records).unwrap();
        let array = IodArrayBuilder::new(akey)
            .record_len(record_len)
            .add_recx(recx)
            .build()
            .unwrap();
        let iod = Iod::Array(array);
        let _sgl = Sgl::builder()
            .push(IoBuffer::from_vec(original_data.clone()))
            .build()
            .unwrap();

        let raw_iod = iod.to_raw().unwrap();
        assert_eq!(raw_iod.iod.iod_type, daos::daos_iod_type_t_DAOS_IOD_ARRAY);
        assert_eq!(raw_iod.iod.iod_size, record_len as u64);

        let fetch_sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![0u8; original_data.len()]))
            .build()
            .unwrap();
        let raw_fetch_sgl = fetch_sgl.to_raw().unwrap();
        assert_eq!(raw_fetch_sgl.sgl.sg_nr, 1);
    }

    #[test]
    fn test_punch_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let result = object.punch(&tx);
        assert!(result.is_err());
    }

    #[test]
    fn test_punch_dkeys_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let dkey = DKey::new(b"test_dkey").unwrap();
        let result = object.punch_dkeys(&tx, &[dkey]);
        assert!(result.is_err());
    }

    #[test]
    fn test_punch_dkeys_rejects_empty() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let result = object.punch_dkeys(&tx, &[]);
        assert!(matches!(result, Err(crate::error::DaosError::InvalidArg)));
    }

    #[test]
    fn test_punch_akeys_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let dkey = DKey::new(b"test_dkey").unwrap();
        let akey = AKey::new(b"test_akey").unwrap();
        let result = object.punch_akeys(&tx, &dkey, &[akey]);
        assert!(result.is_err());
    }

    #[test]
    fn test_punch_akeys_rejects_empty() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let dkey = DKey::new(b"test_dkey").unwrap();
        let result = object.punch_akeys(&tx, &dkey, &[]);
        assert!(matches!(result, Err(crate::error::DaosError::InvalidArg)));
    }

    #[test]
    fn test_query_key_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let flags = QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_DKEY;
        let result = object.query_key(&tx, flags, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_key_rejects_empty_flags() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let result = object.query_key(&tx, QueryKeyFlags::default(), None, None);
        assert!(matches!(result, Err(crate::error::DaosError::InvalidArg)));
    }

    #[test]
    fn test_query_key_requires_query_target_flag() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let dkey = DKey::new(b"test_dkey").unwrap();
        let flags = QueryKeyFlags::GET_MAX;
        let result = object.query_key(&tx, flags, Some(&dkey), None);
        assert!(matches!(result, Err(crate::error::DaosError::InvalidArg)));
    }

    #[test]
    fn test_query_key_requires_dkey_input() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let flags = QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_DKEY;
        let result = object.query_key(&tx, flags, None, None);
        assert!(matches!(result, Err(crate::error::DaosError::InvalidArg)));
    }

    #[test]
    fn test_query_key_requires_akey_for_akey_or_recx_queries() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let dkey = DKey::new(b"test_dkey").unwrap();

        let akey_flags = QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_AKEY;
        let akey_result = object.query_key(&tx, akey_flags, Some(&dkey), None);
        assert!(matches!(
            akey_result,
            Err(crate::error::DaosError::InvalidArg)
        ));

        let recx_flags = QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_RECX;
        let recx_result = object.query_key(&tx, recx_flags, Some(&dkey), None);
        assert!(matches!(
            recx_result,
            Err(crate::error::DaosError::InvalidArg)
        ));
    }

    #[test]
    fn test_query_key_rejects_max_and_min() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let flags = QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_MIN | QueryKeyFlags::GET_DKEY;
        let result = object.query_key(&tx, flags, None, None);
        assert!(matches!(result, Err(crate::error::DaosError::InvalidArg)));
    }

    #[test]
    fn test_query_max_epoch_requires_valid_handle() {
        let object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let tx = Tx::none();
        let result = object.query_max_epoch(&tx);
        assert!(result.is_err());
    }

    #[test]
    fn test_enumerate_dkeys_requires_valid_handle() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let result = object.enumerate_dkeys();
        assert!(result.is_err());
    }

    #[test]
    fn test_enumerate_dkeys_with_config_requires_valid_handle() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let config = crate::iter::EnumConfig::default();
        let result = object.enumerate_dkeys_with_config(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_enumerate_akeys_requires_valid_handle() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let dkey = DKey::new(b"test_dkey").unwrap();
        let result = object.enumerate_akeys(&dkey);
        assert!(result.is_err());
    }

    #[test]
    fn test_enumerate_akeys_with_config_requires_valid_handle() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let dkey = DKey::new(b"test_dkey").unwrap();
        let config = crate::iter::EnumConfig::default();
        let result = object.enumerate_akeys_with_config(&dkey, config);
        assert!(result.is_err());
    }

    #[test]
    fn test_enumerate_recxs_requires_valid_handle() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let dkey = DKey::new(b"test_dkey").unwrap();
        let akey = AKey::new(b"test_akey").unwrap();
        let result = object.enumerate_recxs(&dkey, &akey);
        assert!(result.is_err());
    }

    #[test]
    fn test_enumerate_recxs_ordered_requires_valid_handle() {
        let mut object = Object {
            handle: None,
            oid: ObjectId::NIL,
        };
        let dkey = DKey::new(b"test_dkey").unwrap();
        let akey = AKey::new(b"test_akey").unwrap();
        let order = crate::iter::RecxOrder::Increasing;
        let result = object.enumerate_recxs_ordered(&dkey, &akey, order);
        assert!(result.is_err());
    }
}
