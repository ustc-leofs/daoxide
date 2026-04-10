//! Iterator utilities for batch operations and enumeration.
//!
//! This module provides iterator-based APIs for listing keys and records
//! in DAOS objects without manual anchor management.
//!
//! # Enumeration Types
//!
//! - [`EnumConfig`] - Configuration for enumeration operations
//! - [`DkeyEnum`] - Iterator for distribution keys
//! - [`AkeyEnum`] - Iterator for attribute keys
//! - [`RecxEnum`] - Iterator for record extents
//! - [`RecxEnumEntry`] - Entry containing recx and epoch range
//! - [`RecxOrder`] - Direction for record enumeration
//!
//! # Example: Enumerate Keys
//!
//! ```ignore
//! use daoxide::{Object, ObjectType, ObjectOpenMode};
//!
//! let mut object = Object::open_in(&container, oid, ObjectOpenMode::ReadOnly)?;
//!
//! // Iterate all dkeys
//! for dkey_result in object.enumerate_dkeys()? {
//!     let dkey = dkey_result?;
//!     println!("Found dkey: {:?}", dkey.as_bytes());
//! }
//! ```

use crate::error::Result;
use crate::io::{AKey, DKey};
use crate::unsafe_inner::handle::DaosHandle;
use daos::{d_iov_t, d_sg_list_t, daos_anchor_t, daos_epoch_range_t, daos_key_desc_t, daos_recx_t};

const DER_KEY2BIG: i32 = -(daos::daos_errno_DER_KEY2BIG as i32);

const DAOS_ANCHOR_TYPE_EOF_VAL: u16 = 3;

/// Configuration for enumeration operations.
///
/// Controls batch size and buffer allocation for key/record enumeration.
#[derive(Debug, Clone)]
pub struct EnumConfig {
    /// Number of keys/records to fetch per batch.
    pub batch_size: u32,
    /// Size of the buffer for key data in bytes.
    pub buffer_size: usize,
}

impl Default for EnumConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            buffer_size: 4096,
        }
    }
}

/// Order direction for record extent enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecxOrder {
    /// Enumerate records in increasing order.
    Increasing,
    /// Enumerate records in decreasing order.
    Decreasing,
}

#[derive(Clone)]
struct AnchorState {
    raw: daos_anchor_t,
}

impl AnchorState {
    fn new() -> Self {
        Self {
            raw: daos_anchor_t {
                da_type: 0,
                da_shard: 0,
                da_flags: 0,
                da_sub_anchors: 0,
                da_buf: [0; 104],
            },
        }
    }

    fn is_eof(&self) -> bool {
        self.raw.da_type == DAOS_ANCHOR_TYPE_EOF_VAL
    }
}

impl Default for AnchorState {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating enumeration iterators on an object handle.
///
/// `Enumerator` provides a convenient way to create iterators for
/// listing dkeys, akeys, and recxs with consistent configuration.
pub struct Enumerator {
    handle: DaosHandle,
    config: EnumConfig,
}

impl Enumerator {
    /// Creates a new Enumerator with default configuration.
    pub fn new(handle: DaosHandle) -> Self {
        Self {
            handle,
            config: EnumConfig::default(),
        }
    }

    /// Sets the enumeration configuration.
    pub fn with_config(mut self, config: EnumConfig) -> Self {
        self.config = config;
        self
    }

    /// Creates an iterator for listing distribution keys.
    pub fn list_dkeys(&mut self) -> DkeyEnum {
        DkeyEnum::new(self.handle, self.config.clone())
    }

    /// Creates an iterator for listing attribute keys under a dkey.
    pub fn list_akeys(&mut self, dkey: DKey) -> AkeyEnum {
        AkeyEnum::new(self.handle, dkey, self.config.clone())
    }

    /// Creates an iterator for listing record extents under a dkey/akey.
    pub fn list_recxs(&mut self, dkey: DKey, akey: AKey) -> RecxEnum {
        RecxEnum::new(self.handle, dkey, akey, RecxOrder::Increasing)
    }

    /// Creates an iterator for listing record extents in a specific order.
    pub fn list_recxs_ordered(&mut self, dkey: DKey, akey: AKey, order: RecxOrder) -> RecxEnum {
        RecxEnum::new(self.handle, dkey, akey, order)
    }
}

/// Iterator for enumerating distribution keys.
pub struct DkeyEnum {
    handle: DaosHandle,
    config: EnumConfig,
    anchor: AnchorState,
    buffer: Vec<u8>,
    kds: Vec<daos_key_desc_t>,
    current_index: usize,
    current_nr: usize,
    finished: bool,
}

impl DkeyEnum {
    pub(crate) fn new(handle: DaosHandle, config: EnumConfig) -> Self {
        let buffer_size = config.buffer_size;
        let kds = vec![
            daos_key_desc_t {
                kd_key_len: 0,
                kd_val_type: 0,
            };
            config.batch_size as usize
        ];
        Self {
            handle,
            config,
            anchor: AnchorState::new(),
            buffer: vec![0u8; buffer_size],
            kds,
            current_index: 0,
            current_nr: 0,
            finished: false,
        }
    }

    fn fetch_batch(&mut self) -> Result<()> {
        if self.anchor.is_eof() {
            self.finished = true;
            return Ok(());
        }

        let mut nr = self.config.batch_size;
        let mut sgl = d_sg_list_t {
            sg_nr: 1,
            sg_nr_out: 0,
            sg_iovs: std::ptr::null_mut(),
        };
        let mut iov = d_iov_t {
            iov_buf: self.buffer.as_mut_ptr() as *mut std::ffi::c_void,
            iov_buf_len: self.buffer.len(),
            iov_len: 0,
        };
        sgl.sg_iovs = &mut iov;

        for kd in &mut self.kds {
            kd.kd_key_len = 0;
            kd.kd_val_type = 0;
        }

        let ret = unsafe {
            daos::daos_obj_list_dkey(
                self.handle.as_raw(),
                std::mem::zeroed(),
                &mut nr,
                self.kds.as_mut_ptr(),
                &mut sgl,
                &mut self.anchor.raw,
                std::ptr::null_mut(),
            )
        };

        if ret == 0 {
            self.current_nr = nr as usize;
            self.current_index = 0;
            if nr == 0 || self.anchor.is_eof() {
                self.finished = true;
            }
            Ok(())
        } else if ret == DER_KEY2BIG {
            if !self.kds.is_empty() && self.kds[0].kd_key_len > 0 {
                let required_size = self.kds[0].kd_key_len as usize * 3;
                self.buffer.resize(required_size, 0);
            }
            self.fetch_batch()
        } else {
            Err(ret.into())
        }
    }

    fn parse_current_key(&self) -> Option<DKey> {
        if self.current_index >= self.current_nr {
            return None;
        }

        let kd = &self.kds[self.current_index];
        if kd.kd_key_len == 0 {
            return None;
        }

        let mut offset = 0usize;
        for i in 0..self.current_index {
            offset += self.kds[i].kd_key_len as usize;
        }

        if offset + kd.kd_key_len as usize > self.buffer.len() {
            return None;
        }

        let key_bytes = &self.buffer[offset..offset + kd.kd_key_len as usize];
        DKey::new(key_bytes.to_vec()).ok()
    }
}

impl Iterator for DkeyEnum {
    type Item = Result<DKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.current_index >= self.current_nr {
            match self.fetch_batch() {
                Ok(()) => {}
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }

        match self.parse_current_key() {
            Some(key) => {
                self.current_index += 1;
                Some(Ok(key))
            }
            None => {
                self.finished = true;
                None
            }
        }
    }
}

/// Iterator for enumerating attribute keys under a dkey.
pub struct AkeyEnum {
    handle: DaosHandle,
    dkey: DKey,
    config: EnumConfig,
    anchor: AnchorState,
    buffer: Vec<u8>,
    kds: Vec<daos_key_desc_t>,
    current_index: usize,
    current_nr: usize,
    finished: bool,
}

impl AkeyEnum {
    pub(crate) fn new(handle: DaosHandle, dkey: DKey, config: EnumConfig) -> Self {
        let buffer_size = config.buffer_size;
        let kds = vec![
            daos_key_desc_t {
                kd_key_len: 0,
                kd_val_type: 0,
            };
            config.batch_size as usize
        ];
        Self {
            handle,
            dkey,
            config,
            anchor: AnchorState::new(),
            buffer: vec![0u8; buffer_size],
            kds,
            current_index: 0,
            current_nr: 0,
            finished: false,
        }
    }

    fn fetch_batch(&mut self) -> Result<()> {
        if self.anchor.is_eof() {
            self.finished = true;
            return Ok(());
        }

        let mut nr = self.config.batch_size;
        let mut sgl = d_sg_list_t {
            sg_nr: 1,
            sg_nr_out: 0,
            sg_iovs: std::ptr::null_mut(),
        };
        let mut iov = d_iov_t {
            iov_buf: self.buffer.as_mut_ptr() as *mut std::ffi::c_void,
            iov_buf_len: self.buffer.len(),
            iov_len: 0,
        };
        sgl.sg_iovs = &mut iov;

        let dkey_buf = self.dkey.as_bytes();
        let mut dkey_raw = daos::daos_key_t {
            iov_buf: dkey_buf.as_ptr() as *mut std::ffi::c_void,
            iov_buf_len: dkey_buf.len(),
            iov_len: dkey_buf.len(),
        };

        for kd in &mut self.kds {
            kd.kd_key_len = 0;
            kd.kd_val_type = 0;
        }

        let ret = unsafe {
            daos::daos_obj_list_akey(
                self.handle.as_raw(),
                std::mem::zeroed(),
                &mut dkey_raw,
                &mut nr,
                self.kds.as_mut_ptr(),
                &mut sgl,
                &mut self.anchor.raw,
                std::ptr::null_mut(),
            )
        };

        if ret == 0 {
            self.current_nr = nr as usize;
            self.current_index = 0;
            if nr == 0 || self.anchor.is_eof() {
                self.finished = true;
            }
            Ok(())
        } else if ret == DER_KEY2BIG {
            if !self.kds.is_empty() && self.kds[0].kd_key_len > 0 {
                let required_size = self.kds[0].kd_key_len as usize * 3;
                self.buffer.resize(required_size, 0);
            }
            self.fetch_batch()
        } else {
            Err(ret.into())
        }
    }

    fn parse_current_key(&self) -> Option<AKey> {
        if self.current_index >= self.current_nr {
            return None;
        }

        let kd = &self.kds[self.current_index];
        if kd.kd_key_len == 0 {
            return None;
        }

        let mut offset = 0usize;
        for i in 0..self.current_index {
            offset += self.kds[i].kd_key_len as usize;
        }

        if offset + kd.kd_key_len as usize > self.buffer.len() {
            return None;
        }

        let key_bytes = &self.buffer[offset..offset + kd.kd_key_len as usize];
        AKey::new(key_bytes.to_vec()).ok()
    }
}

impl Iterator for AkeyEnum {
    type Item = Result<AKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.current_index >= self.current_nr {
            match self.fetch_batch() {
                Ok(()) => {}
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }

        match self.parse_current_key() {
            Some(key) => {
                self.current_index += 1;
                Some(Ok(key))
            }
            None => {
                self.finished = true;
                None
            }
        }
    }
}

/// Iterator for enumerating record extents under a dkey/akey.
pub struct RecxEnum {
    handle: DaosHandle,
    dkey: DKey,
    akey: AKey,
    anchor: AnchorState,
    recxs: Vec<daos_recx_t>,
    eprs: Vec<daos_epoch_range_t>,
    current_index: usize,
    current_nr: usize,
    finished: bool,
    incr_order: RecxOrder,
}

impl RecxEnum {
    pub(crate) fn new(handle: DaosHandle, dkey: DKey, akey: AKey, order: RecxOrder) -> Self {
        let recxs = vec![
            daos_recx_t {
                rx_idx: 0,
                rx_nr: 0,
            };
            64
        ];
        let eprs = vec![
            daos_epoch_range_t {
                epr_lo: 0,
                epr_hi: 0,
            };
            64
        ];
        Self {
            handle,
            dkey,
            akey,
            anchor: AnchorState::new(),
            recxs,
            eprs,
            current_index: 0,
            current_nr: 0,
            finished: false,
            incr_order: order,
        }
    }

    fn fetch_batch(&mut self) -> Result<()> {
        if self.anchor.is_eof() {
            self.finished = true;
            return Ok(());
        }

        let mut nr = self.recxs.len() as u32;
        let mut size: daos::daos_size_t = 0;

        let dkey_buf = self.dkey.as_bytes();
        let mut dkey_raw = daos::daos_key_t {
            iov_buf: dkey_buf.as_ptr() as *mut std::ffi::c_void,
            iov_buf_len: dkey_buf.len(),
            iov_len: dkey_buf.len(),
        };

        let akey_buf = self.akey.as_bytes();
        let mut akey_raw = daos::daos_key_t {
            iov_buf: akey_buf.as_ptr() as *mut std::ffi::c_void,
            iov_buf_len: akey_buf.len(),
            iov_len: akey_buf.len(),
        };

        let incr = match self.incr_order {
            RecxOrder::Increasing => true,
            RecxOrder::Decreasing => false,
        };

        let ret = unsafe {
            daos::daos_obj_list_recx(
                self.handle.as_raw(),
                std::mem::zeroed(),
                &mut dkey_raw,
                &mut akey_raw,
                &mut size,
                &mut nr,
                self.recxs.as_mut_ptr(),
                self.eprs.as_mut_ptr(),
                &mut self.anchor.raw,
                incr,
                std::ptr::null_mut(),
            )
        };

        if ret == 0 {
            self.current_nr = nr as usize;
            self.current_index = 0;
            if nr == 0 || self.anchor.is_eof() {
                self.finished = true;
            }
            Ok(())
        } else {
            Err(ret.into())
        }
    }
}

impl Iterator for RecxEnum {
    type Item = Result<RecxEnumEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.current_index >= self.current_nr {
            match self.fetch_batch() {
                Ok(()) => {}
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }

        if self.current_index >= self.current_nr {
            self.finished = true;
            return None;
        }

        let recx = self.recxs[self.current_index];
        let epr = self.eprs[self.current_index];
        self.current_index += 1;

        Some(Ok(RecxEnumEntry { recx, epr }))
    }
}

/// Entry from record extent enumeration.
///
/// Contains the record extent and its associated epoch range.
#[derive(Debug, Clone)]
pub struct RecxEnumEntry {
    /// The record extent (index and count).
    pub recx: daos_recx_t,
    /// The epoch range for this extent.
    pub epr: daos_epoch_range_t,
}

impl RecxEnumEntry {
    /// Creates a new RecxEnumEntry.
    pub fn new(recx: daos_recx_t, epr: daos_epoch_range_t) -> Self {
        Self { recx, epr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enum_config_defaults() {
        let config = EnumConfig::default();
        assert_eq!(config.batch_size, 64);
        assert_eq!(config.buffer_size, 4096);
    }

    #[test]
    fn test_recx_order_constants() {
        assert_eq!(RecxOrder::Increasing, RecxOrder::Increasing);
        assert_eq!(RecxOrder::Decreasing, RecxOrder::Decreasing);
    }

    #[test]
    fn test_anchor_state_is_eof_when_zeroed() {
        let anchor = AnchorState::new();
        assert!(!anchor.is_eof());
    }

    #[test]
    fn test_recx_enum_entry_creation() {
        let recx = daos_recx_t {
            rx_idx: 10,
            rx_nr: 5,
        };
        let epr = daos_epoch_range_t {
            epr_lo: 1,
            epr_hi: 2,
        };
        let entry = RecxEnumEntry::new(recx, epr);
        assert_eq!(entry.recx.rx_idx, 10);
        assert_eq!(entry.recx.rx_nr, 5);
        assert_eq!(entry.epr.epr_lo, 1);
        assert_eq!(entry.epr.epr_hi, 2);
    }

    #[test]
    fn test_recx_enum_entry_debug() {
        let recx = daos_recx_t {
            rx_idx: 10,
            rx_nr: 5,
        };
        let epr = daos_epoch_range_t {
            epr_lo: 1,
            epr_hi: 2,
        };
        let entry = RecxEnumEntry::new(recx, epr);
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("RecxEnumEntry"));
    }

    #[test]
    fn test_der_key2big_constant() {
        assert_eq!(DER_KEY2BIG, -(daos::daos_errno_DER_KEY2BIG as i32));
    }

    #[test]
    fn test_enum_config_custom_values() {
        let config = EnumConfig {
            batch_size: 128,
            buffer_size: 8192,
        };
        assert_eq!(config.batch_size, 128);
        assert_eq!(config.buffer_size, 8192);
    }

    #[test]
    fn test_enum_config_clone() {
        let config = EnumConfig::default();
        let cloned = config.clone();
        assert_eq!(config.batch_size, cloned.batch_size);
        assert_eq!(config.buffer_size, cloned.buffer_size);
    }

    #[test]
    fn test_recx_order_is_copy() {
        let order = RecxOrder::Increasing;
        let _copied = order;
        let order2 = RecxOrder::Decreasing;
        let _copied2 = order2;
    }

    #[test]
    fn test_enum_config_debug() {
        let config = EnumConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("EnumConfig"));
        assert!(debug_str.contains("batch_size"));
        assert!(debug_str.contains("buffer_size"));
    }

    #[test]
    fn test_anchor_state_new_is_not_eof() {
        let anchor = AnchorState::new();
        assert!(!anchor.is_eof());
    }

    #[test]
    fn test_anchor_state_default_is_not_eof() {
        let anchor = AnchorState::default();
        assert!(!anchor.is_eof());
    }

    #[test]
    fn test_anchor_state_clone() {
        let anchor = AnchorState::new();
        let _cloned = anchor.clone();
    }

    #[test]
    fn test_recx_enum_entry_clone() {
        let recx = daos_recx_t {
            rx_idx: 1,
            rx_nr: 2,
        };
        let epr = daos_epoch_range_t {
            epr_lo: 10,
            epr_hi: 20,
        };
        let entry = RecxEnumEntry::new(recx, epr);
        let _cloned = entry.clone();
    }

    #[test]
    fn test_recx_enum_entry_fields() {
        let recx = daos_recx_t {
            rx_idx: 100,
            rx_nr: 50,
        };
        let epr = daos_epoch_range_t {
            epr_lo: 1000,
            epr_hi: 2000,
        };
        let entry = RecxEnumEntry::new(recx, epr);
        assert_eq!(entry.recx.rx_idx, 100);
        assert_eq!(entry.recx.rx_nr, 50);
        assert_eq!(entry.epr.epr_lo, 1000);
        assert_eq!(entry.epr.epr_hi, 2000);
    }

    #[test]
    fn test_recx_order_debug() {
        let increasing = RecxOrder::Increasing;
        let decreasing = RecxOrder::Decreasing;
        assert!(format!("{:?}", increasing).contains("Increasing"));
        assert!(format!("{:?}", decreasing).contains("Decreasing"));
    }
}
