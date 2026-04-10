//! Key and buffer abstractions for DAOS object I/O.
//!
//! This module provides type-safe wrappers for DAOS keys and buffers:
//!
//! - [`DKey`] - Distribution key for object records
//! - [`AKey`] - Attribute key for storing values
//! - [`IoBuffer`] - Memory buffer for data transfer
//! - [`Sgl`] - Scatter-gather list for efficient I/O
//! - [`Iod`] - I/O descriptor describing data layout
//!
//! # Example: Simple KV Store
//!
//! ```ignore
//! use daoxide::io::{DKey, AKey, IoBuffer, Sgl, Iod, IodSingleBuilder};
//!
//! let dkey = DKey::new(b"my_dkey")?;
//! let akey = AKey::new(b"my_akey")?;
//! let value = IoBuffer::from_vec(b"hello world".to_vec());
//!
//! let iod = Iod::Single(IodSingleBuilder::new(akey)
//!     .value_len(value.len())
//!     .build()?);
//!
//! let sgl = Sgl::builder()
//!     .push(value)
//!     .build()?;
//! ```

use crate::error::{DaosError, Result};
use daos::{
    d_iov_t, d_sg_list_t, daos_iod_t, daos_iod_type_t_DAOS_IOD_ARRAY,
    daos_iod_type_t_DAOS_IOD_SINGLE, daos_key_t, daos_recx_t,
};

/// Distribution key (dkey) for DAOS objects.
///
/// DKeys are the top-level keys in DAOS object storage. Each object
/// can have multiple dkeys, which partition the object's keyspace.
///
/// # Example
///
/// ```
/// use daoxide::io::DKey;
///
/// let dkey = DKey::new(b"my_dkey").unwrap();
/// assert_eq!(dkey.as_bytes(), b"my_dkey");
/// ```
///
/// # Constraints
///
/// - DKey cannot be empty
/// - Maximum dkey size is determined by DAOS container properties
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DKey(Vec<u8>);

impl DKey {
    /// Creates a new DKey from bytes.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if the key is empty.
    ///
    /// # Example
    ///
    /// ```
    /// use daoxide::io::DKey;
    ///
    /// let dkey = DKey::new(b"my_key").unwrap();
    /// ```
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(DaosError::InvalidArg);
        }
        Ok(Self(bytes))
    }

    /// Returns the raw bytes of this key.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Attribute key (akey) for DAOS records.
///
/// AKeys are second-level keys stored under a [`DKey`]. Each dkey can have
/// multiple akeys, allowing for flexible nested key-value storage.
///
/// # Example
///
/// ```
/// use daoxide::io::AKey;
///
/// let akey = AKey::new(b"my_akey").unwrap();
/// assert_eq!(akey.as_bytes(), b"my_akey");
/// ```
///
/// # Constraints
///
/// - AKey cannot be empty
/// - Maximum akey size is determined by DAOS container properties
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AKey(Vec<u8>);

impl AKey {
    /// Creates a new AKey from bytes.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if the key is empty.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(DaosError::InvalidArg);
        }
        Ok(Self(bytes))
    }

    /// Returns the raw bytes of this key.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Memory buffer for DAOS I/O operations.
///
/// `IoBuffer` wraps a `Vec<u8>` and provides a stable interface for
/// data transfer to and from DAOS objects.
///
/// # Example
///
/// ```
/// use daoxide::io::IoBuffer;
///
/// let buffer = IoBuffer::from_vec(vec![1, 2, 3, 4, 5]);
/// assert_eq!(buffer.len(), 5);
/// assert_eq!(buffer.as_slice(), &[1, 2, 3, 4, 5]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoBuffer {
    bytes: Vec<u8>,
}

impl IoBuffer {
    /// Creates a buffer from a `Vec<u8>`.
    ///
    /// Takes ownership of the data.
    #[inline]
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Returns a slice of the buffer contents.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns a mutable slice of the buffer contents.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    /// Returns the length of the buffer in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Scatter-gather list for efficient DAOS I/O.
///
/// An `Sgl` holds multiple [`IoBuffer`]s that can be read from or written to
/// in a single DAOS I/O operation. This allows combining multiple buffers
/// without extra copies.
///
/// # Example
///
/// ```
/// use daoxide::io::{IoBuffer, Sgl};
///
/// let sgl = Sgl::builder()
///     .push(IoBuffer::from_vec(vec![1, 2, 3]))
///     .push(IoBuffer::from_vec(vec![4, 5, 6]))
///     .build()
///     .unwrap();
///
/// assert_eq!(sgl.buffers().len(), 2);
/// assert_eq!(sgl.total_len(), 6);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sgl {
    buffers: Vec<IoBuffer>,
}

impl Sgl {
    /// Creates a new [`SglBuilder`] for constructing an Sgl.
    #[inline]
    pub fn builder() -> SglBuilder {
        SglBuilder::new()
    }

    /// Returns the buffers in this Sgl.
    #[inline]
    pub fn buffers(&self) -> &[IoBuffer] {
        &self.buffers
    }

    /// Returns the total length of all buffers.
    #[inline]
    pub fn total_len(&self) -> usize {
        self.buffers.iter().map(IoBuffer::len).sum()
    }

    /// Returns true if this Sgl has no buffers.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }

    /// Converts to the raw DAOS scatter-gather list representation.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if no buffers are present.
    pub fn to_raw(&self) -> Result<RawSgl> {
        if self.buffers.is_empty() {
            return Err(DaosError::InvalidArg);
        }

        let mut iovs = Vec::with_capacity(self.buffers.len());
        for buffer in &self.buffers {
            iovs.push(d_iov_t {
                iov_buf: buffer.as_slice().as_ptr() as *mut std::ffi::c_void,
                iov_buf_len: buffer.len(),
                iov_len: buffer.len(),
            });
        }

        let mut sgl = d_sg_list_t {
            sg_nr: iovs.len() as u32,
            sg_nr_out: iovs.len() as u32,
            sg_iovs: std::ptr::null_mut(),
        };

        sgl.sg_iovs = iovs.as_mut_ptr();

        Ok(RawSgl { iovs, sgl })
    }
}

/// Builder for creating [`Sgl`] instances.
#[derive(Debug, Default)]
pub struct SglBuilder {
    buffers: Vec<IoBuffer>,
}

impl SglBuilder {
    /// Creates a new SglBuilder.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a buffer to the Sgl being built.
    ///
    /// Returns the builder for chaining.
    #[inline]
    pub fn push(mut self, buffer: IoBuffer) -> Self {
        self.buffers.push(buffer);
        self
    }

    /// Builds the [`Sgl`] from this builder.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if no buffers were added.
    #[inline]
    pub fn build(self) -> Result<Sgl> {
        if self.buffers.is_empty() {
            return Err(DaosError::InvalidArg);
        }
        Ok(Sgl {
            buffers: self.buffers,
        })
    }
}

/// Raw scatter-gather list for FFI interop.
pub struct RawSgl {
    /// Vector of iovec structures backing this Sgl.
    pub iovs: Vec<d_iov_t>,
    /// The raw DAOS scatter-gather list structure.
    pub sgl: d_sg_list_t,
}

/// Record extent for array values.
///
/// A `Recx` describes a contiguous range of records within an array object.
/// The `idx` field specifies the starting record index, and `nr` specifies
/// the number of records in this extent.
///
/// # Example
///
/// ```
/// use daoxide::io::Recx;
///
/// let recx = Recx::new(10, 5).unwrap();
/// assert_eq!(recx.idx, 10);
/// assert_eq!(recx.nr, 5);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recx {
    /// Starting record index.
    pub idx: u64,
    /// Number of records in this extent.
    pub nr: u64,
}

impl Recx {
    /// Creates a new Recx with the given index and record count.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if `nr` is zero.
    #[inline]
    pub fn new(idx: u64, nr: u64) -> Result<Self> {
        if nr == 0 {
            return Err(DaosError::InvalidArg);
        }
        Ok(Self { idx, nr })
    }
}

/// I/O descriptor for single-value data.
///
/// A single-value IOD stores a fixed-size value at an akey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IodSingle {
    /// The akey for this value.
    pub akey: AKey,
    /// Size of the value in bytes.
    pub value_len: usize,
}

/// I/O descriptor for array-value data.
///
/// An array IOD stores records with a fixed record size at an akey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IodArray {
    /// The akey for these records.
    pub akey: AKey,
    /// Size of each record in bytes.
    pub record_len: usize,
    /// Record extents describing the data layout.
    pub recxs: Vec<Recx>,
}

/// I/O descriptor describing data layout.
///
/// `Iod` can represent either a single value or an array of records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Iod {
    /// Single fixed-size value.
    Single(IodSingle),
    /// Array of records with extents.
    Array(IodArray),
}

/// Builder for creating [`IodSingle`] instances.
///
/// # Example
///
/// ```
/// use daoxide::io::{AKey, Iod, IodSingleBuilder};
///
/// let akey = AKey::new(b"my_akey").unwrap();
/// let iod = Iod::Single(
///     IodSingleBuilder::new(akey)
///         .value_len(8)
///         .build()
///         .unwrap()
/// );
/// ```
#[derive(Debug)]
pub struct IodSingleBuilder {
    akey: AKey,
    value_len: Option<usize>,
}

impl IodSingleBuilder {
    /// Creates a new builder for an IodSingle.
    pub fn new(akey: AKey) -> Self {
        Self {
            akey,
            value_len: None,
        }
    }

    /// Sets the expected value length.
    pub fn value_len(mut self, value_len: usize) -> Self {
        self.value_len = Some(value_len);
        self
    }

    /// Builds the [`IodSingle`].
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if value_len was not set or is zero.
    pub fn build(self) -> Result<IodSingle> {
        let value_len = self.value_len.ok_or(DaosError::InvalidArg)?;
        if value_len == 0 {
            return Err(DaosError::InvalidArg);
        }
        Ok(IodSingle {
            akey: self.akey,
            value_len,
        })
    }
}

/// Builder for creating [`IodArray`] instances.
///
/// # Example
///
/// ```
/// use daoxide::io::{AKey, Iod, IodArrayBuilder, Recx};
///
/// let akey = AKey::new(b"my_array_akey").unwrap();
/// let recx = Recx::new(0, 10).unwrap();
/// let iod = Iod::Array(
///     IodArrayBuilder::new(akey)
///         .record_len(8)
///         .add_recx(recx)
///         .build()
///         .unwrap()
/// );
/// ```
#[derive(Debug)]
pub struct IodArrayBuilder {
    akey: AKey,
    record_len: Option<usize>,
    recxs: Vec<Recx>,
}

impl IodArrayBuilder {
    /// Creates a new builder for an IodArray.
    pub fn new(akey: AKey) -> Self {
        Self {
            akey,
            record_len: None,
            recxs: Vec::new(),
        }
    }

    /// Sets the record length in bytes.
    pub fn record_len(mut self, record_len: usize) -> Self {
        self.record_len = Some(record_len);
        self
    }

    /// Adds a record extent to the array.
    pub fn add_recx(mut self, recx: Recx) -> Self {
        self.recxs.push(recx);
        self
    }

    /// Builds the [`IodArray`].
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if record_len is not set, is zero,
    /// or no record extents were added.
    pub fn build(self) -> Result<IodArray> {
        let record_len = self.record_len.ok_or(DaosError::InvalidArg)?;
        if record_len == 0 || self.recxs.is_empty() {
            return Err(DaosError::InvalidArg);
        }

        let mut total_records: u64 = 0;
        for recx in &self.recxs {
            total_records = total_records
                .checked_add(recx.nr)
                .ok_or(DaosError::InvalidArg)?;
        }

        let _total_bytes = (record_len as u128)
            .checked_mul(total_records as u128)
            .ok_or(DaosError::InvalidArg)?;

        Ok(IodArray {
            akey: self.akey,
            record_len,
            recxs: self.recxs,
        })
    }
}

/// Raw I/O descriptor for FFI interop.
pub struct RawIod {
    /// Buffer holding the encoded akey.
    pub akey_buf: Vec<u8>,
    /// Record extents for array types.
    pub recxs: Vec<daos_recx_t>,
    /// The raw DAOS I/O descriptor.
    pub iod: daos_iod_t,
}

impl Iod {
    /// Converts to the raw DAOS I/O descriptor for FFI calls.
    ///
    /// # Errors
    ///
    /// Returns `Err(DaosError::InvalidArg)` if:
    /// - For single-value: value_len is zero
    /// - For array: record_len is zero or no extents were added
    pub fn to_raw(&self) -> Result<RawIod> {
        match self {
            Iod::Single(single) => {
                if single.value_len == 0 {
                    return Err(DaosError::InvalidArg);
                }
                let mut akey_buf = single.akey.as_bytes().to_vec();
                let key = daos_key_t {
                    iov_buf: akey_buf.as_mut_ptr() as *mut std::ffi::c_void,
                    iov_buf_len: akey_buf.len(),
                    iov_len: akey_buf.len(),
                };

                Ok(RawIod {
                    akey_buf,
                    recxs: Vec::new(),
                    iod: daos_iod_t {
                        iod_name: key,
                        iod_type: daos_iod_type_t_DAOS_IOD_SINGLE,
                        iod_size: single.value_len as u64,
                        iod_flags: 0,
                        iod_nr: 1,
                        iod_recxs: std::ptr::null_mut(),
                    },
                })
            }
            Iod::Array(array) => {
                if array.record_len == 0 || array.recxs.is_empty() {
                    return Err(DaosError::InvalidArg);
                }

                let mut akey_buf = array.akey.as_bytes().to_vec();
                let key = daos_key_t {
                    iov_buf: akey_buf.as_mut_ptr() as *mut std::ffi::c_void,
                    iov_buf_len: akey_buf.len(),
                    iov_len: akey_buf.len(),
                };

                let mut recxs: Vec<daos_recx_t> = array
                    .recxs
                    .iter()
                    .map(|r| daos_recx_t {
                        rx_idx: r.idx,
                        rx_nr: r.nr,
                    })
                    .collect();

                Ok(RawIod {
                    akey_buf,
                    iod: daos_iod_t {
                        iod_name: key,
                        iod_type: daos_iod_type_t_DAOS_IOD_ARRAY,
                        iod_size: array.record_len as u64,
                        iod_flags: 0,
                        iod_nr: recxs.len() as u32,
                        iod_recxs: recxs.as_mut_ptr(),
                    },
                    recxs,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keys_reject_empty() {
        assert!(DKey::new(Vec::<u8>::new()).is_err());
        assert!(AKey::new(Vec::<u8>::new()).is_err());
    }

    #[test]
    fn test_keys_accept_non_empty() {
        let dkey = DKey::new(b"dkey".to_vec()).unwrap();
        let akey = AKey::new(b"akey".to_vec()).unwrap();
        assert_eq!(dkey.as_bytes(), b"dkey");
        assert_eq!(akey.as_bytes(), b"akey");
    }

    #[test]
    fn test_sgl_builder_rejects_empty() {
        assert!(Sgl::builder().build().is_err());
    }

    #[test]
    fn test_sgl_builder_accepts_buffers() {
        let sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![1, 2, 3]))
            .push(IoBuffer::from_vec(vec![4, 5]))
            .build()
            .unwrap();
        assert_eq!(sgl.buffers().len(), 2);
        assert_eq!(sgl.total_len(), 5);
    }

    #[test]
    fn test_sgl_to_raw() {
        let sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![1, 2, 3]))
            .push(IoBuffer::from_vec(vec![4]))
            .build()
            .unwrap();
        let raw = sgl.to_raw().unwrap();
        assert_eq!(raw.sgl.sg_nr, 2);
        assert_eq!(raw.sgl.sg_nr_out, 2);
        assert!(!raw.sgl.sg_iovs.is_null());
        assert_eq!(raw.iovs.len(), 2);
    }

    #[test]
    fn test_recx_validation() {
        assert!(Recx::new(0, 0).is_err());
        let recx = Recx::new(42, 7).unwrap();
        assert_eq!(recx.idx, 42);
        assert_eq!(recx.nr, 7);
    }

    #[test]
    fn test_iod_single_builder_validation() {
        let akey = AKey::new(b"a".to_vec()).unwrap();
        assert!(IodSingleBuilder::new(akey.clone()).build().is_err());
        assert!(IodSingleBuilder::new(akey).value_len(0).build().is_err());
    }

    #[test]
    fn test_iod_single_builder_success() {
        let akey = AKey::new(b"a".to_vec()).unwrap();
        let single = IodSingleBuilder::new(akey).value_len(16).build().unwrap();
        assert_eq!(single.value_len, 16);
    }

    #[test]
    fn test_iod_array_builder_validation() {
        let akey = AKey::new(b"a".to_vec()).unwrap();
        assert!(IodArrayBuilder::new(akey.clone()).build().is_err());
        assert!(
            IodArrayBuilder::new(akey.clone())
                .record_len(0)
                .build()
                .is_err()
        );

        let recx = Recx::new(0, 1).unwrap();
        let ok = IodArrayBuilder::new(akey)
            .record_len(8)
            .add_recx(recx)
            .build()
            .unwrap();
        assert_eq!(ok.recxs.len(), 1);
    }

    #[test]
    fn test_iod_to_raw_single() {
        let akey = AKey::new(b"akey".to_vec()).unwrap();
        let single = Iod::Single(IodSingleBuilder::new(akey).value_len(32).build().unwrap());
        let raw = single.to_raw().unwrap();
        assert_eq!(raw.iod.iod_type, daos_iod_type_t_DAOS_IOD_SINGLE);
        assert_eq!(raw.iod.iod_size, 32);
        assert_eq!(raw.iod.iod_nr, 1);
        assert!(raw.iod.iod_recxs.is_null());
    }

    #[test]
    fn test_iod_to_raw_array() {
        let akey = AKey::new(b"akey".to_vec()).unwrap();
        let recx1 = Recx::new(0, 2).unwrap();
        let recx2 = Recx::new(10, 3).unwrap();
        let array = Iod::Array(
            IodArrayBuilder::new(akey)
                .record_len(8)
                .add_recx(recx1)
                .add_recx(recx2)
                .build()
                .unwrap(),
        );

        let raw = array.to_raw().unwrap();
        assert_eq!(raw.iod.iod_type, daos_iod_type_t_DAOS_IOD_ARRAY);
        assert_eq!(raw.iod.iod_size, 8);
        assert_eq!(raw.iod.iod_nr, 2);
        assert!(!raw.iod.iod_recxs.is_null());
        assert_eq!(raw.recxs.len(), 2);
        assert_eq!(raw.recxs[0].rx_idx, 0);
        assert_eq!(raw.recxs[0].rx_nr, 2);
    }
}
