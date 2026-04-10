//! Direct FFI wrappers calling daos-rs functions.
//!
//! All unsafe FFI calls to the `daos-rs` crate are centralized here.
//! Each function has explicit SAFETY documentation explaining the
//! invariants that must be maintained.

use crate::error::{Result, from_daos_errno};
use crate::unsafe_inner::handle::{DaosHandle, validate_handle};
use crate::unsafe_inner::pointer::as_const_char_ptr;
use daos::{
    daos_cont_info_t, daos_epoch_t, daos_event_t, daos_handle_t, daos_key_t, daos_obj_id_t,
    daos_oclass_hints_t, daos_oclass_id_t, daos_otype_t, daos_pool_info_t, daos_recx_t,
};
use std::ptr::NonNull;

/// DAOS success return code.
const DER_SUCCESS: i32 = 0;

/// Calls daos_init() to initialize the DAOS library.
///
/// SAFETY: This function is safe to call multiple times; subsequent calls
/// after the first are no-ops according to the DAOS API.
pub fn daos_init() -> Result<()> {
    let ret = unsafe { daos::daos_init() };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Calls daos_fini() to finalize the DAOS library.
pub fn daos_fini() -> Result<()> {
    let ret = unsafe { daos::daos_fini() };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Connects to a DAOS pool.
///
/// SAFETY: The pool connection string must be valid NUL-terminated UTF-8.
/// The `poh` output handle must not be used if this function returns an error.
/// Callers should use `validate_handle()` on the returned handle before use.
pub fn daos_pool_connect(pool: &str, sys: Option<&str>, flags: u32) -> Result<DaosHandle> {
    let pool_ptr = as_const_char_ptr(pool)?;
    let sys_ptr: Option<std::ptr::NonNull<std::os::raw::c_char>> = match sys {
        Some(s) => Some(as_const_char_ptr(s)?),
        None => None,
    };

    let mut handle: daos_handle_t = daos_handle_t { cookie: 0 };
    let ret = unsafe {
        daos::daos_pool_connect2(
            pool_ptr.as_ptr().cast(),
            sys_ptr
                .map(|p| p.as_ptr().cast())
                .unwrap_or(std::ptr::null_mut()),
            flags,
            &mut handle,
            std::ptr::null_mut(), // info
            std::ptr::null_mut(), // ev
        )
    };

    if ret == DER_SUCCESS {
        validate_handle(handle)?;
        // SAFETY: We've validated the handle above
        Ok(unsafe { DaosHandle::from_raw(handle) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Disconnects from a DAOS pool.
///
/// SAFETY: The handle must be valid and obtained from daos_pool_connect().
/// The handle becomes invalid after this call and must not be used.
pub fn daos_pool_disconnect(poh: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_pool_disconnect(poh.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Opens a DAOS container within a pool.
///
/// SAFETY: The pool handle must be valid. The container path must be a
/// valid NUL-terminated string. The returned handle must be validated
/// before use.
pub fn daos_cont_open(poh: DaosHandle, cont: &str, flags: u32) -> Result<DaosHandle> {
    let cont_ptr = as_const_char_ptr(cont)?;

    let mut coh: daos_handle_t = daos_handle_t { cookie: 0 };
    let ret = unsafe {
        daos::daos_cont_open2(
            poh.as_raw(),
            cont_ptr.as_ptr(),
            flags,
            &mut coh,
            std::ptr::null_mut(), // info
            std::ptr::null_mut(), // ev
        )
    };

    if ret == DER_SUCCESS {
        validate_handle(coh)?;
        // SAFETY: Handle validated above
        Ok(unsafe { DaosHandle::from_raw(coh) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Closes a DAOS container.
///
/// SAFETY: The handle must be valid and obtained from daos_cont_open().
pub fn daos_cont_close(coh: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_cont_close(coh.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Queries container information.
///
/// SAFETY: The container handle must be valid. The info pointer must be
/// valid for writing sizeof(daos_cont_info_t) bytes.
pub fn daos_cont_query(coh: DaosHandle) -> Result<daos_cont_info_t> {
    let mut info: daos_cont_info_t = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        daos::daos_cont_query(
            coh.as_raw(),
            &mut info,
            std::ptr::null_mut(), // cont_prop
            std::ptr::null_mut(), // ev
        )
    };
    if ret == DER_SUCCESS {
        Ok(info)
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Creates a new container with a label on the pool connected by poh.
///
/// SAFETY: The pool handle must be valid. The label must be a valid
/// NUL-terminated string. The uuid pointer must be valid for writing
/// sizeof(uuid_t) bytes if not null.
pub fn daos_cont_create_with_label(
    poh: DaosHandle,
    label: &str,
    uuid: Option<&mut std::mem::MaybeUninit<daos::uuid_t>>,
) -> Result<()> {
    let label_ptr = as_const_char_ptr(label)?;
    let uuid_ptr = match uuid {
        Some(u) => u.as_mut_ptr(),
        None => std::ptr::null_mut(),
    };
    let ret = unsafe {
        daos::daos_cont_create_with_label(
            poh.as_raw(),
            label_ptr.as_ptr(),
            std::ptr::null_mut(), // cont_prop
            uuid_ptr,
            std::ptr::null_mut(), // ev
        )
    };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Allocates object IDs in a container.
///
/// SAFETY: The container handle must be valid. The oid pointer must be
/// valid for writing sizeof(u64) bytes.
pub fn daos_cont_alloc_oids(coh: DaosHandle, num_oids: u64) -> Result<u64> {
    let mut oid: u64 = 0;
    let ret = unsafe {
        daos::daos_cont_alloc_oids(
            coh.as_raw(),
            num_oids,
            &mut oid,
            std::ptr::null_mut(), // ev
        )
    };
    if ret == DER_SUCCESS {
        Ok(oid)
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Queries pool information.
///
/// SAFETY: The pool handle must be valid. The info pointer must be
/// valid for writing sizeof(daos_pool_info_t) bytes.
pub fn daos_pool_query(poh: DaosHandle) -> Result<daos_pool_info_t> {
    let mut info: daos_pool_info_t = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        daos::daos_pool_query(
            poh.as_raw(),
            std::ptr::null_mut(), // ranks
            &mut info,
            std::ptr::null_mut(), // pool_prop
            std::ptr::null_mut(), // ev
        )
    };
    if ret == DER_SUCCESS {
        Ok(info)
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Generates an object ID in a container.
///
/// SAFETY: The container handle must be valid. The oid pointer must be
/// valid for writing sizeof(daos_obj_id_t) bytes.
pub fn daos_obj_generate_oid(coh: DaosHandle, oid: &mut daos_obj_id_t) -> Result<()> {
    let ret = unsafe {
        daos::daos_obj_generate_oid2(
            coh.as_raw(),
            oid,
            0 as daos_otype_t,        // type_
            0 as daos_oclass_id_t,    // cid
            0 as daos_oclass_hints_t, // hints
            0,
        )
    };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Opens an object handle in a container.
///
/// SAFETY: The container handle must be valid. The object ID must be generated
/// for this container. The returned handle must be validated before use.
pub fn daos_obj_open(coh: DaosHandle, oid: daos_obj_id_t, mode: u32) -> Result<DaosHandle> {
    let mut oh: daos_handle_t = daos_handle_t { cookie: 0 };
    let ret =
        unsafe { daos::daos_obj_open(coh.as_raw(), oid, mode, &mut oh, std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        validate_handle(oh)?;
        Ok(unsafe { DaosHandle::from_raw(oh) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Closes an object handle.
///
/// SAFETY: The object handle must be valid and obtained from `daos_obj_open`.
pub fn daos_obj_close(oh: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_obj_close(oh.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Creates a container snapshot.
///
/// SAFETY: The container handle must be valid. The epoch pointer must be
/// valid for writing sizeof(daos_epoch_t) bytes if not null.
pub fn daos_cont_create_snap(coh: DaosHandle, name: Option<&str>) -> Result<u64> {
    let name_ptr = match name {
        Some(n) => Some(as_const_char_ptr(n)?),
        None => None,
    };
    let mut epoch: u64 = 0;
    let ret = unsafe {
        daos::daos_cont_create_snap(
            coh.as_raw(),
            &mut epoch,
            name_ptr.map(|p| p.as_ptr()).unwrap_or(std::ptr::null_mut()),
            std::ptr::null_mut(), // ev
        )
    };
    if ret == DER_SUCCESS {
        Ok(epoch)
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Destroys a container snapshot.
///
/// SAFETY: The container handle must be valid.
pub fn daos_cont_destroy_snap(coh: DaosHandle, epoch: u64) -> Result<()> {
    let epr = daos::daos_epoch_range_t {
        epr_lo: epoch,
        epr_hi: epoch,
    };
    let ret = unsafe {
        daos::daos_cont_destroy_snap(
            coh.as_raw(),
            epr,
            std::ptr::null_mut(), // ev
        )
    };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Creates an event queue.
///
/// SAFETY: The eqh pointer must be valid for writing sizeof(daos_handle_t) bytes.
pub fn daos_eq_create() -> Result<DaosHandle> {
    let mut eqh: daos_handle_t = daos_handle_t { cookie: 0 };
    let ret = unsafe { daos::daos_eq_create(&mut eqh) };
    if ret == DER_SUCCESS {
        validate_handle(eqh)?;
        Ok(unsafe { DaosHandle::from_raw(eqh) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Destroys an event queue.
///
/// SAFETY: The eq handle must be valid and obtained from daos_eq_create().
pub fn daos_eq_destroy(eqh: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_eq_destroy(eqh.as_raw(), 0) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Initializes an event.
///
/// SAFETY: The event pointer must be valid for writing sizeof(daos_event_t) bytes.
pub fn daos_event_init(eqh: DaosHandle) -> Result<NonNull<daos_event_t>> {
    let mut ev: daos_event_t = unsafe { std::mem::zeroed() };
    let ret = unsafe { daos::daos_event_init(&mut ev, eqh.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        // SAFETY: We've just initialized ev and it has valid memory
        Ok(unsafe { NonNull::new_unchecked(&mut ev) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Finalizes an event.
///
/// SAFETY: The event pointer must be valid and initialized.
pub fn daos_event_fini(ev: NonNull<daos_event_t>) -> Result<()> {
    let ret = unsafe { daos::daos_event_fini(ev.as_ptr()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Transaction flags for daos_tx_open.
pub mod tx_flags {
    use daos::DAOS_TF_RDONLY;

    pub const TX_RDONLY: u64 = DAOS_TF_RDONLY as u64;
    pub const TX_ZERO_COPY: u64 = (daos::DAOS_TF_ZERO_COPY) as u64;
}

/// Opens a transaction on a container handle.
///
/// SAFETY: The container handle must be valid. The returned transaction
/// handle must be validated before use.
pub fn daos_tx_open(coh: DaosHandle, flags: u64) -> Result<DaosHandle> {
    let mut th: daos_handle_t = daos_handle_t { cookie: 0 };
    let ret = unsafe { daos::daos_tx_open(coh.as_raw(), &mut th, flags, std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        validate_handle(th)?;
        Ok(unsafe { DaosHandle::from_raw(th) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Opens a read-only transaction on a container snapshot.
///
/// SAFETY: The container handle must be valid. The epoch must be a valid
/// snapshot epoch. The returned transaction handle must be validated before use.
pub fn daos_tx_open_snap(coh: DaosHandle, epoch: u64) -> Result<DaosHandle> {
    let mut th: daos_handle_t = daos_handle_t { cookie: 0 };
    let ret =
        unsafe { daos::daos_tx_open_snap(coh.as_raw(), epoch, &mut th, std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        validate_handle(th)?;
        Ok(unsafe { DaosHandle::from_raw(th) })
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Commits a transaction.
///
/// SAFETY: The transaction handle must be valid and obtained from daos_tx_open()
/// or daos_tx_open_snap(). After commit, the handle cannot be used for new IO.
/// May return TxRestart if the transaction needs to be restarted.
pub fn daos_tx_commit(th: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_tx_commit(th.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Aborts a transaction, discarding all modifications.
///
/// SAFETY: The transaction handle must be valid and obtained from daos_tx_open().
/// After abort, the handle cannot be used for new IO.
pub fn daos_tx_abort(th: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_tx_abort(th.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Closes a transaction handle.
///
/// SAFETY: The transaction handle must be valid. This is a local operation.
/// The handle becomes invalid after this call.
pub fn daos_tx_close(th: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_tx_close(th.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Restarts a transaction after encountering a TxRestart error.
///
/// SAFETY: The transaction handle must be valid and previously opened.
/// This drops all IOs that have been issued via the transaction handle. This is a local operation.
pub fn daos_tx_restart(th: DaosHandle) -> Result<()> {
    let ret = unsafe { daos::daos_tx_restart(th.as_raw(), std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Punches an entire object with all keys associated with it.
///
/// SAFETY: The object handle must be valid. The transaction handle can be
/// DAOS_HANDLE_NULL for independent operations.
pub fn daos_obj_punch(oh: DaosHandle, th: daos_handle_t, flags: u64) -> Result<()> {
    let ret = unsafe { daos::daos_obj_punch(oh.as_raw(), th, flags, std::ptr::null_mut()) };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Punches dkeys (with all akeys) from an object.
///
/// SAFETY: The object handle must be valid. The dkeys array must be valid
/// for reading nr elements. The transaction handle can be DAOS_HANDLE_NULL.
pub fn daos_obj_punch_dkeys(
    oh: DaosHandle,
    th: daos_handle_t,
    flags: u64,
    dkeys: &[daos_key_t],
) -> Result<()> {
    let ret = unsafe {
        daos::daos_obj_punch_dkeys(
            oh.as_raw(),
            th,
            flags,
            dkeys.len() as u32,
            dkeys.as_ptr() as *mut daos_key_t,
            std::ptr::null_mut(),
        )
    };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Punches akeys (with all records) from an object.
///
/// SAFETY: The object handle must be valid. The dkey must be valid. The akeys
/// array must be valid for reading nr elements. The transaction handle can be DAOS_HANDLE_NULL.
pub fn daos_obj_punch_akeys(
    oh: DaosHandle,
    th: daos_handle_t,
    flags: u64,
    dkey: &daos_key_t,
    akeys: &[daos_key_t],
) -> Result<()> {
    let ret = unsafe {
        daos::daos_obj_punch_akeys(
            oh.as_raw(),
            th,
            flags,
            dkey as *const daos_key_t as *mut daos_key_t,
            akeys.len() as u32,
            akeys.as_ptr() as *mut daos_key_t,
            std::ptr::null_mut(),
        )
    };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Query flags for daos_obj_query_key.
pub mod query_flags {
    /// Retrieve the max of dkey, akey, and/or idx of array value.
    pub const GET_MAX: u64 = 1 << 0;
    /// Retrieve the min of dkey, akey, and/or idx of array value.
    pub const GET_MIN: u64 = 1 << 1;
    /// Retrieve the dkey.
    pub const GET_DKEY: u64 = 1 << 2;
    /// Retrieve the akey.
    pub const GET_AKEY: u64 = 1 << 3;
    /// Retrieve the idx of array value.
    pub const GET_RECX: u64 = 1 << 4;
}

/// Retrieves the largest or smallest integer DKEY, AKEY, and array offset from an object.
///
/// SAFETY: The object handle must be valid. The transaction handle can be DAOS_HANDLE_NULL.
/// The dkey and akey buffers must be valid for reading and writing.
pub fn daos_obj_query_key(
    oh: DaosHandle,
    th: daos_handle_t,
    flags: u64,
    dkey: &mut daos_key_t,
    akey: &mut daos_key_t,
    recx: &mut daos_recx_t,
) -> Result<()> {
    let ret = unsafe {
        daos::daos_obj_query_key(
            oh.as_raw(),
            th,
            flags,
            dkey as *mut daos_key_t,
            akey as *mut daos_key_t,
            recx as *mut daos_recx_t,
            std::ptr::null_mut(),
        )
    };
    if ret == DER_SUCCESS {
        Ok(())
    } else {
        Err(from_daos_errno(ret))
    }
}

/// Retrieves the max epoch where the object has been updated.
///
/// SAFETY: The object handle must be valid. The transaction handle can be DAOS_HANDLE_NULL.
pub fn daos_obj_query_max_epoch(oh: DaosHandle, th: daos_handle_t) -> Result<u64> {
    let mut epoch: u64 = 0;
    let ret = unsafe {
        daos::daos_obj_query_max_epoch(oh.as_raw(), th, &mut epoch, std::ptr::null_mut())
    };
    if ret == DER_SUCCESS {
        Ok(epoch)
    } else {
        Err(from_daos_errno(ret))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unsafe_inner::handle::is_valid_handle;

    #[test]
    fn test_daos_init_and_fini() {
        let result = daos_init();
        assert!(result.is_ok());
        let result = daos_fini();
        assert!(result.is_ok());
    }

    #[test]
    fn test_daos_handle_validation() {
        let invalid = daos_handle_t { cookie: 0 };
        assert!(!is_valid_handle(invalid));

        let valid = daos_handle_t { cookie: 12345 };
        assert!(is_valid_handle(valid));
    }
}
