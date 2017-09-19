use gpgme_sys;
use libc;
use libgpg_error_sys;
use std::ffi::{ CString };
use std::ffi;
use std::ptr;
use std::result::{ Result };
use std::sync::{ Once, ONCE_INIT };
use std::vec;

pub struct Context {
    context: gpgme_sys::gpgme_ctx_t,
    key: gpgme_sys::gpgme_key_t,
}

#[derive(Clone)]
pub struct Crypto {
    key_fingerprint: String,
}


fn _create_empty_data() -> Result<gpgme_sys::gpgme_data_t, ()> {
    let mut data: gpgme_sys::gpgme_data_t = ptr::null_mut();
    unsafe {
        match gpgme_sys::gpgme_data_new(& mut data) {
            libgpg_error_sys::GPG_ERR_NO_ERROR => (),
            error => {
                warn!("Failed to create empty data: {}", _error_string(error));
                return Err(())
            }
        }
    }
    return Ok(data)
}

fn _create_data_from_buffer(buffer: & Vec<u8>) -> Result<gpgme_sys::gpgme_data_t, ()> {
    let mut data: gpgme_sys::gpgme_data_t = ptr::null_mut();
    unsafe {
        match gpgme_sys::gpgme_data_new_from_mem(
            & mut data,
            buffer.as_ptr() as * mut i8,
            buffer.len() as u64,
            0) {
            libgpg_error_sys::GPG_ERR_NO_ERROR => (),
            error => {
                warn!("Failed to create data from buffer: {}", _error_string(error));
                return Err(())
            }
        }
    }
    Ok(data)
}

fn _copy_data_to_buffer(data: gpgme_sys::gpgme_data_t) -> Vec<u8> {
    unsafe {
        let data_size: u64 = gpgme_sys::gpgme_data_seek(data, 0, libc::SEEK_END) as u64;
        gpgme_sys::gpgme_data_seek(data, 0, libc::SEEK_SET);

        let mut buffer: Vec<u8> = vec![0; data_size as usize];
        gpgme_sys::gpgme_data_read(data, buffer.as_mut_ptr() as * mut _, data_size);
        buffer
    }
}

fn _error_string(error_code: u32) -> String {
    unsafe {
        return String::from(ffi::CStr::from_ptr(libgpg_error_sys::gpg_strerror(error_code)).to_str().unwrap());
    }
}

static INIT_GUARD: Once = ONCE_INIT;
impl Crypto {

    pub fn new(key_fingerprint: String) -> Result<Crypto, ()> { // todo: take & str instead of string
        INIT_GUARD.call_once( || {
            unsafe {
                let lib_version = ffi::CStr::from_ptr(gpgme_sys::gpgme_check_version(ptr::null_mut()));
                info!("Using gpgme version {}", lib_version.to_str().unwrap());
            }
        });

        Ok(Crypto{
            key_fingerprint: key_fingerprint,
        })
    }

    pub fn create_context(& self) -> Result<Context, ()> {
        let mut ctx: gpgme_sys::gpgme_ctx_t = ptr::null_mut();
        let mut key: gpgme_sys::gpgme_key_t = ptr::null_mut();

        unsafe {
            match gpgme_sys::gpgme_new(& mut ctx) {
                libgpg_error_sys::GPG_ERR_NO_ERROR => {},
                error => {
                    error!("Failed to create gpgme context: {}", _error_string(error));
                    return Err(())
                }
            }

            match gpgme_sys::gpgme_ctx_set_engine_info(
                ctx,
                gpgme_sys::GPGME_PROTOCOL_OpenPGP,
                ptr::null(),
                ptr::null()) {

                libgpg_error_sys::GPG_ERR_NO_ERROR => (),
                error => {
                    error!("Failed to set gpgme context info: {}", _error_string(error));
                    return Err(())
                }
            }

            match gpgme_sys::gpgme_get_key(
                ctx,
                CString::new(self.key_fingerprint.clone()).unwrap().as_ptr(),
                & mut key,
                0) {
                libgpg_error_sys::GPG_ERR_NO_ERROR => (),
                _ => {
                    error!("Specified gpgme key not found");
                    return Err(())
                }
            }
        }

        Ok(Context {
            context: ctx,
            key: key
        })
    }
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn encrypt(& self, data: & Vec<u8>) -> Result<Vec<u8>, ()> {

        let data_plain = match _create_data_from_buffer(data) {
            Ok(data) => data,
            Err(()) => {
                return Err(());
            }
        };

        let data_encrypted = match _create_empty_data() {
            Ok(data) => data,
            Err(()) => {
                return Err(());
            }
        };

        let mut keys: vec::Vec<_> = vec![self.key, ptr::null_mut()];
        unsafe {
            match gpgme_sys::gpgme_op_encrypt(
                self.context,
                keys.as_mut_ptr(),
                0,
                data_plain,
                data_encrypted) {
                libgpg_error_sys::GPG_ERR_NO_ERROR => (),
                error => {
                    warn!("failed to encrypt: {}", _error_string(error));
                    return Err(());
                }
            }
        }
        Ok(_copy_data_to_buffer(data_encrypted))
    }

    pub fn decrypt(& self, encrypted: & Vec<u8>) -> Result<Vec<u8>, ()> {
        let data_encrypted = match _create_data_from_buffer(encrypted) {
            Ok(data) => data,
            Err(()) => {
                return Err(());
            }
        };

        let data_plain = match _create_empty_data() {
            Ok(data) => data,
            Err(()) => {
                return Err(());
            }
        };

        unsafe {
            match gpgme_sys::gpgme_op_decrypt(self.context, data_encrypted, data_plain) {
                libgpg_error_sys::GPG_ERR_NO_ERROR => (),
                error => {
                    warn!("failed to decrypt data: {}", _error_string(error));
                    return Err(());
                }
            }
        }
        Ok(_copy_data_to_buffer(data_plain))
    }
}
