// https://users.rust-lang.org/t/disable-warnings-in-ffi-module-generated-by-bindgen/22571/4
#[allow(non_upper_case_globals)]
#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[allow(unused_attributes)]
pub mod tls;
