use std::env;

fn main() {

    // Use env/scripts/install-libsll-debian.sh to install Libressl

    let mode = env::var("zyn_linkage").unwrap_or("dylib".to_owned());
    if mode == "static" {
        println!("cargo:rustc-link-search=/usr/lib/");
        for lib in &["crypto", "ssl", "tls"] {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    } else if mode == "dylib" {
        println!("cargo:rustc-link-lib=dylib=tls");
    } else {
        panic!("Undefined linkage");
    }
}
