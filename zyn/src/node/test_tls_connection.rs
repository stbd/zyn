use crate::node::tls_connection::{ TlsServer };
use crate::node::test_util::{ certificate_paths, init_logging };

#[test]
fn test_binding_to_local_addres_and_accepting_returning_none() {
    init_logging();
    let (cert, key) = certificate_paths();
    let server = TlsServer::new(
        & "127.0.0.1",
        4433,
        key.as_path(),
        cert.as_path()).unwrap();

    assert!(server.accept().unwrap().is_none())
}
