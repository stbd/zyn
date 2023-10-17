use crate::node::socket::{ SocketServer };
use crate::node::test_util::{ init_logging };

#[test]
fn test_binding_to_local_addres_and_accepting_returning_none() {
    init_logging();
    let server = SocketServer::new(
        & "127.0.0.1",
        4433).unwrap();

    assert!(server.accept().unwrap().is_none())
}
