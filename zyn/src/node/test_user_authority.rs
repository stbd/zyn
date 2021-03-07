use chrono::{ DateTime, Utc, NaiveDateTime };

use crate::node::test_util::tempdir::{ TempDir };
use crate::node::user_authority::{ UserAuthority };
use crate::node::common::{ Timestamp };
use crate::node::test_util;

struct State {
    auth: UserAuthority,
    dir: TempDir,
}

use std::mem::swap;
impl State {
    pub fn new() -> State {
        test_util::init_logging();

        State {
            dir: test_util::create_temp_folder(),
            auth: UserAuthority::new(),
        }
    }

    pub fn serialize_deserailize(& mut self) -> UserAuthority {
        let path = self.dir.path().to_path_buf().join("users");
        assert!(self.auth.store(test_util::create_crypto_context(), & path).is_ok());
        let mut handle = UserAuthority::load(test_util::create_crypto_context(), & path).unwrap();
        swap(& mut self.auth, & mut handle);
        handle
    }

    pub fn datetime_plus(seconds: Timestamp) -> Timestamp {
        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(seconds, 0), Utc);
        dt.timestamp()
    }

    pub fn username() -> String {
        String::from("username")
    }

    pub fn username_2() -> String {
        String::from("username-2")
    }

    pub fn password() -> String {
        String::from("password")
    }

    pub fn groupname() -> String {
        String::from("group name")
    }

    pub fn groupname_2() -> String {
        String::from("group name 2")
    }
}

#[test]
fn test_validating_user_without_expiration() {
    let mut state = State::new();
    let id_1 = state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    let id_2 = state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(1)).unwrap();
    assert!(id_1 == id_2);
}

#[test]
fn test_validating_user_with_expiration() {
    let mut state = State::new();
    let id_1 = state.auth.add_user(& State::username(), & State::password(), Some(State::datetime_plus(5))).unwrap();
    let id_2 = state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(1)).unwrap();
    assert!(id_1 == id_2);
}

#[test]
fn test_validating_user_when_user_has_expired() {
    let mut state = State::new();
    state.auth.add_user(& State::username(), & State::password(), Some(State::datetime_plus(5))).unwrap();
    assert!(state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(6)).is_err());
}

#[test]
fn test_validating_user_with_incorrect_password() {
    let mut state = State::new();
    state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    assert!(state.auth.validate_user(& State::username(), "wrong password", State::datetime_plus(1)).is_err());
}

#[test]
fn test_modify_user_expiration() {
    let mut state = State::new();
    state.auth.add_user(& State::username(), & State::password(),  Some(State::datetime_plus(-1))).unwrap();
    assert!(state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(0)).is_err());
    let user_id = state.auth.resolve_user_id(& State::username()).unwrap();
    assert!(state.auth.modify_user_expiration(& user_id, Some(State::datetime_plus(5))).is_ok());
    assert!(state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(0)).is_ok());
}

#[test]
fn test_modify_user_password() {
    let new_password = String::from("qwerty");
    let mut state = State::new();
    state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    assert!(state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(0)).is_ok());
    assert!(state.auth.validate_user(& State::username(), & new_password, State::datetime_plus(0)).is_err());
    let user_id = state.auth.resolve_user_id(& State::username()).unwrap();
    assert!(state.auth.modify_user_password(& user_id, & new_password).is_ok());
    assert!(state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(0)).is_err());
    assert!(state.auth.validate_user(& State::username(), & new_password, State::datetime_plus(0)).is_ok());
}

#[test]
fn test_user_is_member_of_group() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let user_id = state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    assert!(state.auth.modify_group_add_user(& group_id, & user_id, State::datetime_plus(1)).is_ok());
    assert!(state.auth.is_authorized(& group_id, & user_id, State::datetime_plus(1)).is_ok());
}

#[test]
fn test_modify_group() {
    let mut state = State::new();
    let user_id = state.auth.add_user(& State::username(), & State::password(),  Some(State::datetime_plus(-1))).unwrap();
    let group_id = state.auth.add_group(& State::groupname(), Some(State::datetime_plus(-5))).unwrap();
    assert!(state.auth.modify_group_add_user(& group_id, & user_id, State::datetime_plus(1)).is_ok());
    assert!(state.auth.is_authorized(& group_id, & user_id, State::datetime_plus(0)).is_err());
    assert!(state.auth.modify_group_expiration(& group_id, Some(State::datetime_plus(5))).is_ok());
    assert!(state.auth.is_authorized(& group_id, & user_id, State::datetime_plus(0)).is_ok());
}

#[test]
fn test_resolve_user() {
    let mut state = State::new();
    let user_id_1 = state.auth.add_user(& State::username(), & State::password(),  Some(State::datetime_plus(-1))).unwrap();
    state.auth.add_user(& State::username_2(), & State::password(),  Some(State::datetime_plus(-1))).unwrap();
    assert!(user_id_1 == state.auth.resolve_user_id(& State::username()).unwrap());
}

#[test]
fn test_resolve_group() {
    let mut state = State::new();
    let group_id_1 = state.auth.add_group(& State::groupname(), Some(State::datetime_plus(-5))).unwrap();
    state.auth.add_group(& State::groupname_2(), Some(State::datetime_plus(-5))).unwrap();
    assert!(group_id_1 == state.auth.resolve_group_id(& State::groupname()).unwrap());
}

#[test]
fn test_serialization() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let user_id = state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    assert!(state.auth.modify_group_add_user(& group_id, & user_id, State::datetime_plus(1)).is_ok());
    assert!(state.auth.is_authorized(& group_id, & user_id, State::datetime_plus(1)).is_ok());
    state.serialize_deserailize();
    assert!(state.auth.validate_user(& State::username(), & State::password(), State::datetime_plus(1)).is_ok());
    assert!(state.auth.is_authorized(& group_id, & user_id, State::datetime_plus(1)).is_ok());
}

#[test]
fn test_temporary_link() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let link = state.auth.generate_temporary_link_for_id(& group_id, State::datetime_plus(1)).unwrap();
    assert!(state.auth.consume_link_to_id(& link, State::datetime_plus(1)).unwrap() == group_id);
}

#[test]
fn test_temporary_link_multiple_links() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let user_id = state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    let link_1 = state.auth.generate_temporary_link_for_id(& group_id, State::datetime_plus(1)).unwrap();
    let link_2 = state.auth.generate_temporary_link_for_id(& user_id, State::datetime_plus(1)).unwrap();
    assert!(state.auth.consume_link_to_id(& link_1, State::datetime_plus(1)).unwrap() == group_id);
    assert!(state.auth.consume_link_to_id(& link_2, State::datetime_plus(1)).unwrap() == user_id);
}

#[test]
fn test_temporary_link_is_usable_only_once() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let link = state.auth.generate_temporary_link_for_id(& group_id, State::datetime_plus(1)).unwrap();
    assert!(state.auth.consume_link_to_id(& link, State::datetime_plus(1)).unwrap() == group_id);
    assert!(state.auth.consume_link_to_id(& link, State::datetime_plus(1)).is_err());
}

#[test]
fn test_temporary_link_is_not_usable_after_expiration() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let link = state.auth.generate_temporary_link_for_id(& group_id, State::datetime_plus(1)).unwrap();
    assert!(state.auth.consume_link_to_id(& link, State::datetime_plus(2)).is_err());
}
