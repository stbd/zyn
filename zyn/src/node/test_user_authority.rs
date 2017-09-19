use chrono::{ DateTime, UTC, NaiveDateTime };

use node::test_util::tempdir::{ TempDir };
use node::user_authority::{ UserAuthority };
use node::common::{ Timestamp };
use node::test_util;

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
            auth: UserAuthority::new("salt"),
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
        let dt = DateTime::<UTC>::from_utc(NaiveDateTime::from_timestamp(seconds, 0), UTC);
        dt.timestamp()
    }

    pub fn username() -> String {
        String::from("username")
    }

    pub fn password() -> String {
        String::from("password")
    }

    pub fn groupname() -> String {
        String::from("group name")
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
fn test_user_is_member_of_group() {
    let mut state = State::new();
    let group_id = state.auth.add_group(& State::groupname(), None).unwrap();
    let user_id = state.auth.add_user(& State::username(), & State::password(), None).unwrap();
    assert!(state.auth.modify_group_add_user(& group_id, & user_id, State::datetime_plus(1)).is_ok());
    assert!(state.auth.is_authorized(& group_id, & user_id, State::datetime_plus(1)).is_ok());
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
