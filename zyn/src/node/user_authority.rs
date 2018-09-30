use std::fmt::{ Display, Formatter, Result as FmtResult} ;
use std::collections::{ HashMap, HashSet };
use std::path::{ PathBuf };
use std::result::{ Result };
use std::string::{ String };
use std::vec::{ Vec };

use sha2::{ Sha256, Digest };
use rand::{ random };

use node::crypto::{ Context };
use node::common::{ Timestamp };
use node::serialize::{ SerializedUserAuthority };

#[derive(PartialEq, Eq, Hash, Clone)]
pub enum Id {
    User(u64),
    Group(u64),
}

impl Id {
    fn id(& self) -> u64 {
        match *self {
            Id::User(id) => id,
            Id::Group(id) => id,
        }
    }

    pub fn state(& self) -> (u8, u64) {
        match *self {
            Id::User(id) => (1, id),
            Id::Group(id) => (2, id),
        }
    }

    pub fn from_state(state: (u8, u64)) -> Result<Id, ()> {
        let (t, v) = state;
        match t {
            1 => Ok(Id::User(v)),
            2 => Ok(Id::Group(v)),
            _ => Err(()),
        }
    }
}

impl Display for Id {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        match *self {
            Id::User(id) => write!(f, "User:{}", id),
            Id::Group(id) => write!(f, "Group:{}", id),
        }
    }
}

struct Group {
    name: String,
    expiration: Option<Timestamp>,
    members: HashSet<Id>,
}

struct User {
    salt: u64,
    name: String,
    expiration: Option<Timestamp>,
    password: Vec<u8>,
}

pub struct UserAuthority {
    groups: HashMap<u64, Group>,
    users: HashMap<u64, User>,
    next_user_id: u64,
    next_group_id: u64,
}

impl UserAuthority {
    pub fn new() -> UserAuthority {
        UserAuthority {
            groups: HashMap::new(),
            users: HashMap::new(),
            next_user_id: 1,
            next_group_id: 1,
        }
    }

    pub fn resolve_id_name(& self, id: & Id) -> Result<String, ()> { // todo: Rename resolve_username
        match *id {
            Id::User(user_id) => {
                self.users.get(& user_id)
                    .ok_or(())
                    .and_then(| element | Ok(element.name.clone()))
            },
            Id::Group(group_id) => {
                self.groups.get(& group_id)
                    .ok_or(())
                    .and_then(| element | Ok(element.name.clone()))
            },
        }
    }

    pub fn resolve_user_id(& self, name: & str) -> Result<Id, ()> {
        self.users
            .iter()
            .find(| & (_, user) | user.name == name)
            .map(| (& id, _) | Id::User(id))
            .ok_or(())
    }

    pub fn resolve_group_id(& self, name: & str) -> Result<Id, ()> {
        self.groups
            .iter()
            .find(| & (_, group) | group.name == name)
            .map(| (& id, _) | Id::Group(id))
            .ok_or(())
    }

    pub fn store(& self, crypto_context: Context, path_basename: & PathBuf)
                 -> Result<(), ()> {

        let mut state = SerializedUserAuthority::new(
            self.next_user_id,
            self.next_group_id
        );

        for (id, user) in self.users.iter() {
            state.add_user(id.clone(), user.salt.clone(), user.name.clone(), user.password.clone(), user.expiration);
        }

        for (id, group) in self.groups.iter() {
            let members: Vec<(u8, u64)> = group.members.iter().map(| member | member.state()).collect();
            state.add_group(id.clone(), group.name.clone(), members, group.expiration);
        }

        state.write(crypto_context, & path_basename)
    }

    pub fn load(crypto_context: Context, path_basename: & PathBuf)
                -> Result<UserAuthority, ()> {

        let state = SerializedUserAuthority::read(crypto_context, & path_basename)
            ? ;
        let (next_user_id, next_group_id) = state.state();

        let mut auth = UserAuthority::new();
        auth.next_user_id = next_user_id;
        auth.next_group_id = next_group_id;

        for & (ref id, ref salt, ref name, ref password, ref serialized_expiration) in state.users_iter() {
            auth.users.insert(
                id.clone(),
                User {
                    salt: salt.clone(),
                    name: name.clone(),
                    expiration: serialized_expiration.clone(),
                    password: password.clone(),
                }
            );
        }

        for & (ref id, ref name, ref serialized_members, ref serialized_expiration) in state.groups_iter() {
            let mut members = HashSet::<Id>::with_capacity(serialized_members.len());
            for state in serialized_members.iter() {
                let id_ = Id::from_state(*state)
                    .map_err(| () | error!("Failed to parse memeber id for group id {}", id))
                    ? ;
                members.insert(id_);
            }

            auth.groups.insert(
                id.clone(),
                Group {
                    name: name.clone(),
                    expiration: serialized_expiration.clone(),
                    members: members,
                }
            );
        }

        Ok(auth)
    }

    pub fn configure_admin_group(
        & mut self,
        group: & Id,
        name: & str,
    ) -> Result<(), ()> {
        if let & Id::Group(id) = group {
            self.groups.insert(
                id,
                Group {
                    name: String::from(name),
                    expiration: None,
                    members: HashSet::new(),
                }
            );
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn add_group(& mut self, name: & str, expiration: Option<Timestamp>)
                     -> Result<Id, ()> {

        for (_, group) in self.groups.iter() {
            if group.name == name {
                return Err(());

            }
        }

        let id = self.next_group_id;
        self.next_group_id += 1;
        self.groups.insert(
            id,
            Group {
                name: String::from(name),
                expiration: expiration,
                members: HashSet::new(),
            }
        );

        Ok(Id::Group(id))
    }

    pub fn modify_group_expiration(
        & mut self,
        group_id: & Id,
        expiration: Option<Timestamp>,
    ) -> Result<(), ()> {

        if let & Id::User(_) = group_id {
            return Err(());
        }

        let ref mut group = self.groups.get_mut(& group_id.id())
            .ok_or(())
            ? ;

        group.expiration = expiration;

        Ok(())
    }

    pub fn modify_group_remove_user(
        & mut self,
        group_id: & Id,
        user_id: & Id,
        _current_time: Timestamp
    ) -> Result<(), ()> {

        if let & Id::User(_) = group_id {
            return Err(());
        }
        if let & Id::Group(_) = user_id {
            return Err(());
        }

        let ref mut group = self.groups.get_mut(& group_id.id())
            .ok_or(())
            ? ;

        if group.members.remove(user_id) {
            return Ok(());
        }
        Err(())
    }

    pub fn modify_group_add_user(
        & mut self,
        group_id: & Id,
        user_id: & Id,
        _current_time: Timestamp
    ) -> Result<(), ()> {

        if let & Id::User(_) = group_id {
            return Err(());
        }
        if let & Id::Group(_) = user_id {
            return Err(());
        }

        let ref mut group = self.groups.get_mut(& group_id.id())
            .ok_or(())
            ? ;

        if group.members.insert((*user_id).clone()) {
            return Ok(());
        }
        Err(())
    }

    fn hash(& self, content: & str, salt: u64) -> Vec<u8> {
        let mut hasher = Sha256::default();

        hasher.input(content.as_bytes());
        hasher.input(format!("{}", salt).as_bytes());

        let result = hasher.result();
        let length = result.as_slice().len();
        let mut buffer: Vec<u8> = vec![0; length];
        buffer.clone_from_slice(result.as_slice());
        buffer
    }

    pub fn add_user(
        & mut self,
        name: & str,
        password: & str,
        expiration: Option<Timestamp>
    ) -> Result<Id, ()> {

        for (_, user) in self.users.iter() {
            if user.name == name {
                return Err(());
            }
        }

        let id = self.next_user_id;
        self.next_user_id += 1;

        let salt = UserAuthority::salt();
        let hashed_password = self.hash(password, salt);
        self.users.insert(
            id,
            User {
                salt: salt,
                name: String::from(name),
                expiration: expiration,
                password: hashed_password,
           }
        );

        Ok(Id::User(id))
    }

    pub fn modify_user_expiration(
        & mut self,
        id: & Id,
        expiration: Option<Timestamp>
    ) -> Result<(), ()> {

        if let & Id::Group(_) = id {
            return Err(());
        }

        let ref mut user = self.users.get_mut(& id.id()).ok_or(()) ? ;
        user.expiration = expiration;
        Ok(())
    }

    pub fn modify_user_password(
        & mut self,
        id: & Id,
        password: & str
    ) -> Result<(), ()> {

        if let & Id::Group(_) = id {
            return Err(());
        }

        let salt = UserAuthority::salt();
        let hashed_password = self.hash(password, salt);
        let ref mut user = self.users.get_mut(& id.id()).ok_or(()) ? ;
        user.password = hashed_password;
        user.salt = salt;
        Ok(())
    }

    pub fn is_authorized(
        & self,
        authority: & Id,
        tested: & Id,
        current_time: Timestamp
    ) -> Result<(), ()> {

        // Group is not allowed to be member of user
        if let (& Id::User(_), & Id::Group(_)) = (authority, tested) {
            return Err(());
        }

        // If both are users, test if they are equal
        if let (& Id::User(id_authority), & Id::User(id_tested)) = (authority, tested) {
            if id_authority == id_tested {
                return Ok(());
            }
            return Err(());
        }

        // Test if part of group
        let ref group = self.groups.get(& authority.id())
            .ok_or(())
            ? ;

        if let Some(ref expiration) = group.expiration {
            if *expiration < current_time {
                return Err(());
            }
        }

        if group.members.contains(tested) {
            return Ok(());
        }
        Err(())
    }

    pub fn validate_user(
        & self,
        name: & str,
        password: & str,
        current_time: Timestamp
    ) -> Result<Id, ()> {

        let (id, user) = self.users
            .iter()
            .find(| & (_, user) | user.name == name)
            .ok_or(())
            ? ;

        if let Some(ref expiration) = user.expiration {
            if *expiration < current_time {
                return Err(());
            }
        }

        let hashed_password = self.hash(password, user.salt);
        if hashed_password != user.password {
            return Err(());
        }

        Ok(Id::User(id.clone()))
    }

    fn salt() -> u64 {
        random::<u64>()
    }
}
