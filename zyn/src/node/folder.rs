use std::slice::{ Iter };
use std::vec::{ Vec };

use node::common::{ NodeId, Timestamp, utc_timestamp };
use node::user_authority::{ Id };

pub struct Child {
    pub node_id: NodeId,
    pub name: String,
}

pub struct Folder {
    children: Vec<Child>,
    parent: NodeId,  // todo: This may be problem for root, should be optional
    created: Timestamp,
    modified: Timestamp,
    read: Id,
    write: Id,
}

impl Folder {
    pub fn create(user: Id, parent: NodeId) -> Folder {
        let ts = utc_timestamp();
        Folder {
            children: Vec::with_capacity(5),
            parent: parent,
            created: ts.clone(),
            modified: ts,
            read: user.clone(),
            write: user,
        }
    }

    pub fn from(parent: NodeId, created: Timestamp, modified: Timestamp, read: Id, write: Id) -> Folder {
        Folder {
            children: Vec::with_capacity(5),
            parent: parent,
            created: created,
            modified: modified,
            read: read,
            write: write,
        }
    }

    pub fn parent(& self) -> NodeId { self.parent }
    pub fn created(& self) -> Timestamp { self.created }
    pub fn modified(& self) -> Timestamp { self.modified }
    pub fn read(& self) -> & Id { & self.read }
    pub fn write(& self) -> & Id { & self.write }

    pub fn number_of_children(& self) -> usize {
        self.children.len()
    }

    pub fn is_empty(& self) -> bool {
        self.children.is_empty()
    }

    pub fn remove_child(& mut self, index: usize, node_id: & NodeId)
                  -> Result<(), ()> {

        if self.children[index].node_id == *node_id {
            self.children.remove(index);
            return Ok(())
        }
        Err(())
    }

    pub fn add_child(& mut self, node_id: NodeId, name: & str) {
        self.children.push(Child {
            node_id: node_id,
            name: String::from(name),
        });
    }

    pub fn child_with_name(& self, name: & str) -> Result<(NodeId, usize), ()> {
        for (i, iter) in self.children.iter().enumerate() {
            if iter.name == name {
                return Ok((iter.node_id, i))
            }
        }
        Err(())
    }

    pub fn child_with_node_id(& self, node_id: & NodeId) -> Result<usize, ()> {
        for (i, iter) in self.children.iter().enumerate() {
            if iter.node_id == *node_id {
                return Ok(i)
            }
        }
        Err(())
    }

    pub fn children(& self) -> Iter<Child> {
        self.children.iter()
    }
}
