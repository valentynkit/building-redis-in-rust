use std::{collections::HashMap, time::Instant};

#[derive(Eq, PartialEq)]
pub struct Value {
    value: String,
}

#[derive(Eq, Hash, PartialEq)]
pub struct Key {
    value: String,
}

impl From<&Vec<u8>> for Key {
    fn from(value: &Vec<u8>) -> Self {
        Self {
            value: String::from_utf8_lossy(value).into_owned(),
        }
    }
}

impl From<&Vec<u8>> for Value {
    fn from(value: &Vec<u8>) -> Self {
        Self {
            value: String::from_utf8_lossy(value).into_owned(),
        }
    }
}

// The reverse direction — separate impl, From doesn't generate it for you.
impl From<&Value> for Vec<u8> {
    fn from(value: &Value) -> Self {
        value.value.as_bytes().to_vec()
    }
}

pub struct Db {
    keyspace: HashMap<Key, Value>,
    expires: HashMap<Key, Instant>,
    start_ms: Instant,
}

impl Db {
    pub fn create(start_ms: Instant) -> Self {
        Db {
            keyspace: HashMap::new(),
            expires: HashMap::new(),
            start_ms,
        }
    }

    pub fn remove(&mut self, key: &Key) {
        self.keyspace.remove(key);
        self.expires.remove(key);
    }

    // TODO: Add lazy expiration here.
    pub fn get(&self, key: &Key) -> Option<&Value> {
        self.keyspace.get(&key)
    }

    pub fn set(&mut self, key: Key, value: Value) -> Option<Value> {
        self.keyspace.insert(key, value)
    }
}
