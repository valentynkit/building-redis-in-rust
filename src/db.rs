use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Eq, PartialEq)]
pub struct Value {
    value: String,
}

#[derive(Eq, Hash, PartialEq, Clone)]
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
    expires: HashMap<Key, Duration>,
    start_ms: Instant,
    realtime_ms: Duration,
}

impl Db {
    pub fn create(start_ms: Instant, realtime_ms: Duration) -> Self {
        Db {
            keyspace: HashMap::new(),
            expires: HashMap::new(),
            start_ms,
            realtime_ms,
        }
    }
    pub fn update_time(&mut self, realtime_ms: Duration) {
        self.realtime_ms = realtime_ms;
    }

    pub fn realtime_ms(&self) -> Duration {
        self.realtime_ms
    }

    pub fn remove(&mut self, key: &Key) {
        self.keyspace.remove(key);
        self.expires.remove(key);
    }

    // TODO: Add lazy expiration here.
    pub fn get(&self, key: &Key) -> Option<&Value> {
        self.keyspace.get(&key)
    }

    fn set(&mut self, key: Key, value: Value) -> Option<Value> {
        self.keyspace.insert(key, value)
    }

    pub fn setex(&mut self, key: Key, value: Value, ex_at: Option<Duration>) -> Option<Value> {
        if let Some(ex) = ex_at {
            self.expires.insert(key.clone(), ex);
        }

        self.set(key, value)
    }
}
