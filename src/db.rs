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
    lists: HashMap<Key, Vec<Value>>,
    expires: HashMap<Key, Duration>,
    start_ms: Instant,
    realtime_ms: Duration,
}

impl Db {
    pub fn create(start_ms: Instant, realtime_ms: Duration) -> Self {
        Db {
            keyspace: HashMap::new(),
            expires: HashMap::new(),
            lists: HashMap::new(),
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

    pub fn list_upsert(&mut self, key: Key, elems: Vec<Value>) -> i64 {
        let list = self.lists.entry(key).or_default();
        list.extend(elems);
        list.len() as i64
    }

    // TODO: potential improvements:
    // 1: when there are 3 elements, but range is 0-9 redis will not
    // error but return all the existing elements in this range
    // 2: Also we currently don't distinquish between the case when the key itself is missing, and when
    // the key has no elements
    pub fn list_get(&self, key: Key, from: usize, to: usize) -> &[Value] {
        self.lists
            .get(&key)
            .and_then(|l| {
                if l.is_empty() {
                    return None;
                }
                let to = to.min(l.len() - 1);
                l.get(from..=to)
            })
            .unwrap_or_default()
    }
    // Lazy Epiration
    fn expire_clean(&mut self, key: &Key) -> bool {
        let is_expired = self
            .expires
            .get(key)
            .is_some_and(|&exp| exp <= self.realtime_ms);

        if is_expired {
            self.remove(key);
        }

        is_expired
    }

    pub fn get(&mut self, key: &Key) -> Option<&Value> {
        if self.expire_clean(key) {
            return None;
        }
        self.keyspace.get(key)
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
