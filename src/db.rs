use std::{
    collections::{HashMap, VecDeque},
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
    lists: HashMap<Key, VecDeque<Value>>,
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
    pub fn list_prepand(&mut self, key: Key, elems: Vec<Value>) -> i64 {
        let list = self.lists.entry(key).or_default();
        elems.into_iter().for_each(|e| list.push_front(e));
        list.len() as i64
    }

    pub fn list_len(&self, key: Key) -> i64 {
        let list = self.lists.get(&key);

        list.map_or(0, |list| list.len() as i64)
    }

    pub fn list_pop(&mut self, key: &Key) -> Option<Value> {
        let list = self.lists.get_mut(key);
        list.map_or(None, |list| list.pop_front())
    }
    pub fn list_append(&mut self, key: Key, elems: Vec<Value>) -> i64 {
        let list = self.lists.entry(key).or_default();
        list.extend(elems);
        list.len() as i64
    }

    // TODO: potential improvements:
    // 1: when there are 3 elements, but range is 0-9 redis will not
    // error but return all the existing elements in this range
    // 2: Also we currently don't distinquish between the case when the key itself is missing, and when
    // the key has no elements
    pub fn list_get(&self, key: Key, mut from: i32, mut to: i32) -> Vec<&Value> {
        let Some(l) = self.lists.get(&key) else {
            return Vec::new();
        };
        let len = l.len() as i32;
        if l.is_empty() {
            return Vec::new();
        }
        if from < 0 {
            from = len as i32 + from;
            if from < 0 {
                from = 0;
            }
        }
        if to < 0 {
            to = len as i32 + to;
            if to < 0 || to < from {
                return Vec::new();
            }
        }
        l.range(from as usize..=to as usize).collect()
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
