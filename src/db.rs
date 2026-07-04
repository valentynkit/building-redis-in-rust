use core::{fmt, time};
use std::{
    borrow::Borrow,
    collections::{HashMap, VecDeque},
    mem,
    os::fd::RawFd,
    time::{Duration, Instant},
};

use tracing::{debug, info};

#[derive(Eq, Default, Debug, PartialEq)]
pub struct Value {
    value: Vec<u8>,
}

#[derive(Eq, Default, Debug, Hash, PartialEq, Clone)]
pub struct Key {
    value: Vec<u8>,
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Self { value }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self { value }
    }
}

impl From<&Vec<u8>> for Key {
    fn from(value: &Vec<u8>) -> Self {
        Self {
            value: value.clone(),
        }
    }
}

impl From<&Vec<u8>> for Value {
    fn from(value: &Vec<u8>) -> Self {
        Self {
            value: value.clone(),
        }
    }
}
impl From<Key> for Vec<u8> {
    fn from(value: Key) -> Vec<u8> {
        value.value
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Vec<u8> {
        value.value
    }
}

impl From<&Value> for Vec<u8> {
    fn from(value: &Value) -> Vec<u8> {
        value.value.clone()
    }
}
impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.value
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.value))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.value))
    }
}

pub struct Db {
    keyspace: HashMap<Key, Value>,
    lists: HashMap<Key, VecDeque<Value>>,
    expires: HashMap<Key, Duration>,
    // TODO: maybe extend VecDequeu<(RawFd, Duration) and than in server we could compare current
    // time, if timeout, we could send null or whatever response to this client.
    waiters: HashMap<Key, VecDeque<(RawFd, Option<Duration>)>>,
    outbox: Vec<Key>,
    start_ms: Instant,
    realtime_ms: Duration,
}

impl Db {
    pub fn create(start_ms: Instant, realtime_ms: Duration) -> Self {
        debug!("db initialized");
        Db {
            keyspace: HashMap::new(),
            expires: HashMap::new(),
            lists: HashMap::new(),
            waiters: HashMap::new(),
            outbox: Vec::new(),
            start_ms,
            realtime_ms,
        }
    }

    // TODO: consider refactoring for better lifetimes and optimizing to avoid using clone
    pub fn handle_waiters(&mut self) -> HashMap<RawFd, Option<(Key, Value)>> {
        let mut out: HashMap<RawFd, Option<(Key, Value)>> = HashMap::new();
        // cleanup timeout waiters
        self.waiters.retain(|_key, waiters| {
            waiters.retain(|(fd, timeout)| {
                let is_expired = timeout.is_some_and(|timeout| timeout <= self.realtime_ms);
                if is_expired {
                    out.insert(*fd, None);
                }
                !is_expired
            });
            !waiters.is_empty()
        });

        let outbox = mem::take(&mut self.outbox);
        for key in &outbox {
            // Defence in Depth
            while let Some(list) = self.lists.get_mut(key)
                && !list.is_empty()
                && let Some(waiters) = self.waiters.get_mut(key)
                && !waiters.is_empty()
            {
                let (fd, _timeout) = waiters
                    .pop_front()
                    .expect("queue is guaranteed by the is_empty check before");
                let item = list
                    .pop_front()
                    .expect("value is guaranteed by the is_empty check before");
                out.insert(fd, Some((key.clone(), item)));
            }
        }
        out
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
        let list = self.lists.entry(key.clone()).or_default();
        elems.into_iter().for_each(|e| list.push_front(e));

        if self.waiters.contains_key(&key) {
            info!(%key, "adding outbox");
            self.outbox.push(key);
        }

        list.len() as i64
    }

    pub fn list_len(&self, key: Key) -> i64 {
        let list = self.lists.get(&key);

        list.map_or(0, |list| list.len() as i64)
    }

    pub fn blpop(&mut self, key: Key, timeout: Option<Duration>, cur_fd: RawFd) -> Option<Value> {
        // TODO: could be refactored
        if let Some(list) = self.lists.get_mut(&key)
            && let Some(item) = list.pop_front()
        {
            return Some(item);
        }

        info!(%key, "adding waiter");
        let waiters = self.waiters.entry(key).or_default();
        waiters.push_back((cur_fd, timeout));
        None
    }

    pub fn list_append(&mut self, key: Key, elems: Vec<Value>) -> i64 {
        let list = self.lists.entry(key.clone()).or_default();
        list.extend(elems);

        if self.waiters.contains_key(&key) {
            self.outbox.push(key);
        }
        list.len() as i64
    }

    pub fn list_pop(&mut self, key: &Key, len: usize) -> Vec<Value> {
        let mut out: Vec<Value> = vec![];
        let Some(list) = self.lists.get_mut(key) else {
            return out;
        };

        let len = len.min(list.len() - 1);
        for _ in 0..len {
            if let Some(item) = list.pop_front() {
                out.push(item);
            }
        }
        out
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
        let to = to.min(len - 1);
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
