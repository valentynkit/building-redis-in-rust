use core::fmt;
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap, VecDeque},
    mem,
    time::{Duration, Instant},
};

type Stream = BTreeMap<(u64, u64), Vec<(Key, Value)>>;
type Waiters = VecDeque<(ClientId, Option<Duration>)>;

use tracing::{debug, info};
pub enum Object {
    String(Value),
    List(VecDeque<Value>),
    Stream(Stream),
}

impl Object {
    fn type_name(&self) -> &'static str {
        match self {
            Object::String(_) => "string",
            Object::List(_) => "list",
            Object::Stream(_) => "stream",
        }
    }
}

use crate::{client::ClientId, command::common::CommandError};
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
    keyspace: HashMap<Key, Object>,
    expires: HashMap<Key, Duration>,
    // TODO: maybe extend VecDequeu<(ClientId, Duration) and than in server we could compare current
    // time, if timeout, we could send null or whatever response to this client.
    waiters: HashMap<Key, Waiters>,
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
            waiters: HashMap::new(),
            outbox: Vec::new(),
            start_ms,
            realtime_ms,
        }
    }
    fn as_string(&mut self, key: &Key) -> Result<Option<&Value>, CommandError> {
        if self.expire_clean(key) {
            return Ok(None);
        }
        match self.keyspace.get(key) {
            None => Ok(None),
            Some(Object::String(value)) => Ok(Some(value)),
            Some(_) => Err(CommandError::WrongType("string".into())),
        }
    }

    fn as_list(&mut self, key: &Key) -> Result<Option<&VecDeque<Value>>, CommandError> {
        if self.expire_clean(key) {
            return Ok(None);
        }
        match self.keyspace.get(key) {
            None => Ok(None),
            Some(Object::List(list)) => Ok(Some(list)),
            Some(_) => Err(CommandError::WrongType("list".into())),
        }
    }
    fn as_stream(&mut self, key: &Key) -> Result<Option<&Stream>, CommandError> {
        if self.expire_clean(key) {
            return Ok(None);
        }
        match self.keyspace.get(key) {
            None => Ok(None),
            Some(Object::Stream(stream)) => Ok(Some(stream)),
            Some(_) => Err(CommandError::WrongType("stream".into())),
        }
    }
    fn list_or_create(&mut self, key: &Key) -> Result<&mut VecDeque<Value>, CommandError> {
        todo!()
    }

    fn stream_or_create(&mut self, key: &Key) -> Result<&mut Stream, CommandError> {
        todo!()
    }
    // TODO: consider refactoring for better lifetimes and optimizing to avoid using clone
    pub fn handle_waiters(
        &mut self,
    ) -> Result<(HashMap<ClientId, Option<(Key, Value)>>, Option<Duration>), CommandError> {
        let date_now = self.realtime_ms();
        let mut nearest_deadline: Option<Duration> = None;
        let mut out: HashMap<ClientId, Option<(Key, Value)>> = HashMap::new();
        // cleanup timeout waiters
        self.waiters.retain(|_key, waiters| {
            waiters.retain(|(client_id, timeout)| {
                let mut is_expired: bool = false;
                if let Some(value) = timeout {
                    is_expired = *value <= date_now;
                    if is_expired {
                        out.insert(*client_id, None);
                    } else {
                        let deadline = *value - date_now;
                        nearest_deadline =
                            Some(nearest_deadline.map_or(deadline, |cur| cur.min(deadline)));
                    }
                }
                !is_expired
            });
            !waiters.is_empty()
        });

        let outbox = mem::take(&mut self.outbox);
        for key in &outbox {
            // Defence in Depth
            while let Some(list) = self.as_list(key)?
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
        Ok((out, nearest_deadline))
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

    pub fn list_prepand(&mut self, key: Key, elems: Vec<Value>) -> Result<i64, CommandError> {
        let list = self.list_or_create(&key)?;
        elems.into_iter().for_each(|e| list.push_front(e));

        if self.waiters.contains_key(&key) {
            info!(%key, "adding outbox");
            self.outbox.push(key);
        }

        Ok(list.len() as i64)
    }

    pub fn list_len(&mut self, key: Key) -> Result<i64, CommandError> {
        let list = self.as_list(&key)?;

        Ok(list.map_or(0, |list| list.len() as i64))
    }

    pub fn blpop(
        &mut self,
        key: Key,
        timeout: Option<Duration>,
        cur_client: ClientId,
    ) -> Result<Option<Value>, CommandError> {
        // TODO: could be refactored
        if let Some(list) = self.as_list(&key)?
            && let Some(item) = list.pop_front()
        {
            return Ok(Some(item));
        }

        info!(%key, "adding waiter");
        let waiters = self.waiters.entry(key).or_default();
        let deadline = timeout.map(|t| self.realtime_ms + t);
        waiters.push_back((cur_client, deadline));
        Ok(None)
    }

    pub fn list_append(&mut self, key: Key, elems: Vec<Value>) -> Result<i64, CommandError> {
        let list = self.list_or_create(&key)?;
        list.extend(elems);

        if self.waiters.contains_key(&key) {
            self.outbox.push(key);
        }
        Ok(list.len() as i64)
    }

    pub fn list_pop(&mut self, key: &Key, len: usize) -> Result<Vec<Value>, CommandError> {
        let mut out: Vec<Value> = vec![];
        let Some(list) = self.as_list(key)? else {
            return Ok(out);
        };

        let len = len.min(list.len() - 1);
        for _ in 0..len {
            if let Some(item) = list.pop_front() {
                out.push(item);
            }
        }
        Ok(out)
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
