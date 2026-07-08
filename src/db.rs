use core::fmt;
use std::ops::Bound::Included;
use std::u64;
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap, VecDeque},
    mem,
    time::{Duration, Instant},
};

// (ms, seq); tuple order == redis id order, so a BTreeMap stays sorted for XRANGE
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct StreamId(u64, u64);
pub type Stream = BTreeMap<StreamId, Vec<(Key, Value)>>;
type Waiters = VecDeque<(ClientId, Option<Duration>)>;

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}
impl StreamId {
    pub fn parse(ms: &str, seq: &str) -> Result<Self, CommandError> {
        let ms: u64 = ms
            .parse()
            .map_err(|_| CommandError::ParseStream(ms.into()))?;
        let seq: u64 = seq
            .parse()
            .map_err(|_| CommandError::ParseStream(seq.into()))?;
        Ok(Self(ms, seq))
    }

    pub fn parse_opt_seq(string: &str) -> Result<Self, CommandError> {
        if string.len() == 1 {
            match string {
                "-" => return Ok(Self(0, 0)),
                "+" => return Ok(Self(u64::MAX, u64::MAX)),
                _ => return Err(CommandError::ParseStream(string.into())),
            }
        }
        if string.contains('-') {
            let Some((ms, seq)) = string.split_once('-') else {
                return Err(CommandError::ParseStream(string.into()));
            };
            return Self::parse(ms, seq);
        }
        let ms: u64 = string
            .parse()
            .map_err(|_| CommandError::ParseStream(string.into()))?;

        Ok(Self(ms, 0))
    }

    pub const fn is_valid(&self, other: &Self) -> bool {
        let ms_greater = self.0 > other.0;
        let ms_equal_and_seq_greater = self.0 == other.0 && self.1 > other.1;
        ms_greater || ms_equal_and_seq_greater
    }
}

// XADD accepts an explicit id, a ms part with an auto-generated sequence
// (<ms>-*), or a fully auto-generated id (*).
pub enum StreamIdSpec {
    Explicit(StreamId),
    AutoSeq(u64),
    Auto,
}

impl StreamIdSpec {
    pub fn parse(string: &str) -> Result<Self, CommandError> {
        if string == "*" {
            return Ok(Self::Auto);
        }

        let Some((ms, seq)) = string.split_once('-') else {
            return Err(CommandError::ParseStream(string.into()));
        };

        if seq == "*" {
            let ms: u64 = ms
                .parse()
                .map_err(|_| CommandError::ParseStream(ms.into()))?;
            return Ok(Self::AutoSeq(ms));
        }

        Ok(Self::Explicit(StreamId::parse(ms, seq)?))
    }
}

impl From<StreamId> for Vec<u8> {
    fn from(value: StreamId) -> Self {
        format!("{0}-{1}", value.0, value.1).into_bytes()
    }
}

impl From<StreamId> for String {
    fn from(value: StreamId) -> Self {
        format!("{0}-{1}", value.0, value.1)
    }
}

impl From<StreamId> for Resp {
    fn from(value: StreamId) -> Self {
        Resp::Bulk(Some(value.into()))
    }
}
use tracing::{debug, info};

// one type per key; the tagged-union equivalent of C's robj + union pointer
pub enum Object {
    String(Value),
    List(VecDeque<Value>),
    Stream(Stream),
}

impl Object {
    pub fn type_name(&self) -> &'static str {
        match self {
            Object::String(_) => "string",
            Object::List(_) => "list",
            Object::Stream(_) => "stream",
        }
    }
}

use crate::{client::ClientId, command::common::CommandError, resp::Resp};
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

impl From<&[u8]> for Key {
    fn from(value: &[u8]) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self {
            value: value.into(),
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

impl From<Key> for Resp {
    fn from(value: Key) -> Self {
        Resp::Bulk(Some(value.into()))
    }
}

impl From<&Key> for Resp {
    fn from(value: &Key) -> Self {
        Resp::Bulk(Some(value.clone().into()))
    }
}

impl From<Value> for Resp {
    fn from(value: Value) -> Self {
        Resp::Bulk(Some(value.into()))
    }
}

impl From<&Value> for Resp {
    fn from(value: &Value) -> Self {
        Resp::Bulk(Some(value.into()))
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

pub struct HandleWaitersResult(
    pub HashMap<ClientId, Option<(Key, Value)>>,
    pub Option<Duration>,
);

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

    // Ok(None) = absent, Ok(Some) = right type, Err = wrong type
    pub fn as_string(&mut self, key: &Key) -> Result<Option<&Value>, CommandError> {
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

    fn as_list_mut(&mut self, key: &Key) -> Result<Option<&mut VecDeque<Value>>, CommandError> {
        if self.expire_clean(key) {
            return Ok(None);
        }
        match self.keyspace.get_mut(key) {
            None => Ok(None),
            Some(Object::List(list)) => Ok(Some(list)),
            Some(_) => Err(CommandError::WrongType("list".into())),
        }
    }

    pub fn as_stream(&mut self, key: &Key) -> Result<Option<&Stream>, CommandError> {
        if self.expire_clean(key) {
            return Ok(None);
        }
        match self.keyspace.get(key) {
            None => Ok(None),
            Some(Object::Stream(stream)) => Ok(Some(stream)),
            Some(_) => Err(CommandError::WrongType("stream".into())),
        }
    }

    // or_insert_with only inserts when absent, so a wrong-type key isn't clobbered
    fn list_or_create(&mut self, key: &Key) -> Result<&mut VecDeque<Value>, CommandError> {
        match self
            .keyspace
            .entry(key.clone())
            .or_insert_with(|| Object::List(VecDeque::new()))
        {
            Object::List(list) => Ok(list),
            _ => Err(CommandError::WrongType("list".into())),
        }
    }

    pub fn stream_or_create(&mut self, key: &Key) -> Result<&mut Stream, CommandError> {
        match self
            .keyspace
            .entry(key.clone())
            .or_insert_with(|| Object::Stream(Stream::new()))
        {
            Object::Stream(stream) => Ok(stream),
            _ => Err(CommandError::WrongType("stream".into())),
        }
    }

    pub fn key_type(&mut self, key: &Key) -> &'static str {
        if self.expire_clean(key) {
            return "none";
        }
        self.keyspace.get(key).map_or("none", Object::type_name)
    }

    // TODO: consider refactoring for better lifetimes and optimizing to avoid using clone
    pub fn handle_waiters(&mut self) -> HandleWaitersResult {
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
            // index keyspace + waiters directly (not as_list_mut) so the two
            // fields borrow disjointly
            loop {
                let Some(Object::List(list)) = self.keyspace.get_mut(key) else {
                    break;
                };
                if list.is_empty() {
                    break;
                }
                let Some(waiters) = self.waiters.get_mut(key) else {
                    break;
                };
                if waiters.is_empty() {
                    break;
                }
                let (fd, _timeout) = waiters
                    .pop_front()
                    .expect("queue is guaranteed by the is_empty check before");
                let item = list
                    .pop_front()
                    .expect("value is guaranteed by the is_empty check before");
                out.insert(fd, Some((key.clone(), item)));
            }
        }
        HandleWaitersResult(out, nearest_deadline)
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
        let len = list.len() as i64; // ends the borrow before we touch self below

        if self.waiters.contains_key(&key) {
            info!(%key, "adding outbox");
            self.outbox.push(key);
        }

        Ok(len)
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
        if let Some(list) = self.as_list_mut(&key)?
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
        let len = list.len() as i64; // ends the borrow before we touch self below

        if self.waiters.contains_key(&key) {
            self.outbox.push(key);
        }
        Ok(len)
    }

    pub fn list_pop(&mut self, key: &Key, len: usize) -> Result<Vec<Value>, CommandError> {
        let mut out: Vec<Value> = vec![];
        let Some(list) = self.as_list_mut(key)? else {
            return Ok(out);
        };

        let len = len.min(list.len());
        for _ in 0..len {
            if let Some(item) = list.pop_front() {
                out.push(item);
            }
        }
        Ok(out)
    }

    // Redis clamps out-of-range indices to the available elements rather than erroring
    // (e.g. LRANGE on a 3-element list with range 0-9 returns all 3, not an error).
    pub fn list_get(
        &mut self,
        key: &Key,
        mut from: i32,
        mut to: i32,
    ) -> Result<Vec<&Value>, CommandError> {
        let Some(l) = self.as_list(key)? else {
            return Ok(Vec::new());
        };
        if l.is_empty() {
            return Ok(Vec::new());
        }
        let len = l.len() as i32;

        if from < 0 {
            from = (len + from).max(0);
        }
        if to < 0 {
            to += len;
        }
        let to = to.min(len - 1);

        if from > to {
            return Ok(Vec::new());
        }
        Ok(l.range(from as usize..=to as usize).collect())
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

    fn set(&mut self, key: Key, value: Value) {
        self.keyspace.insert(key, Object::String(value));
    }

    pub fn get(&mut self, key: &Key) -> Option<&Object> {
        if self.expire_clean(key) {
            return None;
        }
        self.keyspace.get(key)
    }
    pub fn stream_range(
        &mut self,
        key: &Key,
        start: StreamId,
        end: StreamId,
    ) -> Result<Vec<(&StreamId, &Vec<(Key, Value)>)>, CommandError> {
        let Some(stream) = self.as_stream(key)? else {
            return Ok(Vec::new());
        };
        Ok(stream.range((Included(start), Included(end))).collect())
    }
    pub fn stream_add(
        &mut self,
        key: &Key,
        id_spec: StreamIdSpec,
        elems: Vec<(Key, Value)>,
    ) -> Result<StreamId, CommandError> {
        let realtime_ms = self.realtime_ms.as_millis() as u64;
        let stream = self.stream_or_create(key)?;
        let last = stream.last_key_value().map(|(id, _)| *id);

        let stream_id = match id_spec {
            StreamIdSpec::Explicit(id) => id,
            StreamIdSpec::AutoSeq(ms) => {
                let seq = match last {
                    Some(StreamId(last_ms, last_seq)) if last_ms == ms => last_seq + 1,
                    _ if ms == 0 => 1,
                    _ => 0,
                };
                StreamId(ms, seq)
            }
            StreamIdSpec::Auto => {
                let seq = match last {
                    Some(StreamId(last_ms, last_seq)) if last_ms == realtime_ms => last_seq + 1,
                    _ => 0,
                };
                StreamId(realtime_ms, seq)
            }
        };

        if stream_id == StreamId(0, 0) {
            return Err(CommandError::InvalidStreamZero);
        }
        if let Some(id) = last
            && !stream_id.is_valid(&id)
        {
            return Err(CommandError::InvalidStream);
        }

        let stream_values = stream.entry(stream_id).or_insert_with(Vec::new);
        stream_values.extend(elems);
        Ok(stream_id)
    }

    pub fn setex(&mut self, key: Key, value: Value, ex_at: Option<Duration>) {
        if let Some(ex) = ex_at {
            self.expires.insert(key.clone(), ex);
        }

        self.set(key, value);
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::db::HandleWaitersResult;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn db() -> Db {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Db::create(Instant::now(), realtime_ms)
    }

    #[test]
    fn popping_an_emptied_list_again_does_not_panic() {
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();
        db.list_append(key.clone(), vec![b"a".to_vec().into()])
            .unwrap();
        assert_eq!(db.list_pop(&key, 1).unwrap().len(), 1);
        // key stays in keyspace as an empty list — popping again must not underflow.
        assert_eq!(db.list_pop(&key, 1).unwrap().len(), 0);
    }

    #[test]
    fn setex_roundtrips_through_as_string() {
        let mut db = db();
        let key: Key = b"greeting".to_vec().into();
        db.setex(key.clone(), b"hello".to_vec().into(), None);

        let got = db.as_string(&key).unwrap().unwrap();
        assert_eq!(Vec::<u8>::from(got), b"hello".to_vec());
    }

    #[test]
    fn as_string_on_missing_key_is_ok_none() {
        let mut db = db();
        let key: Key = b"absent".to_vec().into();
        assert!(db.as_string(&key).unwrap().is_none());
    }

    #[test]
    fn as_string_on_list_key_is_wrong_type() {
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();
        db.list_append(key.clone(), vec![b"a".to_vec().into()])
            .unwrap();
        assert!(matches!(
            db.as_string(&key),
            Err(CommandError::WrongType(_))
        ));
    }

    #[test]
    fn expired_key_reads_as_absent() {
        let mut db = db();
        let key: Key = b"greeting".to_vec().into();
        let now = db.realtime_ms();
        db.setex(key.clone(), b"hello".to_vec().into(), Some(now));

        db.update_time(now + Duration::from_millis(1));

        assert!(db.as_string(&key).unwrap().is_none());
    }

    #[test]
    fn list_append_and_prepand_order_elements() {
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();
        db.list_append(key.clone(), vec![b"b".to_vec().into()])
            .unwrap();
        db.list_prepand(key.clone(), vec![b"a".to_vec().into()])
            .unwrap();

        let items = db.list_get(&key, 0, -1).unwrap();
        let items: Vec<Vec<u8>> = items.into_iter().map(Into::into).collect();
        assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn list_get_out_of_range_indices_yield_empty() {
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();
        db.list_append(key.clone(), vec![b"a".to_vec().into()])
            .unwrap();

        assert!(db.list_get(&key, 10, 20).unwrap().is_empty());
    }

    #[test]
    fn blpop_returns_immediately_when_list_has_items() {
        use crate::client::ClientId;
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();
        db.list_append(key.clone(), vec![b"a".to_vec().into()])
            .unwrap();

        let got = db.blpop(key, None, ClientId::new(1)).unwrap().unwrap();
        assert_eq!(Vec::<u8>::from(got), b"a".to_vec());
    }

    #[test]
    fn stream_add_rejects_id_of_zero_zero() {
        let mut db = db();
        let key: Key = b"mystream".to_vec().into();
        assert!(matches!(
            db.stream_add(&key, StreamIdSpec::Explicit(StreamId(0, 0)), vec![]),
            Err(CommandError::InvalidStreamZero)
        ));
    }

    #[test]
    fn stream_add_rejects_id_not_greater_than_top_item() {
        let mut db = db();
        let key: Key = b"mystream".to_vec().into();
        db.stream_add(&key, StreamIdSpec::Explicit(StreamId(1, 1)), vec![])
            .unwrap();

        assert!(matches!(
            db.stream_add(&key, StreamIdSpec::Explicit(StreamId(1, 1)), vec![]),
            Err(CommandError::InvalidStream)
        ));
        assert!(matches!(
            db.stream_add(&key, StreamIdSpec::Explicit(StreamId(0, 3)), vec![]),
            Err(CommandError::InvalidStream)
        ));
    }

    #[test]
    fn stream_add_auto_seq_increments_within_same_ms() {
        let mut db = db();
        let key: Key = b"mystream".to_vec().into();

        let first = db
            .stream_add(&key, StreamIdSpec::AutoSeq(5), vec![])
            .unwrap();
        assert_eq!(String::from(first), "5-0");

        let second = db
            .stream_add(&key, StreamIdSpec::AutoSeq(5), vec![])
            .unwrap();
        assert_eq!(String::from(second), "5-1");
    }

    #[test]
    fn stream_add_auto_seq_defaults_to_one_when_ms_is_zero() {
        let mut db = db();
        let key: Key = b"mystream".to_vec().into();

        let first = db
            .stream_add(&key, StreamIdSpec::AutoSeq(0), vec![])
            .unwrap();
        assert_eq!(String::from(first), "0-1");
    }

    #[test]
    fn stream_add_auto_generates_id_from_current_time() {
        let mut db = db();
        let key: Key = b"mystream".to_vec().into();
        let now_ms = db.realtime_ms().as_millis() as u64;

        let first = db.stream_add(&key, StreamIdSpec::Auto, vec![]).unwrap();
        assert_eq!(String::from(first), format!("{now_ms}-0"));

        let second = db.stream_add(&key, StreamIdSpec::Auto, vec![]).unwrap();
        assert_eq!(String::from(second), format!("{now_ms}-1"));
    }

    #[test]
    fn blpop_registers_a_waiter_when_list_is_empty() {
        use crate::client::ClientId;
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();

        assert!(
            db.blpop(key.clone(), None, ClientId::new(1))
                .unwrap()
                .is_none()
        );

        // a later push now delivers straight to the waiting client via the outbox.
        db.list_append(key.clone(), vec![b"a".to_vec().into()])
            .unwrap();
        let HandleWaitersResult(mut delivered, _deadline) = db.handle_waiters();
        let (got_key, got_value) = delivered.remove(&ClientId::new(1)).unwrap().unwrap();
        assert_eq!(got_key, key);
        assert_eq!(Vec::<u8>::from(got_value), b"a".to_vec());
    }
}
