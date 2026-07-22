mod key_value;
mod stream_id;
pub use key_value::{Key, Value};
pub use stream_id::{StreamId, StreamIdSpec};

use std::collections::HashSet;
use std::ops::Bound::Included;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    mem,
    time::Duration,
};

use tracing::debug;

use crate::{client::ClientId, command::common::CommandError, resp::RespBody};

pub type Stream = BTreeMap<StreamId, Vec<(Key, Value)>>;
type Waiters = VecDeque<(ClientId, Option<Duration>)>;

// one type per key; the tagged-union equivalent of C's robj + union pointer
pub enum Object {
    String(Value),
    List(VecDeque<Value>),
    Stream(Stream),
}

impl Object {
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::List(_) => "list",
            Self::Stream(_) => "stream",
        }
    }
}

// One client's pending XREAD BLOCK: which (key, exclusive-lower-bound) pairs
// it's watching, and when to give up and reply with a null array.
struct StreamWait {
    client_id: ClientId,
    positions: Vec<(Key, StreamId)>,
    deadline: Option<Duration>,
}

// bidirectional index between watched keys and the clients watching them
struct ClientWatch {
    keys: HashSet<Key>,
    dirty: bool,
}

impl ClientWatch {
    fn new() -> Self {
        Self {
            keys: HashSet::new(),
            dirty: false,
        }
    }

    fn add(&mut self, key: Key) {
        self.keys.insert(key);
    }

    fn make_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn remove(&mut self) -> Option<HashSet<Key>> {
        Some(mem::take(&mut self.keys))
    }
}

pub struct HandleWaitersResult {
    pub replies: HashMap<ClientId, RespBody>,
    pub deadline: Option<Duration>,
}

pub struct Db {
    keyspace: HashMap<Key, Object>,
    expires: HashMap<Key, Duration>,
    list_waiters: HashMap<Key, Waiters>,
    key_watchers: HashMap<Key, HashSet<ClientId>>,
    client_watches: HashMap<ClientId, ClientWatch>,
    outbox: Vec<Key>,
    stream_waiters: Vec<StreamWait>,
    realtime_ms: Duration,
}

impl Db {
    pub fn create(realtime_ms: Duration) -> Self {
        debug!("db initialized");
        Self {
            keyspace: HashMap::new(),
            expires: HashMap::new(),
            list_waiters: HashMap::new(),
            key_watchers: HashMap::new(),
            client_watches: HashMap::new(),
            outbox: Vec::new(),
            stream_waiters: Vec::new(),
            realtime_ms,
        }
    }

    // ---------------------------------------------------------------
    // String ops
    // ---------------------------------------------------------------

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

    fn set(&mut self, key: Key, value: Value) {
        self.make_dirty(key.clone());
        self.keyspace.insert(key, Object::String(value));
    }

    pub fn setex(&mut self, key: Key, value: Value, ex_at: Option<Duration>) {
        if let Some(ex) = ex_at {
            self.expires.insert(key.clone(), ex);
        }

        self.set(key, value);
    }

    pub fn incr(&mut self, key: Key) -> Result<i64, CommandError> {
        let value = self.as_string(&key)?;
        let new_value = match value {
            Some(value) => Value::from_int(value.parse_int()? + 1),
            None => Value::from_int(1),
        };
        let out = new_value.parse_int()?;
        self.set(key, new_value);
        Ok(out)
    }

    // ---------------------------------------------------------------
    // List ops
    // ---------------------------------------------------------------

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

    pub fn list_len(&mut self, key: Key) -> Result<i64, CommandError> {
        let list = self.as_list(&key)?;

        Ok(list.map_or(0, |list| list.len() as i64))
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

    pub fn list_prepend(&mut self, key: Key, elems: Vec<Value>) -> Result<i64, CommandError> {
        self.make_dirty(key.clone());
        let list = self.list_or_create(&key)?;

        for e in elems {
            list.push_front(e);
        }

        let len = list.len() as i64; // ends the borrow before we touch self below

        if self.list_waiters.contains_key(&key) {
            debug!(%key, "adding outbox");
            self.outbox.push(key);
        }

        Ok(len)
    }

    pub fn list_append(&mut self, key: Key, elems: Vec<Value>) -> Result<i64, CommandError> {
        self.make_dirty(key.clone());
        let list = self.list_or_create(&key)?;
        list.extend(elems);
        let len = list.len() as i64; // ends the borrow before we touch self below

        if self.list_waiters.contains_key(&key) {
            debug!(%key, "adding outbox");
            self.outbox.push(key);
        }
        Ok(len)
    }

    pub fn list_pop(&mut self, key: &Key, len: usize) -> Result<Vec<Value>, CommandError> {
        self.make_dirty(key.clone());
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

    pub fn blpop(
        &mut self,
        key: Key,
        timeout: Option<Duration>,
        client_id: ClientId,
    ) -> Result<Option<Value>, CommandError> {
        self.make_dirty(key.clone());
        // TODO: could be refactored
        if let Some(list) = self.as_list_mut(&key)?
            && let Some(item) = list.pop_front()
        {
            return Ok(Some(item));
        }

        debug!(%key, "adding waiter");
        let waiters = self.list_waiters.entry(key).or_default();
        let deadline = timeout.map(|t| self.realtime_ms + t);
        waiters.push_back((client_id, deadline));
        Ok(None)
    }

    // ---------------------------------------------------------------
    // Stream ops
    // ---------------------------------------------------------------

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
        self.make_dirty(key.clone());
        let realtime_ms = self.realtime_ms.as_millis() as u64;
        let stream = self.stream_or_create(key)?;
        let last = stream.last_key_value().map(|(id, _)| *id);

        let stream_id = match id_spec {
            StreamIdSpec::Explicit(id) => id,
            StreamIdSpec::AutoSeq(ms) => {
                let seq = match last {
                    Some(last) if last.ms() == ms => last.seq() + 1,
                    _ if ms == 0 => 1,
                    _ => 0,
                };
                StreamId::new(ms, seq)
            }
            StreamIdSpec::Auto => {
                let seq = match last {
                    Some(last) if last.ms() == realtime_ms => last.seq() + 1,
                    _ => 0,
                };
                StreamId::new(realtime_ms, seq)
            }
        };

        if stream_id == StreamId::ZERO {
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

    // Non-blocking XREAD range check, reused for both the immediate reply and
    // re-checking a blocked client on wake — reading a stream is non-destructive,
    // so there's nothing to precompute at registration time, just re-run this.
    pub fn xread_snapshot(
        &mut self,
        positions: &[(Key, StreamId)],
    ) -> Result<Option<RespBody>, CommandError> {
        let mut streams: Vec<RespBody> = Vec::new();
        for (key, id_start) in positions {
            let field_items: Vec<RespBody> = self
                .stream_range(key, *id_start, StreamId::MAX)?
                .into_iter()
                .map(|(id, fields)| {
                    let field_arr: RespBody = fields
                        .iter()
                        .flat_map(|(k, v)| [RespBody::from(k), RespBody::from(v)])
                        .collect();
                    RespBody::Array(Some(vec![RespBody::from(*id), field_arr]))
                })
                .collect();

            if !field_items.is_empty() {
                streams.push(RespBody::Array(Some(vec![
                    RespBody::from(key),
                    field_items.into_iter().collect(),
                ])));
            }
        }
        Ok(if streams.is_empty() {
            None
        } else {
            Some(streams.into_iter().collect())
        })
    }

    pub fn xread_wait(
        &mut self,
        client_id: ClientId,
        positions: Vec<(Key, StreamId)>,
        timeout: Option<Duration>,
    ) {
        debug!(?client_id, num_keys = positions.len(), "registering stream waiter");
        let deadline = timeout.map(|t| self.realtime_ms + t);
        self.stream_waiters.push(StreamWait {
            client_id,
            positions,
            deadline,
        });
    }

    // ---------------------------------------------------------------
    // Watcher ops (WATCH / MULTI dirty-tracking)
    // ---------------------------------------------------------------

    fn get_or_create_client_watchers(&mut self, client_id: ClientId) -> &mut ClientWatch {
        self.client_watches
            .entry(client_id)
            .or_insert_with(ClientWatch::new)
    }

    pub fn add_watchers(&mut self, keys: Vec<Key>, client_id: ClientId) {
        for key in keys {
            self.get_or_create_client_watchers(client_id).add(key.clone());
            self.key_watchers.entry(key).or_default().insert(client_id);
        }
    }

    pub fn make_dirty(&mut self, key: Key) {
        if let Some(watchers) = self.key_watchers.get_mut(&key) {
            for client in watchers.iter() {
                if let Some(client) = self.client_watches.get_mut(client) {
                    client.make_dirty();
                }
            }
        }
    }

    pub fn is_dirty(&self, client_id: ClientId) -> bool {
        if let Some(client) = self.client_watches.get(&client_id) {
            return client.is_dirty();
        }
        false
    }

    pub fn remove_watcher(&mut self, client_id: ClientId) {
        if let Some(mut watch) = self.client_watches.remove(&client_id) {
            let keys = watch.remove().unwrap_or_default();
            for key in keys {
                if let Some(watchers) = self.key_watchers.get_mut(&key) {
                    watchers.remove(&client_id);
                    if watchers.is_empty() {
                        self.key_watchers.remove(&key);
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Waiter housekeeping — called once per event-loop tick
    // ---------------------------------------------------------------

    // TODO: consider refactoring for better lifetimes and optimizing to avoid using clone
    pub fn handle_list_waiters(&mut self) -> HandleWaitersResult {
        let date_now = self.realtime_ms();
        let mut nearest_deadline: Option<Duration> = None;
        let mut out: HashMap<ClientId, RespBody> = HashMap::new();
        // cleanup timeout waiters
        self.list_waiters.retain(|_key, waiters| {
            waiters.retain(|(client_id, timeout)| {
                let mut is_expired: bool = false;
                if let Some(value) = timeout {
                    is_expired = *value <= date_now;
                    if is_expired {
                        out.insert(*client_id, RespBody::Array(None));
                    } else {
                        let deadline = (*value).checked_sub(date_now).unwrap();
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
                let Some(waiters) = self.list_waiters.get_mut(key) else {
                    break;
                };
                if waiters.is_empty() {
                    break;
                }
                let (client_id, _timeout) = waiters
                    .pop_front()
                    .expect("queue is guaranteed by the is_empty check before");
                let item = list
                    .pop_front()
                    .expect("value is guaranteed by the is_empty check before");
                out.insert(
                    client_id,
                    RespBody::Array(Some(vec![
                        RespBody::from(key.clone()),
                        RespBody::from(item),
                    ])),
                );

                self.make_dirty(key.clone());
            }
        }
        HandleWaitersResult {
            replies: out,
            deadline: nearest_deadline,
        }
    }

    // ponytail: re-checks every pending wait on every tick rather than being
    // edge-triggered off an outbox — reads are cheap range queries and waiter
    // counts are small, so the simpler always-rescan approach is fine here.
    pub fn handle_stream_waiters(&mut self) -> HandleWaitersResult {
        let date_now = self.realtime_ms();
        let mut out: HashMap<ClientId, RespBody> = HashMap::new();
        let mut nearest_deadline: Option<Duration> = None;

        let pending = mem::take(&mut self.stream_waiters);
        self.stream_waiters = pending
            .into_iter()
            .filter_map(|wait| {
                if let Ok(Some(resp)) = self.xread_snapshot(&wait.positions) {
                    out.insert(wait.client_id, resp);
                    return None;
                }
                if let Some(deadline) = wait.deadline {
                    if deadline <= date_now {
                        out.insert(wait.client_id, RespBody::Array(None));
                        return None;
                    }

                    // TODO: handle this better
                    // I think the invariant is guaranteed by upper if deadline <= date_now {
                    let remaining = deadline.checked_sub(date_now).unwrap();

                    nearest_deadline =
                        Some(nearest_deadline.map_or(remaining, |cur| cur.min(remaining)));
                }
                Some(wait)
            })
            .collect();

        HandleWaitersResult {
            replies: out,
            deadline: nearest_deadline,
        }
    }

    // ---------------------------------------------------------------
    // Clock
    // ---------------------------------------------------------------

    pub const fn update_time(&mut self, realtime_ms: Duration) {
        self.realtime_ms = realtime_ms;
    }

    pub const fn realtime_ms(&self) -> Duration {
        self.realtime_ms
    }

    // ---------------------------------------------------------------
    // Lifecycle / generic
    // ---------------------------------------------------------------

    pub fn key_type(&mut self, key: &Key) -> &'static str {
        if self.expire_clean(key) {
            return "none";
        }
        self.keyspace.get(key).map_or("none", Object::type_name)
    }

    pub fn remove(&mut self, key: &Key) {
        self.make_dirty(key.clone());
        self.keyspace.remove(key);
        self.expires.remove(key);
    }

    // ---------------------------------------------------------------
    // Private helpers used across more than one family above
    // ---------------------------------------------------------------

    // Lazy expiration
    fn expire_clean(&mut self, key: &Key) -> bool {
        let is_expired = self
            .expires
            .get(key)
            .is_some_and(|&exp| exp <= self.realtime_ms);

        if is_expired {
            debug!(%key, "key expired");
            self.remove(key);
        }

        is_expired
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::db::HandleWaitersResult;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn db() -> Db {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Db::create(realtime_ms)
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
    fn list_append_and_prepend_order_elements() {
        let mut db = db();
        let key: Key = b"mylist".to_vec().into();
        db.list_append(key.clone(), vec![b"b".to_vec().into()])
            .unwrap();
        db.list_prepend(key.clone(), vec![b"a".to_vec().into()])
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
            db.stream_add(&key, StreamIdSpec::Explicit(StreamId::ZERO), vec![]),
            Err(CommandError::InvalidStreamZero)
        ));
    }

    #[test]
    fn stream_add_rejects_id_not_greater_than_top_item() {
        let mut db = db();
        let key: Key = b"mystream".to_vec().into();
        db.stream_add(&key, StreamIdSpec::Explicit(StreamId::new(1, 1)), vec![])
            .unwrap();

        assert!(matches!(
            db.stream_add(&key, StreamIdSpec::Explicit(StreamId::new(1, 1)), vec![]),
            Err(CommandError::InvalidStream)
        ));
        assert!(matches!(
            db.stream_add(&key, StreamIdSpec::Explicit(StreamId::new(0, 3)), vec![]),
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
        let HandleWaitersResult {
            replies: mut delivered,
            deadline: _deadline,
        } = db.handle_list_waiters();
        let resp = delivered.remove(&ClientId::new(1)).unwrap();
        let RespBody::Array(Some(items)) = resp else {
            panic!("expected an array reply");
        };
        assert_eq!(items.len(), 2);
    }
}
