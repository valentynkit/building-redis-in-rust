use std::time::Duration;

use crate::{
    client::ClientId,
    command::{
        common::{get_ttl, ExpCmd, HandleCmdResult},
        CommandError,
    },
    db::{Db, Key, Value},
    resp::{Reply, RespBody},
};

pub enum Side {
    Front,
    Back,
}

pub fn push(db: &mut Db, side: &Side, key: &[u8], elems: &[Vec<u8>]) -> HandleCmdResult {
    let key: Key = key.into();
    let elems: Vec<Value> = elems.iter().map(|item| item.as_slice().into()).collect();
    let out: i64 = match side {
        Side::Front => db.list_prepand(key, elems)?,
        Side::Back => db.list_append(key, elems)?,
    };

    Ok(RespBody::Integer(out).into())
}

pub fn lrange(db: &mut Db, key: &[u8], num_from: &[u8], num_to: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();
    let num_from_err = CommandError::WrongNumber(String::from_utf8_lossy(num_from).into());
    let num_to_err = CommandError::WrongNumber(String::from_utf8_lossy(num_to).into());

    let num_from: i32 = str::from_utf8(num_from)
        .ok()
        .and_then(|item| item.parse().ok())
        .ok_or(num_from_err)?;

    let num_to: i32 = str::from_utf8(num_to)
        .ok()
        .and_then(|item| item.parse().ok())
        .ok_or(num_to_err)?;

    let resp = db
        .list_get(&key, num_from, num_to)?
        .into_iter()
        .collect::<RespBody>();

    Ok(resp.into())
}

pub fn llen(db: &mut Db, key: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();
    let out = db.list_len(key)?;
    Ok(RespBody::Integer(out).into())
}

// TODO: Do we need to handle the case when the len is 1, which means we should use Bulk resp
// directly without packing it into Array?
pub fn blpop(
    db: &mut Db,
    key: &[u8],
    timeout: Option<&[u8]>,
    client_id: ClientId,
) -> HandleCmdResult {
    let key: Key = key.into();
    let timeout = get_ttl(&ExpCmd::Ex, timeout)?.and_then(|timeout| {
        if timeout == Duration::from_millis(0) {
            None
        } else {
            Some(timeout)
        }
    });
    let resp = db
        .blpop(key.clone(), timeout, client_id)?
        .map(|item| RespBody::Array(Some(vec![RespBody::from(key), RespBody::from(item)]))); // None → key absent → caller writes $-1

    resp.map_or(Ok(Reply::Blocked), |resp| Ok(resp.into()))
}

pub fn lpop(db: &mut Db, key: &[u8], num: Option<&[u8]>) -> HandleCmdResult {
    let key: Key = key.into();

    let num_parsed: usize = if let Some(num) = num {
        str::from_utf8(num)
            .ok()
            .and_then(|item| item.parse().ok())
            .ok_or_else(|| CommandError::WrongNumber(String::from_utf8_lossy(num).into()))?
    } else {
        1
    };

    let mut items: Vec<Vec<u8>> = db
        .list_pop(&key, num_parsed)?
        .iter()
        .map(Into::into)
        .collect();

    let resp = if items.len() == 1 {
        RespBody::Bulk(items.pop())
    } else {
        items
            .into_iter()
            .map(|item| RespBody::Bulk(Some(item)))
            .collect::<RespBody>()
    };

    Ok(resp.into())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    fn db() -> Db {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Db::create(Instant::now(), realtime_ms)
    }

    fn body(reply: Reply) -> RespBody {
        let Reply::Now(resp) = reply else {
            panic!("expected an immediate reply");
        };
        resp
    }

    #[test]
    fn rpush_appends_and_reports_length() {
        let mut db = db();
        let resp = body(push(&mut db, &Side::Back, b"mylist".as_ref(), &[b"a".to_vec()]).unwrap());
        assert!(matches!(resp, RespBody::Integer(1)));
    }

    #[test]
    fn lpush_prepends() {
        let mut db = db();
        push(&mut db, &Side::Back, b"mylist".as_ref(), &[b"b".to_vec()]).unwrap();
        push(&mut db, &Side::Front, b"mylist".as_ref(), &[b"a".to_vec()]).unwrap();

        let resp =
            body(lrange(&mut db, b"mylist".as_ref(), b"0".as_ref(), b"-1".as_ref()).unwrap());
        let RespBody::Array(Some(items)) = resp else {
            panic!("expected an array");
        };
        assert!(matches!(&items[0], RespBody::Bulk(Some(v)) if v == b"a"));
        assert!(matches!(&items[1], RespBody::Bulk(Some(v)) if v == b"b"));
    }

    #[test]
    fn lrange_rejects_malformed_number() {
        let result = lrange(
            &mut db(),
            b"mylist".as_ref(),
            b"nope".as_ref(),
            b"-1".as_ref(),
        );
        assert!(matches!(result, Err(CommandError::WrongNumber(_))));
    }

    #[test]
    fn llen_on_missing_key_is_zero() {
        let resp = body(llen(&mut db(), b"absent".as_ref()).unwrap());
        assert!(matches!(resp, RespBody::Integer(0)));
    }

    #[test]
    fn lpop_default_count_returns_bulk() {
        let mut db = db();
        push(
            &mut db,
            &Side::Back,
            b"mylist".as_ref(),
            &[b"a".to_vec(), b"b".to_vec()],
        )
        .unwrap();

        let resp = body(lpop(&mut db, b"mylist".as_ref(), None).unwrap());
        assert!(matches!(resp, RespBody::Bulk(Some(v)) if v == b"a"));
    }

    #[test]
    fn lpop_explicit_count_returns_array() {
        let mut db = db();
        push(
            &mut db,
            &Side::Back,
            b"mylist".as_ref(),
            &[b"a".to_vec(), b"b".to_vec()],
        )
        .unwrap();

        let resp = body(lpop(&mut db, b"mylist".as_ref(), Some(b"2".as_ref())).unwrap());
        let RespBody::Array(Some(items)) = resp else {
            panic!("expected an array");
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn blpop_returns_now_when_list_has_items() {
        let mut db = db();
        push(&mut db, &Side::Back, b"mylist".as_ref(), &[b"a".to_vec()]).unwrap();

        let reply = blpop(&mut db, b"mylist".as_ref(), None, ClientId::new(1)).unwrap();
        assert!(matches!(reply, Reply::Now(_)));
    }

    #[test]
    fn blpop_blocks_when_list_is_empty() {
        let mut db = db();
        let reply = blpop(&mut db, b"mylist".as_ref(), None, ClientId::new(1)).unwrap();
        assert!(matches!(reply, Reply::Blocked));
    }
}
