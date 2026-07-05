use std::time::Duration;

use crate::{
    client::ClientId,
    command::{
        CommandError,
        common::{ExpCmd, get_ttl},
    },
    db::{Db, Key, Value},
    resp::{Reply, Resp},
};

pub enum Side {
    Front,
    Back,
}

pub fn push(
    db: &mut Db,
    side: Side,
    key: &Vec<u8>,
    elems: &[Vec<u8>],
) -> Result<Reply, CommandError> {
    let key: Key = key.into();
    let elems: Vec<Value> = elems.iter().map(Into::into).collect();
    let out: i64 = match side {
        Side::Front => db.list_prepand(key, elems),
        Side::Back => db.list_append(key, elems),
    };

    Ok(Reply::Now(Resp::Integer(out)))
}

pub fn lrange(
    db: &Db,
    key: &Vec<u8>,
    num_from: &Vec<u8>,
    num_to: &Vec<u8>,
) -> Result<Reply, CommandError> {
    let key: Key = key.into();
    let num_from_err = CommandError::WrongNumber(String::from_utf8_lossy(num_from).into());
    let num_to_err = CommandError::WrongNumber(String::from_utf8_lossy(num_to).into());

    let num_from: i32 = str::from_utf8(num_from)
        .ok()
        .and_then(|item| item.parse().ok())
        .ok_or_else(|| num_from_err)?;

    let num_to: i32 = str::from_utf8(num_to)
        .ok()
        .and_then(|item| item.parse().ok())
        .ok_or_else(|| num_to_err)?;

    let items: Vec<Vec<u8>> = db
        .list_get(key, num_from, num_to)
        .iter()
        .map(|&item| item.into())
        .collect();

    let resp_arr = items
        .into_iter()
        .map(|item| Resp::Bulk(Some(item)))
        .collect::<Vec<Resp>>();

    Ok(Reply::Now(Resp::Array(Some(resp_arr))))
}

pub fn llen(db: &Db, key: &Vec<u8>) -> Result<Reply, CommandError> {
    let key: Key = key.into();
    let out = db.list_len(key);
    Ok(Reply::Now(Resp::Integer(out)))
}

// TODO: Do we need to handle the case when the len is 1, which means we should use Bulk resp
// directly without packing it into Array?
pub fn blpop(
    db: &mut Db,
    key: &Vec<u8>,
    timeout: Option<&Vec<u8>>,
    client_id: ClientId,
) -> Result<Reply, CommandError> {
    let key: Key = key.into();
    let timeout = get_ttl(ExpCmd::Ex, timeout)?.and_then(|timeout| {
        if timeout == Duration::from_millis(0) {
            None
        } else {
            Some(timeout)
        }
    });
    let resp = db.blpop(key.clone(), timeout, client_id).map(|item| {
        Resp::Array(Some(vec![
            Resp::Bulk(Some(key.into())),
            Resp::Bulk(Some(item.into())),
        ]))
    }); // None → key absent → caller writes $-1

    resp.map_or(Ok(Reply::Blocked), |resp| Ok(Reply::Now(resp)))
}

pub fn lpop(db: &mut Db, key: &Vec<u8>, num: Option<&Vec<u8>>) -> Result<Reply, CommandError> {
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
        .list_pop(&key, num_parsed)
        .iter()
        .map(Into::into)
        .collect();

    let resp = if items.len() == 1 {
        Resp::Bulk(items.pop())
    } else {
        Resp::Array(Some(
            items
                .into_iter()
                .map(|item| Resp::Bulk(Some(item)))
                .collect::<Vec<Resp>>(),
        ))
    };

    Ok(Reply::Now(resp))
}

pub fn cmd_type(db: &mut Db, key: &Vec<u8>) -> Reply {
    let key: Key = key.into();
    let resp: Resp = if db.list_exist(&key) {
        Resp::Simple("string".into())
    } else {
        Resp::Simple("none".into())
    };
    Reply::Now(resp)
}
