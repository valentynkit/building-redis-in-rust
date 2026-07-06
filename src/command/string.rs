use crate::{
    command::{CommandError, common::parse_ttl},
    db::{Db, Key},
    resp::{Reply, Resp},
};

pub fn get(db: &mut Db, key: &Vec<u8>) -> Result<Reply, CommandError> {
    let key: Key = key.into();
    let opt_value = db.as_string(&key)?.map(Into::into); // None → key absent → caller writes $-1
    Ok(Reply::Now(Resp::Bulk(opt_value)))
}

pub fn set(
    db: &mut Db,
    key: &Vec<u8>,
    value: &Vec<u8>,
    exp_cmd: Option<&Vec<u8>>,
    exp: Option<&Vec<u8>>,
) -> Result<Reply, CommandError> {
    let expiry = parse_ttl(exp_cmd, exp)?.map(|ttl| db.realtime_ms() + ttl);

    db.setex(key.into(), value.into(), expiry);
    Ok(Reply::Now(Resp::Simple("OK".into())))
}
pub fn cmd_type(db: &mut Db, key: &Vec<u8>) -> Reply {
    let key: Key = key.into();
    let value = db.get(key);

    let resp: Resp = value.map_or_else(
        || Resp::Simple("none".into()),
        |obj| Resp::Simple(obj.type_name().into()),
    );

    Reply::Now(resp)
}
