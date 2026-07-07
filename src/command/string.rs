use crate::{
    command::common::{HandleCmdResult, parse_ttl},
    db::{Db, Key},
    resp::{Reply, Resp},
};

pub fn get(db: &mut Db, key: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();
    let opt_value = db.as_string(&key)?.map(Into::into); // None → key absent → caller writes $-1
    Ok(Reply::Now(Resp::Bulk(opt_value)))
}

pub fn set(
    db: &mut Db,
    key: &[u8],
    value: &[u8],
    exp_cmd: Option<&[u8]>,
    exp: Option<&[u8]>,
) -> HandleCmdResult {
    let expiry = parse_ttl(exp_cmd, exp)?.map(|ttl| db.realtime_ms() + ttl);

    db.setex(key.into(), value.into(), expiry);
    Ok(Reply::Now(Resp::Simple("OK".into())))
}
pub fn cmd_type(db: &mut Db, key: &[u8]) -> Reply {
    let key: Key = key.into();
    let value = db.get(&key);

    let resp: Resp = value.map_or_else(
        || Resp::Simple("none".into()),
        |obj| Resp::Simple(obj.type_name().into()),
    );

    Reply::Now(resp)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn db() -> Db {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Db::create(Instant::now(), realtime_ms)
    }

    fn body(reply: Reply) -> Resp {
        let Reply::Now(resp) = reply else {
            panic!("expected an immediate reply");
        };
        resp
    }

    #[test]
    fn get_on_missing_key_is_null_bulk() {
        let mut db = db();
        let resp = body(get(&mut db, b"absent".as_ref()).unwrap());
        assert!(matches!(resp, Resp::Bulk(None)));
    }

    #[test]
    fn set_then_get_roundtrips() {
        let mut db = db();
        set(&mut db, b"greeting".as_ref(), b"hello".as_ref(), None, None).unwrap();

        let resp = body(get(&mut db, b"greeting".as_ref()).unwrap());
        assert!(matches!(resp, Resp::Bulk(Some(v)) if v == b"hello"));
    }

    #[test]
    fn set_with_ex_expires_the_key() {
        let mut db = db();
        set(
            &mut db,
            b"greeting".as_ref(),
            b"hello".as_ref(),
            Some(b"EX".as_ref()),
            Some(b"1".as_ref()),
        )
        .unwrap();

        db.update_time(db.realtime_ms() + Duration::from_secs(2));

        let resp = body(get(&mut db, b"greeting".as_ref()).unwrap());
        assert!(matches!(resp, Resp::Bulk(None)));
    }

    #[test]
    fn type_reports_none_string_and_list() {
        let mut db = db();
        assert!(
            matches!(cmd_type(&mut db, b"absent".as_ref()), Reply::Now(Resp::Simple(s)) if s == "none")
        );

        set(&mut db, b"str".as_ref(), b"hello".as_ref(), None, None).unwrap();
        assert!(
            matches!(cmd_type(&mut db, b"str".as_ref()), Reply::Now(Resp::Simple(s)) if s == "string")
        );

        db.list_append(b"list".to_vec().into(), vec![b"a".to_vec().into()])
            .unwrap();
        assert!(
            matches!(cmd_type(&mut db, b"list".as_ref()), Reply::Now(Resp::Simple(s)) if s == "list")
        );
    }
}
