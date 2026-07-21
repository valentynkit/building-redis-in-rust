use crate::{
    command::common::{parse_ttl, CommandError, HandleCmdResult},
    db::{Db, Key},
    resp::{Reply, RespBody},
};

pub fn get(db: &mut Db, key: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();
    let opt_value = db.as_string(&key)?.map(Into::into); // None → key absent → caller writes $-1
    Ok(RespBody::Bulk(opt_value).into())
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
    Ok(RespBody::new_ok().into())
}
pub fn cmd_type(db: &mut Db, key: &[u8]) -> Reply {
    let key: Key = key.into();
    let value = db.get(&key);

    let resp: RespBody = value.map_or_else(
        || RespBody::Simple("none".into()),
        |obj| RespBody::Simple(obj.type_name().into()),
    );

    resp.into()
}

pub fn incr(db: &mut Db, key: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();
    let result = db.incr(key)?;
    Ok(RespBody::Integer(result).into())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    fn get_on_missing_key_is_null_bulk() {
        let mut db = db();
        let resp = body(get(&mut db, b"absent".as_ref()).unwrap());
        assert!(matches!(resp, RespBody::Bulk(None)));
    }

    #[test]
    fn set_then_get_roundtrips() {
        let mut db = db();
        set(&mut db, b"greeting".as_ref(), b"hello".as_ref(), None, None).unwrap();

        let resp = body(get(&mut db, b"greeting".as_ref()).unwrap());
        assert!(matches!(resp, RespBody::Bulk(Some(v)) if v == b"hello"));
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
        assert!(matches!(resp, RespBody::Bulk(None)));
    }

    #[test]
    fn type_reports_none_string_and_list() {
        let mut db = db();
        assert!(
            matches!(cmd_type(&mut db, b"absent".as_ref()), Reply::Now(RespBody::Simple(s)) if s == "none")
        );

        set(&mut db, b"str".as_ref(), b"hello".as_ref(), None, None).unwrap();
        assert!(
            matches!(cmd_type(&mut db, b"str".as_ref()), Reply::Now(RespBody::Simple(s)) if s == "string")
        );

        db.list_append(b"list".to_vec().into(), vec![b"a".to_vec().into()])
            .unwrap();
        assert!(
            matches!(cmd_type(&mut db, b"list".as_ref()), Reply::Now(RespBody::Simple(s)) if s == "list")
        );
    }

    #[test]
    fn incr_on_missing_key_starts_at_one() {
        let mut db = db();
        let resp = body(incr(&mut db, b"counter".as_ref()).unwrap());
        assert!(matches!(resp, RespBody::Integer(1)));
    }

    #[test]
    fn incr_on_numeric_key_increments() {
        let mut db = db();
        set(&mut db, b"counter".as_ref(), b"41".as_ref(), None, None).unwrap();

        let resp = body(incr(&mut db, b"counter".as_ref()).unwrap());
        assert!(matches!(resp, RespBody::Integer(42)));
    }

    #[test]
    fn incr_on_non_numeric_key_is_not_an_integer() {
        let mut db = db();
        set(&mut db, b"foo".as_ref(), b"xyz".as_ref(), None, None).unwrap();

        assert!(matches!(
            incr(&mut db, b"foo".as_ref()),
            Err(CommandError::NotAnInteger)
        ));
    }
}
