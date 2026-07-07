use crate::{
    command::common::{CommandError, HandleCmdResult},
    db::{Db, Key, StreamId, StreamIdSpec, Value},
    resp::Resp,
};

pub fn xrange(db: &mut Db, key: &[u8], start: &[u8], end: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();

    let start = StreamId::parse_opt_seq(&String::from_utf8_lossy(start))?;
    let end = StreamId::parse_opt_seq(&String::from_utf8_lossy(end))?;

    if end < start {
        return Err(CommandError::ParseStream(format!(
            "Invalid Id range from {start} to {end}"
        )));
    }
    let entries = db
        .stream_range(&key, start, end)?
        .into_iter()
        .map(|(id, fields)| {
            let field_arr = fields
                .iter()
                .flat_map(|(k, v)| [Resp::from(k), Resp::from(v)])
                .collect::<Vec<Resp>>();
            Resp::Array(Some(vec![Resp::from(*id), Resp::Array(Some(field_arr))]))
        })
        .collect::<Vec<Resp>>();

    Ok(Resp::Array(Some(entries)).into())
}

pub fn xadd(db: &mut Db, key: &[u8], id: &[u8], elems: &[Vec<u8>]) -> HandleCmdResult {
    let key: Key = key.into();
    let id_spec = StreamIdSpec::parse(&String::from_utf8_lossy(id))?;
    // 1: key, 2:value, 3:key etc...
    let mut chunks = elems.chunks_exact(2);
    let kv_arr: Vec<(Key, Value)> = (&mut chunks)
        .map(|pair| (pair[0].as_slice().into(), pair[1].as_slice().into()))
        .collect();

    if !chunks.remainder().is_empty() {
        return Err(CommandError::WrongArity(
            "xadd".into(),
            "key id field value [field value....]".into(),
        ));
    }
    let id = db.stream_add(&key, id_spec, kv_arr)?;
    Ok(Resp::from(id).into())
}
