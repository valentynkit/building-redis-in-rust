use crate::{
    command::common::{CommandError, HandleCmdResult},
    db::{Db, Key, StreamId, StreamIdSpec, Value},
    resp::Resp,
};

// STREAMS/BLOCK
pub fn xread(db: &mut Db, _mode: &[u8], elems: &[Vec<u8>]) -> HandleCmdResult {
    if elems.is_empty() || !elems.len().is_multiple_of(2) {
        return Err(CommandError::ParseStream("Invalid xread args".into()));
    }
    // Validate
    let (keys, ids) = elems.split_at(elems.len() / 2);
    if keys.len() != ids.len() {
        return Err(CommandError::ParseStream("Invalid xread args".into()));
    }
    let mut entries: Vec<Resp> = vec![];
    for idx in 0..keys.len() {
        let key = keys
            .get(idx)
            .ok_or_else(|| CommandError::ParseStream("Invalid xread args".into()))?
            .as_slice()
            .into();

        let id = &String::from_utf8_lossy(
            ids.get(idx)
                .ok_or_else(|| CommandError::ParseStream("Invalid xread args".into()))?,
        );

        // StreamId should exclusive, that why we incr by 1, from provided one
        let mut id_start = StreamId::parse_opt_seq(id)?;
        id_start.incr_seq();
        let id_end = StreamId::parse_opt_seq("+")?;

        let stream_entries = db
            .stream_range(&key, id_start, id_end)?
            .into_iter()
            .map(|(id, fields)| {
                let field_arr: Resp = fields
                    .iter()
                    .flat_map(|(k, v)| [Resp::from(k), Resp::from(v)])
                    .collect();
                Resp::Array(Some(vec![Resp::from(*id), field_arr]))
            })
            .collect::<Resp>();

        entries.push(Resp::Array(Some(vec![Resp::from(key), stream_entries])));
    }

    Ok(entries.into_iter().collect::<Resp>().into())
}

pub fn xrange(db: &mut Db, key: &[u8], start: &[u8], end: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();

    let start = StreamId::parse_opt_seq(&String::from_utf8_lossy(start))?;
    let end = StreamId::parse_opt_seq(&String::from_utf8_lossy(end))?;

    if end <= start {
        return Err(CommandError::ParseStream(format!(
            "Invalid Id range from {start} to {end}"
        )));
    }

    let entries = db
        .stream_range(&key, start, end)?
        .into_iter()
        .map(|(id, fields)| {
            let field_arr: Resp = fields
                .iter()
                .flat_map(|(k, v)| [Resp::from(k), Resp::from(v)])
                .collect();
            Resp::Array(Some(vec![Resp::from(*id), field_arr]))
        })
        .collect::<Resp>();

    Ok(entries.into())
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
