use std::time::Duration;

use crate::{
    client::ClientId,
    command::common::{CommandError, HandleCmdResult},
    db::{Db, Key, StreamId, StreamIdSpec, Value},
    resp::{Reply, Resp},
};

// Splits a leading `BLOCK <ms>` prefix off, if present. Returns the timeout
// (if blocking) and the remaining args, still starting at `STREAMS`.
fn parse_block_prefix(args: &[Vec<u8>]) -> Result<(Option<Duration>, &[Vec<u8>]), CommandError> {
    let Some(kw) = args.first() else {
        return Err(CommandError::ParseStream("Invalid xread args".into()));
    };
    if !kw.eq_ignore_ascii_case(b"block") {
        return Ok((None, args));
    }

    let ms_bytes = args.get(1).map(Vec::as_slice).unwrap_or_default();
    let ms: u64 = str::from_utf8(ms_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandError::WrongNumber(String::from_utf8_lossy(ms_bytes).into_owned()))?;

    // 0 = No timeout
    let timeout = if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    };

    Ok((timeout, &args[2..]))
}

fn expect_streams_keyword(args: &[Vec<u8>]) -> Result<&[Vec<u8>], CommandError> {
    match args.first() {
        Some(kw) if kw.eq_ignore_ascii_case(b"streams") => Ok(&args[1..]),
        _ => Err(CommandError::ParseStream("Invalid xread args".into())),
    }
}

pub fn xread(db: &mut Db, client_id: ClientId, args: &[Vec<u8>]) -> HandleCmdResult {
    let (block, rest) = parse_block_prefix(args)?;

    let elems = expect_streams_keyword(rest)?;

    if elems.is_empty() || !elems.len().is_multiple_of(2) {
        return Err(CommandError::ParseStream("Invalid xread args".into()));
    }
    let (keys, ids) = elems.split_at(elems.len() / 2);

    let watch: Vec<(Key, StreamId)> = keys
        .iter()
        .zip(ids)
        .map(|(key, id)| {
            let key: Key = key.as_slice().into();
            // XREAD's id is exclusive, that's why we incr by 1 from the provided one
            let mut id_start = StreamId::parse_opt_seq(&String::from_utf8_lossy(id))?;
            id_start.incr_seq();
            Ok((key, id_start))
        })
        .collect::<Result<_, CommandError>>()?;

    if let Some(resp) = db.xread_snapshot(&watch)? {
        return Ok(resp.into());
    }
    // TODO:
    db.xread_wait(client_id, watch, block);
    Ok(Reply::Blocked)
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
