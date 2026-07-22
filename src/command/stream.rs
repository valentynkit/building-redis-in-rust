use std::time::Duration;

use crate::{
    client::ClientId,
    command::common::{BlockMode, CommandError, HandleCmdResult},
    db::{Db, Key, StreamId, StreamIdSpec, Value},
    resp::{Reply, RespBody},
};
// Splits a leading `BLOCK <ms>` prefix off, if present. Returns the timeout
// (if blocking) and the remaining args, still starting at `STREAMS`.
fn parse_block_prefix(args: &[Vec<u8>]) -> Result<(BlockMode, &[Vec<u8>]), CommandError> {
    let Some(kw) = args.first() else {
        return Err(CommandError::ParseStream("Invalid xread args".into()));
    };
    if !kw.eq_ignore_ascii_case(b"block") {
        return Ok((BlockMode::NotBlocking, args));
    }

    let ms_bytes = args.get(1).map(Vec::as_slice).unwrap_or_default();
    let ms: u64 = str::from_utf8(ms_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandError::WrongNumber(String::from_utf8_lossy(ms_bytes).into_owned()))?;

    // 0 = No timeout
    let timeout: BlockMode = if ms == 0 {
        BlockMode::Forever
    } else {
        BlockMode::Timeout(Duration::from_millis(ms))
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

    let positions: Vec<(Key, StreamId)> = keys
        .iter()
        .zip(ids)
        .map(|(key, id)| {
            let key: Key = key.as_slice().into();
            // XREAD's id is exclusive, that's why we incr by 1 from the provided one
            let stream = db.stream_or_create(&key)?;
            let last = stream.last_key_value().map(|(id, _)| *id);
            let mut id_start = StreamId::parse_opt_seq(&String::from_utf8_lossy(id), last)?;
            id_start.incr_seq();
            Ok((key, id_start))
        })
        .collect::<Result<_, CommandError>>()?;

    if let Some(resp) = db.xread_snapshot(&positions)? {
        return Ok(Reply::readonly(resp));
    }
    let reply = match block {
        BlockMode::NotBlocking => Reply::readonly(RespBody::Array(None)),
        BlockMode::Forever => {
            db.xread_wait(client_id, positions, None);
            Reply::Blocked
        }
        BlockMode::Timeout(timeout) => {
            db.xread_wait(client_id, positions, Some(timeout));
            Reply::Blocked
        }
    };

    Ok(reply)
}

pub fn xrange(db: &mut Db, key: &[u8], start: &[u8], end: &[u8]) -> HandleCmdResult {
    let key: Key = key.into();

    let start = StreamId::parse_opt_seq(&String::from_utf8_lossy(start), None)?;
    let end = StreamId::parse_opt_seq(&String::from_utf8_lossy(end), None)?;

    if end <= start {
        return Err(CommandError::ParseStream(format!(
            "Invalid Id range from {start} to {end}"
        )));
    }

    let entries = db
        .stream_range(&key, start, end)?
        .into_iter()
        .map(|(id, fields)| {
            let field_arr: RespBody = fields
                .iter()
                .flat_map(|(k, v)| [RespBody::from(k), RespBody::from(v)])
                .collect();
            RespBody::Array(Some(vec![RespBody::from(*id), field_arr]))
        })
        .collect::<RespBody>();

    Ok(Reply::readonly(entries))
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
    Ok(Reply::write(RespBody::from(id)))
}
