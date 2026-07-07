use crate::{
    command::common::CommandError,
    db::{Db, Key, StreamIdSpec, Value},
    resp::Reply,
};

pub fn xadd(db: &mut Db, key: &[u8], id: &[u8], elems: &[Vec<u8>]) -> Result<Reply, CommandError> {
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
    Ok(Reply::Now(crate::resp::Resp::Bulk(Some(id.into()))))
}
