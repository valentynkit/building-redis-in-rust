use crate::{command::common::CommandError, db::Db, resp::Reply};

pub fn xadd(
    db: &mut Db,
    key: &Vec<u8>,
    id: &Vec<u8>,
    elems: &[Vec<u8>],
) -> Result<Reply, CommandError> {
    todo!()
}
