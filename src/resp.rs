// find star,

use tracing::trace;

use crate::command::common::CommandError;

pub fn parse_resp(buf: &[u8]) -> Option<Frame> {
    if buf.first()? != &b'*' {
        trace!("malformed resp frame: does not start with '*'");
        return None;
    }

    let mut cursor = find_from(buf, 1, b'\r')?;
    if !is_crlf(buf.get(cursor..cursor + 2)?) {
        trace!(cursor, "malformed resp frame: array header missing CRLF");
        return None;
    }
    let n_elems: usize = parse_number(&buf[1..cursor])?;
    cursor += 2;

    let mut output: Vec<Vec<u8>> = vec![];
    for _ in 0..n_elems {
        // "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
        let num_start = find_from(buf, cursor, b'$')? + 1;
        cursor = find_from(buf, num_start, b'\r')?;
        let part_size = parse_number(&buf[num_start..cursor])?;
        if !is_crlf(buf.get(cursor..cursor + 2)?) {
            trace!(cursor, "malformed resp frame: bulk header missing CRLF");
            return None;
        }
        cursor += 2;

        // checked: part_size comes straight from client-supplied digits, with
        // no upper bound — an unchecked add here would overflow on a huge
        // declared length (panic in debug builds, silent wraparound in release).
        let data_end = cursor.checked_add(part_size)?;
        let part_slice = buf.get(cursor..data_end)?;
        cursor = data_end;

        let part_end = buf.get(cursor..cursor + 2)?;
        if !is_crlf(part_end) {
            trace!(
                cursor,
                ?part_end,
                "malformed resp frame: missing trailing CRLF"
            );
            return None;
        }
        cursor += 2;
        output.push(part_slice.to_vec());
    }

    Some(Frame::new(output, cursor))
}

/// Absolute index of the first `byte` in `buf`, searching from `start` onward.
fn find_from(buf: &[u8], start: usize, byte: u8) -> Option<usize> {
    buf.get(start..)?
        .iter()
        .position(|&b| b == byte)
        .map(|pos| start + pos)
}

fn parse_number(buf: &[u8]) -> Option<usize> {
    str::from_utf8(buf).ok()?.parse().ok()
}

fn is_crlf(buf: &[u8]) -> bool {
    buf == [b'\r', b'\n']
}

pub enum RespBody {
    Simple(String),
    Rdb(Vec<u8>),
    Error(String),
    Integer(i64),
    // TODO: consider migrating to Bytes/BytesMut instead of u8
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Self>>),
}

impl RespBody {
    pub fn new_error(error: &CommandError) -> Self {
        Self::Error(error.to_string())
    }
    pub fn new_queued() -> Self {
        Self::Simple("QUEUED".into())
    }

    pub fn new_ok() -> Self {
        Self::Simple("OK".into())
    }
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Simple(s) => write_simple_string(out, s),
            Self::Rdb(bytes) => write_rdb(out, bytes),
            Self::Error(s) => write_simple_error(out, s),
            Self::Integer(num) => write_int(out, *num),
            Self::Bulk(None) => write_null_bulk(out),
            Self::Bulk(Some(bulk_str)) => write_bulk_string(out, bulk_str),
            Self::Array(None) => write_null_array(out),
            Self::Array(Some(value)) => write_arr(out, value),
        }
    }

    // A client request is always Array(Some([Bulk, Bulk, ...])) flatten to raw args.
    pub fn into_args(self) -> Option<Vec<Vec<u8>>> {
        let Self::Array(Some(items)) = self else {
            return None;
        };
        items
            .into_iter()
            .map(|item| match item {
                Self::Bulk(Some(bytes)) => Some(bytes),
                _ => None,
            })
            .collect()
    }
}

impl From<&str> for RespBody {
    fn from(value: &str) -> Self {
        Self::Bulk(Some(value.as_bytes().to_vec()))
    }
}

impl From<Vec<u8>> for RespBody {
    fn from(v: Vec<u8>) -> Self {
        Self::Bulk(Some(v))
    }
}

impl<T: Into<RespBody>> FromIterator<T> for RespBody {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        RespBody::Array(Some(iter.into_iter().map(Into::into).collect()))
    }
}

const END_OF_LINE: &[u8; 2] = b"\r\n";

// *{v.len()}\r\n then recurse e.encode(out) per element
fn write_arr(out: &mut Vec<u8>, items: &[RespBody]) {
    out.push(b'*');
    out.extend_from_slice(items.len().to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);

    for item in items {
        item.encode(out);
    }
}

fn write_int(out: &mut Vec<u8>, num: i64) {
    out.push(b':');
    out.extend_from_slice(num.to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);
}

fn write_null_bulk(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

fn write_null_array(out: &mut Vec<u8>) {
    out.extend_from_slice(b"*-1\r\n");
}

fn write_simple_error(out: &mut Vec<u8>, msg: &str) {
    out.extend_from_slice(b"-ERR ");
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(END_OF_LINE);
}

fn write_rdb(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);
    out.extend_from_slice(data);
}

fn write_simple_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(END_OF_LINE);
}

fn write_bulk_string(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);
    out.extend_from_slice(data);
    out.extend_from_slice(END_OF_LINE);
}

pub enum Propagate {
    Skip,
    Replicate,
}

pub enum Reply {
    Now(RespBody, Propagate),
    Rdb(RespBody, RespBody),
    StartTransaction,
    AddTransaction(RespBody),
    ExecTransaction,
    DiscardTransaction(Option<RespBody>),
    Blocked,
}

impl Reply {
    pub fn readonly(body: RespBody) -> Reply {
        Reply::Now(body, Propagate::Skip)
    }
    pub fn write(body: RespBody) -> Reply {
        Reply::Now(body, Propagate::Replicate)
    }
}

pub struct Frame {
    body: RespBody,
    consumed: usize,
}

impl Frame {
    fn new(items: Vec<Vec<u8>>, consumed: usize) -> Self {
        let resp_arr = items
            .into_iter()
            .map(|item| RespBody::Bulk(Some(item)))
            .collect::<Vec<RespBody>>();

        Self {
            body: RespBody::Array(Some(resp_arr)),
            consumed,
        }
    }
    pub const fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn body(self) -> RespBody {
        self.body
    }
}

#[cfg(test)]
mod test {
    use crate::command::common::CommandError;
    use crate::resp::{parse_resp, RespBody};

    #[test]
    fn parses_array_of_bulk_strings() {
        let req = parse_resp(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n").unwrap();
        assert_eq!(req.consumed(), 23);
        assert_eq!(
            req.body().into_args().unwrap(),
            vec![b"ECHO".to_vec(), b"hey".to_vec()]
        );
    }

    #[test]
    fn consumed_count_allows_pipelining() {
        let buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let req = parse_resp(buf).unwrap();
        assert_eq!(req.consumed(), 14); // points exactly past the first command

        let second = parse_resp(&buf[req.consumed()..]).unwrap();
        assert_eq!(second.body().into_args().unwrap(), vec![b"PING".to_vec()]);
    }

    #[test]
    fn rejects_non_array_input() {
        assert!(parse_resp(b"PING\r\n").is_none());
        assert!(parse_resp(b"").is_none());
    }

    #[test]
    fn incomplete_frame_is_none() {
        assert!(parse_resp(b"*1\r\n$4\r\nPIN").is_none()); // bulk body truncated
        assert!(parse_resp(b"*2\r\n$4\r\nECHO\r\n$3\r\nhe").is_none()); // second bulk truncated
        assert!(parse_resp(b"*1\r\n$4\r\nPING").is_none()); // missing trailing CRLF
    }

    #[test]
    fn rejects_malformed_array_header() {
        // "\r" not followed by "\n" right after the declared element count.
        assert!(parse_resp(b"*1\rX$4\r\nPING\r\n").is_none());
    }

    #[test]
    fn rejects_malformed_bulk_header() {
        // "\r" not followed by "\n" right after a bulk string's declared length.
        assert!(parse_resp(b"*1\r\n$4\rXPING\r\n").is_none());
    }

    #[test]
    fn rejects_wrong_bytes_where_trailing_crlf_belongs() {
        // Bytes are present (unlike incomplete_frame_is_none's truncated cases),
        // they're just not "\r\n" — this is the one case that actually exercises
        // the trailing-CRLF validate_part_end-false branch, not a too-few-bytes `?`.
        assert!(parse_resp(b"*1\r\n$4\r\nPINGXY").is_none());
    }

    #[test]
    fn rejects_declared_length_that_would_overflow_cursor() {
        // A declared bulk length near usize::MAX must be rejected cleanly via
        // checked_add, not panic (debug builds) or silently wrap (release).
        let huge = usize::MAX.to_string();
        let buf = format!("*1\r\n${huge}\r\nx\r\n").into_bytes();
        assert!(parse_resp(&buf).is_none());
    }

    #[test]
    fn into_args_rejects_non_array() {
        assert!(RespBody::Simple("PONG".into()).into_args().is_none());
    }

    #[test]
    fn into_args_rejects_non_bulk_elements() {
        assert!(RespBody::Array(Some(vec![RespBody::Integer(1)]))
            .into_args()
            .is_none());
    }

    #[test]
    fn encode_simple_string() {
        let mut out = Vec::new();
        RespBody::Simple("PONG".into()).encode(&mut out);
        assert_eq!(out, b"+PONG\r\n");
    }

    #[test]
    fn encode_error() {
        let mut out = Vec::new();
        RespBody::new_error(&CommandError::Unknown("frobnicate".into())).encode(&mut out);
        assert_eq!(out, b"-ERR unknown command 'frobnicate'\r\n");
    }

    #[test]
    fn encode_integer() {
        let mut out = Vec::new();
        RespBody::Integer(42).encode(&mut out);
        assert_eq!(out, b":42\r\n");
    }

    #[test]
    fn encode_bulk_string() {
        let mut out = Vec::new();
        RespBody::Bulk(Some(b"hey".to_vec())).encode(&mut out);
        assert_eq!(out, b"$3\r\nhey\r\n");
    }

    #[test]
    fn encode_null_bulk() {
        let mut out = Vec::new();
        RespBody::Bulk(None).encode(&mut out);
        assert_eq!(out, b"$-1\r\n");
    }

    #[test]
    fn encode_null_array() {
        let mut out = Vec::new();
        RespBody::Array(None).encode(&mut out);
        assert_eq!(out, b"*-1\r\n");
    }

    #[test]
    fn encode_array_of_bulk_strings() {
        let mut out = Vec::new();
        RespBody::Array(Some(vec![
            RespBody::Bulk(Some(b"apple".to_vec())),
            RespBody::Bulk(Some(b"blueberry".to_vec())),
        ]))
        .encode(&mut out);
        assert_eq!(out, b"*2\r\n$5\r\napple\r\n$9\r\nblueberry\r\n");
    }
}
