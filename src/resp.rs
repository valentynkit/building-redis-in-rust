// find star,

use crate::command::common::CommandError;

pub fn parse_request(buf: &[u8]) -> Option<Request> {
    if buf.first()? != &b'*' {
        return None;
    }

    let get_position = |inner_buf: &[u8], ch: u8| inner_buf.iter().position(|&b| b == ch);
    let get_part_end_position = |inner_buf: &[u8]| get_position(inner_buf, b'\r');
    let get_dollar_sign_position = |inner_buf: &[u8]| get_position(inner_buf, b'$');
    let parse_number = |inner_buf: &[u8]| str::from_utf8(inner_buf).ok()?.parse().ok();
    let validate_part_end = |inner_buf: &[u8]| inner_buf == [b'\r', b'\n'];
    let mut cursor = get_part_end_position(&buf[1..])? + 1;
    let n_elems: usize = parse_number(&buf[1..cursor])?;
    let mut output: Vec<Vec<u8>> = vec![];
    for _ in 0..n_elems {
        let num_start = cursor + get_dollar_sign_position(&buf[cursor..])? + 1;
        // "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
        cursor = num_start + get_part_end_position(&buf[num_start + 1..])? + 1;
        let part_size = parse_number(&buf[num_start..cursor])?;
        cursor += 2;

        let part_slice = buf.get(cursor..cursor + part_size)?;
        cursor += part_size;

        if !validate_part_end(buf.get(cursor..cursor + 2)?) {
            return None;
        }
        cursor += 2;
        output.push(part_slice.to_vec());
    }

    Some(Request::new(output, cursor))
}

const END_OF_LINE: &[u8; 2] = b"\r\n";

pub enum Resp {
    Simple(String),
    Error(String),
    Integer(i64),
    // TODO: consider migrating to Bytes/BytesMut instead of u8
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Self>>),
}

impl<T: Into<Resp>> FromIterator<T> for Resp {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Resp::Array(Some(iter.into_iter().map(Into::into).collect()))
    }
}

pub enum Reply {
    Now(Resp),
    StartTransaction,
    AddTransaction(Resp),
    ExecTransaction,
    DiscardTransaction(Option<Resp>),
    Blocked,
}

impl From<Resp> for Reply {
    fn from(resp: Resp) -> Self {
        Reply::Now(resp)
    }
}

pub struct Request {
    body: Resp,
    consumed: usize,
}

impl Request {
    fn new(items: Vec<Vec<u8>>, consumed: usize) -> Self {
        let resp_arr = items
            .into_iter()
            .map(|item| Resp::Bulk(Some(item)))
            .collect::<Vec<Resp>>();

        Self {
            body: Resp::Array(Some(resp_arr)),
            consumed,
        }
    }
    pub const fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn body(self) -> Resp {
        self.body
    }
}

impl Resp {
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

// *{v.len()}\r\n then recurse e.encode(out) per element
fn write_arr(out: &mut Vec<u8>, items: &[Resp]) {
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

#[cfg(test)]
mod test {
    use crate::command::common::CommandError;
    use crate::resp::{Resp, parse_request};

    #[test]
    fn parses_array_of_bulk_strings() {
        let req = parse_request(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n").unwrap();
        assert_eq!(req.consumed(), 23);
        assert_eq!(
            req.body().into_args().unwrap(),
            vec![b"ECHO".to_vec(), b"hey".to_vec()]
        );
    }

    #[test]
    fn consumed_count_allows_pipelining() {
        let buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let req = parse_request(buf).unwrap();
        assert_eq!(req.consumed(), 14); // points exactly past the first command

        let second = parse_request(&buf[req.consumed()..]).unwrap();
        assert_eq!(second.body().into_args().unwrap(), vec![b"PING".to_vec()]);
    }

    #[test]
    fn rejects_non_array_input() {
        assert!(parse_request(b"PING\r\n").is_none());
        assert!(parse_request(b"").is_none());
    }

    #[test]
    fn incomplete_frame_is_none() {
        assert!(parse_request(b"*1\r\n$4\r\nPIN").is_none()); // bulk body truncated
        assert!(parse_request(b"*2\r\n$4\r\nECHO\r\n$3\r\nhe").is_none()); // second bulk truncated
        assert!(parse_request(b"*1\r\n$4\r\nPING").is_none()); // missing trailing CRLF
    }

    #[test]
    fn into_args_rejects_non_array() {
        assert!(Resp::Simple("PONG".into()).into_args().is_none());
    }

    #[test]
    fn into_args_rejects_non_bulk_elements() {
        assert!(
            Resp::Array(Some(vec![Resp::Integer(1)]))
                .into_args()
                .is_none()
        );
    }

    #[test]
    fn encode_simple_string() {
        let mut out = Vec::new();
        Resp::Simple("PONG".into()).encode(&mut out);
        assert_eq!(out, b"+PONG\r\n");
    }

    #[test]
    fn encode_error() {
        let mut out = Vec::new();
        Resp::new_error(&CommandError::Unknown("frobnicate".into())).encode(&mut out);
        assert_eq!(out, b"-ERR unknown command 'frobnicate'\r\n");
    }

    #[test]
    fn encode_integer() {
        let mut out = Vec::new();
        Resp::Integer(42).encode(&mut out);
        assert_eq!(out, b":42\r\n");
    }

    #[test]
    fn encode_bulk_string() {
        let mut out = Vec::new();
        Resp::Bulk(Some(b"hey".to_vec())).encode(&mut out);
        assert_eq!(out, b"$3\r\nhey\r\n");
    }

    #[test]
    fn encode_null_bulk() {
        let mut out = Vec::new();
        Resp::Bulk(None).encode(&mut out);
        assert_eq!(out, b"$-1\r\n");
    }

    #[test]
    fn encode_null_array() {
        let mut out = Vec::new();
        Resp::Array(None).encode(&mut out);
        assert_eq!(out, b"*-1\r\n");
    }

    #[test]
    fn encode_array_of_bulk_strings() {
        let mut out = Vec::new();
        Resp::Array(Some(vec![
            Resp::Bulk(Some(b"apple".to_vec())),
            Resp::Bulk(Some(b"blueberry".to_vec())),
        ]))
        .encode(&mut out);
        assert_eq!(out, b"*2\r\n$5\r\napple\r\n$9\r\nblueberry\r\n");
    }
}
