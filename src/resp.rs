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

pub enum Reply {
    Now(Resp),
    Blocked,
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
    use crate::resp::{Resp, parse_request};

    #[test]
    fn resp_full_line() {
        let (args, n) = parse_request(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n").unwrap();
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hey".to_vec()]);
        assert_eq!(n, 23);
    }
    #[test]
    fn direct_full_line() {
        let (args, n) = parse_direct(b"PING\r\n").unwrap();
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(n, 6);
    }

    #[test]
    fn direct_splits_on_space() {
        let (args, _) = parse_direct(b"ECHO hey\r\n").unwrap();
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hey".to_vec()]);
    }

    #[test]
    fn direct_incomplete_is_none() {
        assert!(parse_direct(b"PING").is_none());
        assert!(parse_direct(b"PING ").is_none());
        assert!(parse_direct(b"PING \r").is_none());
        assert!(parse_direct(b"PING\r").is_none());
        assert!(parse_direct(b"\n").is_none());
    }

    #[test]
    fn encodes_blpop_reply() {
        let mut out = Vec::new();
        Resp::Array(Some(vec![
            Resp::Bulk(Some(b"apple".to_vec())),
            Resp::Bulk(Some(b"blueberry".to_vec())),
        ]))
        .encode(&mut out);
        assert_eq!(out, b"*2\r\n$5\r\napple\r\n$9\r\nblueberry\r\n");
    }

    #[test]
    fn direct_consumed_count_allows_pipelining() {
        let buf = b"PING\r\nECHO hey\r\n";
        let (first, n) = parse_direct(buf).unwrap();
        assert_eq!(first, vec![b"PING".to_vec()]);
        assert_eq!(n, 6); // points exactly past the first \r\n

        let (second, _) = parse_direct(&buf[n..]).unwrap();
        assert_eq!(second, vec![b"ECHO".to_vec(), b"hey".to_vec()]);
    }

    #[test]
    fn direct_bare_newline_terminates() {
        let (args, n) = parse_direct(b"PING\n").unwrap();
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(n, 5);
    }

    #[test]
    fn direct_leftover_after_command_waits() {
        let (args, n) = parse_direct(b"PING\r\nXX").unwrap();
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(n, 6);
        assert!(parse_direct(b"XX").is_none()); // partial remainder isn't a command yet
    }
}
