// find star,

pub fn parse_resp(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if *buf.first()? != b'*' {
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

    Some((output, cursor))
}

pub fn parse_direct(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    let nl = buf.iter().position(|&b| b == b'\n')?;
    let mut line = &buf[..nl];
    if line.is_empty() {
        return None;
    }
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }

    let args = line.split(|&b| b == b' ').map(<[u8]>::to_vec).collect();

    Some((args, nl + 1)) //  to also consume the \n
}

const END_OF_LINE: &[u8; 2] = b"\r\n";

pub enum ResponseKind<'a> {
    NullBulk,
    SimpleOk,
    Simple(&'a str),
    Error(&'a str),
    Str(&'a [u8]),
    Int(i64),
    Array(Vec<Vec<u8>>),
}
pub fn write_out(kind: ResponseKind, out: &mut Vec<u8>) {
    // Each writer emits a fully framed RESP reply (type byte + payload + CRLF).
    // write_out adds nothing — a blanket trailing CRLF double-terminates bulk/error.
    match kind {
        ResponseKind::NullBulk => write_null_bulk(out),
        ResponseKind::SimpleOk => write_simple(out, "OK"),
        ResponseKind::Simple(str) => write_simple(out, str),
        ResponseKind::Error(str) => write_error(out, str),
        ResponseKind::Str(data) => write_str(out, data),
        ResponseKind::Int(num) => write_int(out, num),
        ResponseKind::Array(items) => write_arr(out, items),
    }
}

fn write_arr(out: &mut Vec<u8>, items: Vec<Vec<u8>>) {
    out.push(b'*');
    out.extend_from_slice(items.len().to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);
    for item in items {
        write_str(out, &item);
    }
}

fn write_int(out: &mut Vec<u8>, num: i64) {
    let sign = if num < 0 { b'-' } else { b'+' };
    out.push(b':');
    out.extend_from_slice(num.to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);
}
fn write_null_bulk(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

fn write_error(out: &mut Vec<u8>, msg: &str) {
    out.extend_from_slice(b"-ERR ");
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(END_OF_LINE);
}

fn write_simple(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(END_OF_LINE);
}

fn write_str(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(END_OF_LINE);
    out.extend_from_slice(data);
    out.extend_from_slice(END_OF_LINE);
}

#[cfg(test)]
mod test {
    use crate::resp::{parse_direct, parse_resp};

    #[test]
    fn resp_full_line() {
        let (args, n) = parse_resp(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n").unwrap();
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
