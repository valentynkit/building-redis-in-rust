// find star,
pub fn parse_resp(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if buf[0] != b'*' {
        return None;
    }
    let get_position = |inner_buf: &[u8], ch: u8| inner_buf.iter().position(|&b| b == ch);
    let get_part_end_position = |inner_buf: &[u8]| get_position(inner_buf, b'\r');
    let get_dollar_sign_position = |inner_buf: &[u8]| get_position(inner_buf, b'$');
    let parse_number = |inner_buf: &[u8]| str::from_utf8(inner_buf).ok()?.parse().ok();
    let mut cursor = get_part_end_position(&buf[1..])? + 1;
    let n_elems: u32 = parse_number(&buf[1..cursor])?;
    let output: Vec<Vec<u8>> = vec![];
    for _ in 0..n_elems {
        let num_start = cursor + get_dollar_sign_position(&buf[cursor..])? + 1;
        // "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
        cursor = num_start + get_part_end_position(&buf[num_start + 1..])? + 1;
        let part_size = parse_number(&buf[num_start..cursor])?;
    }
    while cursor < buf.len() {}

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

pub fn write_error(out: &mut Vec<u8>, msg: &str) {
    out.extend_from_slice(b"-ERR ");
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn write_simple(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn write_bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}
#[cfg(test)]
mod test {
    use crate::resp::{parse_direct, parse_resp};

    #[test]
    fn resp_full_line() {
        let (args, _) = parse_resp(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n").unwrap();
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hey".to_vec()]);
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
