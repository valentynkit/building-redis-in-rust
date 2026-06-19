pub fn parse(buf: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    let nl = buf.iter().position(|&b| b == b'\n')?;
    let mut line = &buf[..nl];
    if line.len() == 0 {
        return None;
    }
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }

    let args = line.split(|&b| b == b' ').map(<[u8]>::to_vec).collect();

    Some((args, nl + 1)) //  to also consume the \n
}

#[cfg(test)]
mod test {
    use super::parse;
    #[test]
    fn full_line() {
        let (args, n) = parse(b"PING\r\n").unwrap();
        assert_eq!(args, vec![b"PING".to_vec()]);
        assert_eq!(n, 6);
    }

    #[test]
    fn splits_on_space() {
        let (args, _) = parse(b"ECHO hey\r\n").unwrap();
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hey".to_vec()]);
    }

    #[test]
    fn incomplete_is_none() {
        assert!(parse(b"PING").is_none());
        assert!(parse(b"PING ").is_none());
        assert!(parse(b"PING \r").is_none());
        assert!(parse(b"PING\r").is_none());
        assert!(parse(b"\n").is_none());
    }
}
