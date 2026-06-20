use crate::command;
use crate::command::CommandError;
use crate::resp;
use crate::resp::write_error;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};

pub const READ_BUF: usize = 512;
/// Does this client survive the poll, or get dropped?
pub enum Disposition {
    Keep,
    Drop,
}

pub struct Client {
    stream: TcpStream,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>, // replies waiting to go out
}

impl Client {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            inbuf: Vec::with_capacity(READ_BUF),
            outbuf: Vec::new(),
        }
    }

    /// Poller reported this fd readable: read, parse, run, reply.
    pub fn on_readable(&mut self) -> Disposition {
        let mut stream = &self.stream;
        let mut buf = [0u8; READ_BUF];

        match stream.read(&mut buf) {
            // EOF: peer closed cleanly
            Ok(0) => {
                println!("disconnected (fd{})", stream.as_raw_fd());
                Disposition::Drop
            }
            // TODO extract logic
            Ok(n) => {
                self.inbuf.extend_from_slice(&buf[..n]);
                self.consume()
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep, // nothing yet
            Err(e) if e.kind() == io::ErrorKind::Interrupted => Disposition::Keep, // EINTR
            Err(e) => {
                eprintln!("read (fd {}): {e}", stream.as_raw_fd());
                Disposition::Drop
            }
        }
    }

    /// Drain every complete command from inbuf, then flush replies in one write.
    fn consume(&mut self) -> Disposition {
        while let Some((args, consumed)) = resp::parse(&self.inbuf) {
            self.inbuf.drain(..consumed);
            if let Err(err) = command::dispatch(&args, &mut self.outbuf) {
                write_error(&mut self.outbuf, &err.to_string());
            }
        }
        self.flush()
    }

    fn flush(&mut self) -> Disposition {
        if let Err(e) = self.stream.write_all(&self.outbuf) {
            eprintln!("flush (fd{}): {e}", self.stream.as_raw_fd());
            return Disposition::Drop;
        }

        self.outbuf.clear();
        Disposition::Keep
    }
}

#[cfg(test)]
mod test {
    use super::{Client, Disposition};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    /// A connected loopback pair: (peer we drive, stream the Client owns).
    /// Both blocking — we always write before reading, so reads never stall.
    fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = TcpStream::connect(addr).unwrap();
        let (owned, _) = listener.accept().unwrap();
        (peer, owned)
    }

    #[test]
    fn ping_round_trips() {
        let (mut peer, owned) = pair();
        let mut client = Client::new(owned);
        peer.write_all(b"PING\r\n").unwrap();

        assert!(matches!(client.on_readable(), Disposition::Keep));

        let mut reply = [0u8; 7];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
    }

    #[test]
    fn pipelined_commands_each_reply() {
        let (mut peer, owned) = pair();
        let mut client = Client::new(owned);
        peer.write_all(b"PING\r\nPING\r\n").unwrap(); // two commands, one read

        client.on_readable();

        let mut reply = [0u8; 14];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n+PONG\r\n");
    }

    /// Regression: outbuf must clear between events or replies accumulate
    /// (event 2 would re-send event 1's reply).
    #[test]
    fn outbuf_clears_between_events() {
        let (mut peer, owned) = pair();
        let mut client = Client::new(owned);

        peer.write_all(b"PING\r\n").unwrap();
        client.on_readable();
        peer.write_all(b"PING\r\n").unwrap();
        client.on_readable();

        drop(client); // close owned side → peer reads to EOF
        let mut got = Vec::new();
        peer.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"+PONG\r\n+PONG\r\n"); // exactly two, not three
    }

    #[test]
    fn eof_drops_client() {
        let (peer, owned) = pair();
        let mut client = Client::new(owned);
        drop(peer); // peer hangs up

        assert!(matches!(client.on_readable(), Disposition::Drop));
    }
}
