use mio::net::TcpStream;
use tracing::{debug, error, instrument, warn};

use crate::command::{self};
use crate::db::Db;
use crate::resp::{self, Reply, Resp};
use std::io::{self, Read, Write};

pub const READ_BUF: usize = 512;
/// Does this client survive the poll, or get dropped?
pub enum Disposition {
    Keep,
    Drop,
}

#[derive(Eq, Hash, Debug, PartialEq, Copy, Clone)]
pub struct ClientId(usize);

impl ClientId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
    pub fn get(&self) -> usize {
        self.0
    }
}

pub struct Client {
    id: ClientId,
    stream: TcpStream,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>, // replies waiting to go out
}

impl Client {
    pub fn new(stream: TcpStream, id: ClientId) -> Self {
        Self {
            id,
            stream,
            inbuf: Vec::with_capacity(READ_BUF),
            outbuf: Vec::new(),
        }
    }
    /// Poller reported this client readable: read, parse, run, reply.
    pub fn on_readable(&mut self, db: &mut Db) -> Disposition {
        let mut stream = &self.stream;
        let mut buf = [0u8; READ_BUF];

        match stream.read(&mut buf) {
            // EOF: peer closed cleanly
            Ok(0) => {
                warn!("client disconnected");
                Disposition::Drop
            }
            // TODO extract logic
            Ok(n) => {
                self.inbuf.extend_from_slice(&buf[..n]);
                for resp in self.consume(db) {
                    self.write_out(&resp);
                }

                self.flush()
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep, // nothing yet
            Err(e) if e.kind() == io::ErrorKind::Interrupted => Disposition::Keep, // EINTR
            Err(e) => {
                warn!(?e, "read failed");
                Disposition::Drop
            }
        }
    }

    pub fn write_out(&mut self, resp: &Resp) {
        resp.encode(&mut self.outbuf);
    }

    /// Drain every complete command from inbuf, then flush replies in one write.
    fn consume(&mut self, db: &mut Db) -> Vec<Resp> {
        let mut out: Vec<Resp> = vec![];
        while let Some(request) = resp::parse_request(&self.inbuf) {
            self.inbuf.drain(..request.consumed());
            let response = command::handle(request.body(), db, self.id);
            match response {
                Ok(reply) => match reply {
                    Reply::Now(resp) => out.push(resp),
                    Reply::Blocked => {}
                },
                Err(err) => {
                    debug!(?err, "command error");
                    out.push(Resp::new_error(&err));
                }
            }
        }
        out
    }

    #[instrument(skip(self))]
    pub fn flush(&mut self) -> Disposition {
        if let Err(e) = self.stream.write_all(&self.outbuf) {
            error!(?e, "flush failed");
            return Disposition::Drop;
        }
        debug!(flushing = %self.outbuf.escape_ascii(),"flushing to client");
        self.outbuf.clear();
        Disposition::Keep
    }
}

#[cfg(test)]
mod test {
    use super::{Client, Disposition};
    use crate::db::Db;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant, SystemTime, SystemTimeError, UNIX_EPOCH};

    fn db() -> Db {
        let realtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime::now should work with durion since UNIX_EPOCH");

        Db::create(Instant::now(), realtime_ms)
    }

    /// Encode a command as a RESP array of bulk strings — what real clients send.
    /// Computes the length prefixes so tests can't ship a mismatched `$n`.
    fn resp(args: &[&[u8]]) -> Vec<u8> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

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
        peer.write_all(&resp(&[b"PING"])).unwrap();

        assert!(matches!(client.on_readable(&mut db()), Disposition::Keep));

        let mut reply = [0u8; 7];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
    }

    #[test]
    fn echo_returns_bulk() {
        let (mut peer, owned) = pair();
        let mut client = Client::new(owned);
        peer.write_all(&resp(&[b"ECHO", b"hey"])).unwrap();

        client.on_readable(&mut db());

        // Exactly one bulk frame, no trailing CRLF — guards the double-terminate regression.
        let mut reply = [0u8; 9];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"$3\r\nhey\r\n");
    }

    #[test]
    fn pipelined_commands_each_reply() {
        let (mut peer, owned) = pair();
        let mut client = Client::new(owned);
        let mut frames = resp(&[b"PING"]); // two commands in one write,
        frames.extend(resp(&[b"PING"])); // delivered in a single read
        peer.write_all(&frames).unwrap();

        client.on_readable(&mut db());

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

        let mut db = db();
        peer.write_all(&resp(&[b"PING"])).unwrap();
        client.on_readable(&mut db);
        peer.write_all(&resp(&[b"PING"])).unwrap();
        client.on_readable(&mut db);

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

        assert!(matches!(client.on_readable(&mut db()), Disposition::Drop));
    }
}
