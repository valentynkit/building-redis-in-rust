use mio::net::TcpStream;
use tracing::{debug, error, instrument, warn};

use crate::command::common::CommandError;
use crate::command::{self, ClientInfo, RequestCmd};
use crate::db::Db;
use crate::resp::{self, Reply, Resp};

use std::collections::VecDeque;
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
    pub const fn new(id: usize) -> Self {
        Self(id)
    }
    pub const fn get(&self) -> usize {
        self.0
    }
}

// Transaction = Multi mode for queuing transactions and executing with EXEC
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum ClientMode {
    Normal,
    Transaction,
}

pub struct Client {
    id: ClientId,
    stream: TcpStream,
    mode: ClientMode,
    queue: VecDeque<Resp>,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>, // replies waiting to go out
}

impl Client {
    pub const fn make_normal_mode(&mut self) {
        self.mode = ClientMode::Normal;
    }

    pub fn start_transaction(&mut self) -> Result<(), CommandError> {
        if self.mode == ClientMode::Transaction {
            Err(CommandError::TransactionError)
        } else {
            self.mode = ClientMode::Transaction;
            Ok(())
        }
    }

    pub fn exec_transaction(&mut self, db: &mut Db) -> Result<Vec<Resp>, CommandError> {
        if self.mode != ClientMode::Transaction {
            return Err(CommandError::TransactionError);
        }
        if self.queue.is_empty() {
            return Err(CommandError::TransactionError);
        }
        let mut out: Vec<Resp> = vec![];
        while let Some(item) = self.queue.pop_back() {
            let resp = self.process_request(db, item);
            if let Some(resp) = resp {
                out.push(resp);
            }
        }
        Ok(out)
    }

    pub fn add_to_transaction(&mut self, resp: Resp) -> Result<(), CommandError> {
        if self.mode == ClientMode::Transaction {
            self.queue.push_front(resp);
            Ok(())
        } else {
            Err(CommandError::TransactionError)
        }
    }

    pub fn new(stream: TcpStream, id: ClientId) -> Self {
        Self {
            id,
            stream,
            mode: ClientMode::Normal,
            queue: VecDeque::new(),
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

    // TODO: improve this STATE machinge and state transitions
    fn post_process_success_request(&mut self, db: &mut Db, reply: Reply) -> Option<Resp> {
        match reply {
            Reply::Now(resp) => {
                // self.make_normal_mode();
                Some(resp)
            }
            Reply::StartTransaction => {
                if let Err(err) = self.start_transaction() {
                    debug!(?err, "command error");
                    Some(Resp::new_error(&err))
                } else {
                    Some(Resp::new_ok())
                }
            }
            Reply::AddTransaction(resp) => {
                if let Err(err) = self.add_to_transaction(resp) {
                    debug!(?err, "command error");
                    Some(Resp::new_error(&CommandError::TransactionError))
                } else {
                    Some(Resp::new_queued())
                }
            }
            Reply::ExecTransaction => {
                if self.mode != ClientMode::Transaction || self.queue.is_empty() {
                    Some(Resp::new_error(&CommandError::TransactionError))
                } else {
                    self.mode = ClientMode::Normal;
                    let resp_arr = self.exec_transaction(db);
                    let resp = match resp_arr {
                        Ok(resp_arr) => Resp::Array(Some(resp_arr)),
                        Err(err) => Resp::new_error(&err),
                    };
                    Some(resp)
                }
            }
            Reply::Blocked => None,
        }
    }
    fn process_request(&mut self, db: &mut Db, frame: Resp) -> Option<Resp> {
        let request_cmd = RequestCmd::new(frame, ClientInfo::new(self.id, self.mode));
        let response = command::handle(db, request_cmd);
        let resp: Option<Resp> = match response {
            Ok(reply) => self.post_process_success_request(db, reply),
            Err(err) => {
                debug!(?err, "command error");
                Some(Resp::new_error(&err))
            }
        };
        resp
    }

    /// Drain every complete command from inbuf, then flush replies in one write.
    fn consume(&mut self, db: &mut Db) -> Vec<Resp> {
        let mut out: Vec<Resp> = vec![];
        while let Some(request) = resp::parse_request(&self.inbuf) {
            self.inbuf.drain(..request.consumed());
            let out_item = self.process_request(db, request.body());
            if let Some(item) = out_item {
                out.push(item);
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
    use super::{Client, ClientId, Disposition};
    use crate::db::Db;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

    /// A connected loopback pair: (peer we drive, Client owning the other end).
    /// Both blocking — we always write before reading, so reads never stall.
    fn pair() -> (TcpStream, Client) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = TcpStream::connect(addr).unwrap();
        let (owned, _) = listener.accept().unwrap();
        let client = Client::new(mio::net::TcpStream::from_std(owned), ClientId::new(1));
        (peer, client)
    }

    #[test]
    fn ping_round_trips() {
        let (mut peer, mut client) = pair();
        peer.write_all(&resp(&[b"PING"])).unwrap();

        assert!(matches!(client.on_readable(&mut db()), Disposition::Keep));

        let mut reply = [0u8; 7];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
    }

    #[test]
    fn echo_returns_bulk() {
        let (mut peer, mut client) = pair();
        peer.write_all(&resp(&[b"ECHO", b"hey"])).unwrap();

        client.on_readable(&mut db());

        // Exactly one bulk frame, no trailing CRLF — guards the double-terminate regression.
        let mut reply = [0u8; 9];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"$3\r\nhey\r\n");
    }

    #[test]
    fn pipelined_commands_each_reply() {
        let (mut peer, mut client) = pair();
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
        let (mut peer, mut client) = pair();

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
        let (peer, mut client) = pair();
        drop(peer); // peer hangs up

        assert!(matches!(client.on_readable(&mut db()), Disposition::Drop));
    }
}
