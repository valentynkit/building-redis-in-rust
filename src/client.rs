use mio::net::TcpStream;
use tracing::{debug, error, instrument, warn};

use crate::command::common::CommandError;
use crate::command::{ClientInfo, Command};
use crate::db::Db;
use crate::networking::ServerInfo;
use crate::resp::{self, Propagate, Reply, RespBody};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::rc::Rc;

pub const READ_BUF: usize = 512;
/// Does this client survive the poll, or get dropped?
pub enum Disposition {
    Keep,
    Drop,
    PromoteToSlave,
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

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum ClientRole {
    Normal,
    Master,
    Slave,
}

pub struct Client {
    id: ClientId,
    pub stream: TcpStream,
    mode: ClientMode,
    role: ClientRole,
    queue: VecDeque<RespBody>,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>, // replies waiting to go out
    server_info: Rc<RefCell<ServerInfo>>,
    // this field is used ONLY on master which has finished handshake with slave, to propogate the
    // need for creating a copy of this client or moving it?
}

impl Client {
    fn clear_queue(&mut self) {
        self.queue = VecDeque::new();
    }
    pub const fn role(&self) -> ClientRole {
        self.role
    }

    pub fn promote_to_slave(&mut self) {
        self.role = ClientRole::Slave;
    }

    pub const fn make_normal_mode(&mut self) {
        self.mode = ClientMode::Normal;
    }

    pub fn start_transaction(&mut self) -> RespBody {
        if self.mode == ClientMode::Transaction {
            RespBody::new_error(&CommandError::ExecTransaction)
        } else {
            self.mode = ClientMode::Transaction;
            RespBody::new_ok()
        }
    }

    pub fn exec_transaction(&mut self, db: &mut Db) -> RespBody {
        if self.mode != ClientMode::Transaction {
            RespBody::new_error(&CommandError::ExecTransaction)
        } else if self.queue.is_empty() {
            self.make_normal_mode();
            db.remove_watcher(self.id);
            RespBody::Array(Some(vec![]))
        } else {
            self.make_normal_mode();
            db.remove_watcher(self.id);
            let mut out: Vec<RespBody> = vec![];
            while let Some(item) = self.queue.pop_back() {
                let reply = self.process_request(db, item);

                let resp_arr: Vec<RespBody> = match reply {
                    Ok((reply, _)) => self.post_process_success_request(db, reply),
                    Err(err) => {
                        debug!(?err, "command error");
                        vec![RespBody::new_error(&err)]
                    }
                };

                for resp in resp_arr {
                    out.push(resp);
                }
            }

            RespBody::Array(Some(out))
        }
    }

    pub fn add_to_transaction(&mut self, resp: RespBody) -> RespBody {
        if self.mode == ClientMode::Transaction {
            self.queue.push_front(resp);
            RespBody::new_queued()
        } else {
            RespBody::new_error(&CommandError::ExecTransaction)
        }
    }

    pub fn discard_transaction(&mut self, db: &mut Db, resp: Option<RespBody>) -> RespBody {
        if self.mode == ClientMode::Transaction {
            self.make_normal_mode();
            self.clear_queue();
            db.remove_watcher(self.id);
            resp.unwrap_or_else(RespBody::new_ok)
        } else {
            RespBody::new_error(&CommandError::DiscardTransaction)
        }
    }

    pub fn new(
        stream: TcpStream,
        id: ClientId,
        role: ClientRole,
        server_info: Rc<RefCell<ServerInfo>>,
    ) -> Self {
        Self {
            id,
            stream,
            mode: ClientMode::Normal,
            role,
            queue: VecDeque::new(),
            inbuf: Vec::with_capacity(READ_BUF),
            outbuf: Vec::new(),
            server_info,
        }
    }

    /// Poller reported this client readable: read, parse, run, reply.
    pub fn on_readable(&mut self, db: &mut Db) -> (Disposition, Vec<RespBody>) {
        let mut stream = &self.stream;
        let mut buf = [0u8; READ_BUF];
        let mut to_propogate: Vec<RespBody> = vec![];
        let disposition = match stream.read(&mut buf) {
            // EOF: peer closed cleanly
            Ok(0) => {
                warn!("client disconnected");
                Disposition::Drop
            }
            // TODO extract logic
            Ok(n) => {
                self.inbuf.extend_from_slice(&buf[..n]);
                to_propogate.extend(self.consume(db));

                match self.role {
                    ClientRole::Normal => self.flush(),
                    ClientRole::Master => {
                        // TODO: I think we should move the slave offset withotu replying to client, and
                        // the ACK should be handled not by req-resp but in before sleep
                        // todo!()
                        Disposition::Keep
                    }
                    ClientRole::Slave => {
                        if matches!(self.flush(), Disposition::Drop) {
                            Disposition::Drop
                        } else {
                            Disposition::PromoteToSlave
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep, // nothing yet
            Err(e) if e.kind() == io::ErrorKind::Interrupted => Disposition::Keep, // EINTR
            Err(e) => {
                warn!(?e, "read failed");
                Disposition::Drop
            }
        };

        (disposition, to_propogate)
    }

    pub fn write_out(&mut self, resp: &RespBody) {
        resp.encode(&mut self.outbuf);
    }

    // TODO: improve this STATE machinge and state transitions
    fn post_process_success_request(&mut self, db: &mut Db, reply: Reply) -> Vec<RespBody> {
        let mut out = vec![];
        match reply {
            Reply::Now(resp, _) => {
                // self.make_normal_mode();
                out.push(resp);
            }
            Reply::StartTransaction => out.push(self.start_transaction()),
            Reply::AddTransaction(resp) => out.push(self.add_to_transaction(resp)),
            Reply::ExecTransaction => out.push(self.exec_transaction(db)),
            Reply::DiscardTransaction(resp) => out.push(self.discard_transaction(db, resp)),
            Reply::Blocked => {}
            Reply::Rdb(sync, rdb) => {
                warn!("rdb finished handshake on master side");
                self.promote_to_slave();
                out.push(sync);
                out.push(rdb);
            }
        }
        out
    }
    fn process_request(
        &mut self,
        db: &mut Db,
        frame: RespBody,
    ) -> Result<(Reply, Option<RespBody>), CommandError> {
        let client_info =
            ClientInfo::new(self.id, self.mode, self.role, Rc::clone(&self.server_info));
        Command::new(frame, client_info).and_then(|mut cmd| cmd.execute(db))
    }

    /// Drain every complete command from inbuf, then flush replies in one write.
    /// returns requset to replicate to slaves
    fn consume(&mut self, db: &mut Db) -> Vec<RespBody> {
        let mut out = vec![];
        while let Some(request) = resp::parse_resp(&self.inbuf) {
            self.inbuf.drain(..request.consumed());
            let req_body = request.body();
            let reply = self.process_request(db, req_body);

            let resp_arr: Vec<RespBody> = match reply {
                Ok((reply, forward)) => {
                    if let Some(forward) = forward {
                        out.push(forward);
                    }

                    self.post_process_success_request(db, reply)
                }
                Err(err) => {
                    debug!(?err, "command error");
                    vec![RespBody::new_error(&err)]
                }
            };
            for resp in resp_arr {
                match self.role {
                    ClientRole::Normal | ClientRole::Slave => self.write_out(&resp),
                    ClientRole::Master => {
                        // TODO: I think we should move the slave offset withotu replying to client, and
                        // the ACK should be handled not by req-resp but in before sleep
                        warn!("master (on slave) slave should update it offset etc..");
                    }
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
