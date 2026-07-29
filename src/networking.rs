use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::iter;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use thiserror::Error;
use tracing::{Span, debug, debug_span, error, field, info, info_span, trace};

use crate::Cli;
use crate::client::{Client, ClientId, Disposition, PeerRole};
use crate::db::{Db, HandleWaitersResult};
use crate::resp::RespBody;
const ADDR: &str = "127.0.0.1";
const LISTENER: Token = Token(0);
const MASTER: Token = Token(1);
const MAX_EVENTS: usize = 128;

pub struct StartTime {
    start_ms_mono: Instant,
}

impl StartTime {
    pub const fn new(start_ms_mono: Instant) -> Self {
        Self { start_ms_mono }
    }
}

#[derive(Debug, Error, Clone)]
pub enum NetworkingError {
    #[error("{0} - should be valid IPv4 or IPv6")]
    NotValidAddr(String),
    #[error("slave has invalid state")]
    InvalidSlave,
    #[error("master disconnected")]
    MasterDisconnected,
    #[error("Handshake unfinished")]
    HandshakeUnfinished,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone)]
pub enum ServerRole {
    Master,
    Slave,
}
impl ServerRole {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Slave => "slave",
        }
    }
}

pub struct ServerInfo {
    role: ServerRole,
    connected_slaves: u32,
    master_replid: String,
    master_repl_offset: i64,
    get_ack_from_slaves: bool,
    replica_of: Option<String>,
    dir: String,
    dbfilename: String,
}

impl ServerInfo {
    pub(crate) fn new(
        role: ServerRole,
        connected_slaves: u32,
        master_replid: String,
        master_repl_offset: i64,
        get_ack_from_slaves: bool,
        replica_of: Option<String>,
        dir: String,
        dbfilename: String,
    ) -> Self {
        Self {
            role,
            connected_slaves,
            master_replid,
            master_repl_offset,
            get_ack_from_slaves,
            replica_of,
            dir,
            dbfilename,
        }
    }

    pub(crate) fn repl_request_ack_from_slaves(&mut self) {
        self.get_ack_from_slaves = true;
    }
    pub(crate) const fn role(&self) -> ServerRole {
        self.role
    }

    pub(crate) const fn connected_slaves(&self) -> u32 {
        self.connected_slaves
    }

    pub(crate) fn master_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub(crate) const fn master_repl_offset(&self) -> i64 {
        self.master_repl_offset
    }

    pub(crate) const fn incr_repl_offset(&mut self, consumed: usize) {
        self.master_repl_offset += consumed as i64;
    }

    pub fn rdb_path(&self) -> PathBuf {
        Path::new(&self.dir).join(&self.dbfilename)
    }
}

pub struct ServerConfig {
    hz: u32,
    active_expire_enabled: bool,
    active_expire_percent: u32,
    active_expire_keys_per_round: usize,
    client_timeout_s: u32,
    client_timeout_check_hz: u32,
}

impl ServerConfig {
    fn init() -> ServerConfig {
        Self {
            // Server tick rate
            hz: 10,
            active_expire_enabled: true,
            active_expire_percent: 25,
            active_expire_keys_per_round: 20,
            client_timeout_s: 300,
            client_timeout_check_hz: 10,
        }
    }
}

pub struct Server {
    listener: TcpListener,
    clients: HashMap<Token, Client>,
    // TODO hashset?
    slaves: HashSet<Token>,
    master_link: Option<Client>,
    next_client_id: usize,
    poll: Poll,
    db: Db,
    cronloops: u64,
    start_time: StartTime,
    info: Rc<RefCell<ServerInfo>>,
    config: ServerConfig,
    span: Span,
}

impl Server {
    const fn get_increased_id(&mut self) -> usize {
        self.next_client_id += 1;
        self.next_client_id
    }

    pub fn new(cli: &Cli) -> Result<Self> {
        let replicaof = cli.parse_replicaof()?;
        let port = cli.port();
        let mut listener = server_start(port).context("starting listener")?;
        let poll = Poll::new().context("creating poller")?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)
            .context("registering listener")?;

        // register the listener for "readable" = incoming connection
        let monotonic_ms = Instant::now();
        let realtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("reading wall clock")?;
        let start_time = StartTime::new(monotonic_ms);
        let db = Db::create(monotonic_ms.elapsed(), realtime_ms);
        let mut role = ServerRole::Master;
        let mut replica_of_parsed: Option<String> = None;

        if let Some((host, port)) = replicaof {
            role = ServerRole::Slave;
            replica_of_parsed = Some(format!("{host}:{port}"));
        }

        info!(?role, ?replica_of_parsed, "server role resolved");

        //TODO:  make optional master_replid, offset. and have it None for slaves and configured for master.
        let server_info = Rc::new(RefCell::new(ServerInfo::new(
            role,
            0,
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
            0,
            false,
            replica_of_parsed,
            cli.dir().into(),
            cli.dbfilename().into(),
        )));

        let server_span = info_span!(
            "server",
            role = server_info.borrow().role.as_str(),
            port,
            replid = &server_info.borrow().master_replid[..7],
            run_id = std::process::id(),
            lfd = listener.as_raw_fd(),
            slaves = field::Empty,
        );

        Ok(Self {
            listener,
            clients: HashMap::new(),
            slaves: HashSet::new(),
            next_client_id: 1,
            poll,
            db,
            cronloops: 0,
            start_time,
            info: server_info,
            config: ServerConfig::init(),
            master_link: None,
            span: server_span,
        })
    }

    fn set_current_time(&mut self) -> Result<()> {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let monotonic_ms = Instant::now().elapsed();

        let uptime = self.start_time.start_ms_mono.elapsed();
        trace!(?uptime, "clock tick");

        self.db.update_time(monotonic_ms, realtime_ms);
        Ok(())
    }

    fn active_expire_cron(&mut self) {
        let config = &self.config;
        if !config.active_expire_enabled {
            return;
        }

        let start: u128 = self.db.monotonic_ms().as_millis();
        let mut current: u128 = start;
        let budget_ms: u128 = u128::from((1000 / config.hz) / 4);
        while current - start < budget_ms {
            let expired: u32 = self.db.active_sweep(config.active_expire_keys_per_round);
            let is_below_threshold = (expired * 100)
                .saturating_div(config.active_expire_keys_per_round as u32)
                < config.active_expire_percent;

            if is_below_threshold {
                break;
            }
            current = Instant::now().elapsed().as_millis();
        }
    }
    fn clients_key_waiters_cron(&mut self) -> Option<Duration> {
        let HandleWaitersResult {
            replies: list_replies,
            deadline: list_deadline,
        } = self.db.handle_list_waiters();
        let HandleWaitersResult {
            replies: stream_replies,
            deadline: stream_deadline,
        } = self.db.handle_stream_waiters();

        for (client_id, resp) in list_replies.into_iter().chain(stream_replies) {
            let client_id = Token(client_id.get());
            if let Some(client) = self.clients.get_mut(&client_id) {
                let _g = client.span().entered();
                debug!("delivering blocked reply to waiter");
                client.write_out(&resp);

                if matches!(client.flush(), Disposition::Drop) {
                    info!("waiter disconnected mid-flush; removed");
                    self.clients.remove(&client_id);
                }
            }
        }

        match (list_deadline, stream_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Settle every client parked in WAIT against the replicas' latest ACK
    /// offsets. This is the only place that can see both sides, so it owns the
    /// counting entirely — WAIT itself just registers the block.
    fn client_ack_waiters_cron(&mut self) -> Option<Duration> {
        let acks: Vec<i64> = self
            .slaves
            .iter()
            .filter_map(|token| self.clients.get(token))
            .map(Client::repl_ack_off)
            .collect();

        let now = self.db.realtime_ms();
        let mut nearest_deadline: Option<Duration> = None;
        let mut dropped: Vec<Token> = vec![];

        // TODO: do we really need to loop through the clients, or maybe we could loop through just
        // slaves? and get self.clients by token.
        for (token, client) in &mut self.clients {
            if !client.is_ack_blocked() {
                continue;
            }
            match client.settle_ack_block(&acks, now) {
                Some(remaining) => {
                    nearest_deadline = Some(
                        nearest_deadline.map_or(remaining, |cur: Duration| cur.min(remaining)),
                    );
                }
                None => {
                    let _g = client.span().entered();
                    debug!("delivering WAIT reply");
                    if matches!(client.flush(), Disposition::Drop) {
                        dropped.push(*token);
                    }
                }
            }
        }

        // TODO: what about self.slaves? shouldn't we remove it by token?
        for token in dropped {
            info!(?token, "waiter disconnected mid-flush; removed");
            self.db.remove_watcher(ClientId::new(token.0));
            self.clients.remove(&token);
        }

        nearest_deadline
    }

    /// One GETACK per event-loop tick for every client that blocked in WAIT
    /// since the last one — grouped, so N waiters cost one round trip.
    fn get_ack_from_slaves_cron(&mut self) {
        {
            let server_info = self.info.borrow();
            if !server_info.get_ack_from_slaves || server_info.role() != ServerRole::Master {
                return;
            }
        }

        // No slaves means nothing goes on the wire, so the offset must not move.
        if self.slaves.is_empty() {
            self.info.borrow_mut().get_ack_from_slaves = false;
            return;
        }

        let mut cmd_buffer = Vec::new();
        ["REPLCONF", "GETACK", "*"]
            .into_iter()
            .collect::<RespBody>()
            .encode(&mut cmd_buffer);

        for token in &self.slaves {
            if let Some(slave) = self.clients.get_mut(token) {
                slave.write_out_buffer(&cmd_buffer);
                slave.flush();
            }
        }

        let mut server_info = self.info.borrow_mut();
        // The offset tracks the replication stream, not deliveries: one GETACK
        // advances it once no matter how many slaves it reached. Slaves count
        // these bytes too, so skipping it makes their ACKs overtake the master.
        server_info.incr_repl_offset(cmd_buffer.len());
        server_info.get_ack_from_slaves = false;
        debug!(slaves = self.slaves.len(), "GETACK broadcast to replicas");
    }

    // HouseKeeping
    fn before_sleep(&mut self) -> Option<Duration> {
        self.cronloops += 1;
        self.active_expire_cron();
        self.get_ack_from_slaves_cron();
        let key_waiters_deadline = self.clients_key_waiters_cron();
        let ack_waiters_deadline = self.client_ack_waiters_cron();

        // Not `.min()`: on Option that orders None first, so one absent deadline
        // would swallow the other and poll would block past the live timeout.
        match (key_waiters_deadline, ack_waiters_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn slave_handshake(&mut self, port: u16) -> Result<(), anyhow::Error> {
        {
            let master_addr = {
                let server_info = self.info.borrow();

                if server_info.role == ServerRole::Master {
                    return Ok(());
                }

                let Some(master_addr) = &server_info.replica_of else {
                    return Err(NetworkingError::InvalidSlave.into());
                };

                info!(master_addr = %master_addr, "starting slave handshake");
                master_addr.clone()
            };

            let stream = std::net::TcpStream::connect(master_addr)?;
            // Long lived replication link for slave -> master
            let stream = mio::net::TcpStream::from_std(stream);

            let client = self
                .register_client(stream, MASTER, PeerRole::Master)
                .expect("master link registration should succeed");

            self.master_link = Some(client);
        }

        // Every handshake step logs under the master link's connection span.
        let span = self.master_link.as_ref().expect("master link set").span();
        let _g = span.entered();

        self.slave_ping()?;
        self.slave_replconf(port)?;
        self.slave_psync()?;
        info!("handshake finished; replica ready");
        Ok(())
    }

    pub fn run(mut self, port: u16) -> Result<()> {
        let _server_guard = self.span.clone().entered();
        info!("server starting");
        let mut events = Events::with_capacity(MAX_EVENTS);
        self.slave_handshake(port)?;
        loop {
            let _span = debug_span!("server loop", loop = self.cronloops + 1).entered();

            let timeout = self.before_sleep();
            self.poll.poll(&mut events, timeout)?;
            self.set_current_time()?;
            for event in &events {
                if event.is_readable() {
                    let token = event.token();
                    match token {
                        LISTENER => self.accept_client(),
                        MASTER => self.service_master(),
                        _ => self.service_client(token),
                    }
                }
            }
        }
    }

    fn register_client(
        &self,
        mut stream: TcpStream,
        c_token: Token,
        role: PeerRole,
    ) -> Option<Client> {
        if let Err(error) = self
            .poll
            .registry()
            .register(&mut stream, c_token, Interest::READABLE)
        {
            error!(?c_token, ?error, "registration failed");
            return None;
        }

        let server_info = Rc::clone(&self.info);
        let client = Client::new(stream, ClientId::new(c_token.0), role, server_info);
        Some(client)
    }

    fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    let c_token = Token(self.get_increased_id());
                    let client = self
                        .register_client(stream, c_token, PeerRole::Normal)
                        .expect("client registration should succeed");

                    let _g = client.span().entered();
                    info!(?addr, "client connected");
                    self.clients.insert(c_token, client);
                }

                Err(e) if would_block(&e) => break,
                Err(e) if interrupted(&e) => {}
                Err(error) => {
                    error!(?error, "accept failed");
                    break;
                }
            }
        }
    }

    fn service_master(&mut self) {
        let Some(master) = &mut self.master_link else {
            error!("master link empty");
            return;
        };
        let _g = master.span().entered();
        debug!("servicing master link");
        let (disposition, _) = master.on_readable(&mut self.db);
        if matches!(disposition, Disposition::Drop) {
            error!("master link dropped");
        }
    }

    fn service_client(&mut self, token: Token) {
        let Some(client) = self.clients.get_mut(&token) else {
            return;
        };
        let _g = client.span().entered();
        debug!("servicing client");
        let (disposition, to_propagate) = client.on_readable(&mut self.db);
        if matches!(disposition, Disposition::Drop) {
            let client_role = client.peer_role();

            self.db.remove_watcher(ClientId::new(token.0));
            info!("client disconnected; removed");
            self.clients.remove(&token);

            if client_role == PeerRole::Slave && self.slaves.contains(&token) {
                self.slaves.remove(&token);

                let slaves = {
                    let mut server_info = self.info.borrow_mut();
                    server_info.connected_slaves -= 1;
                    server_info.connected_slaves
                };
                self.span.record("slaves", slaves as u64);
                info!(slaves, "replica detached");
            }
            return;
        }
        if client.peer_role() == PeerRole::Slave && !self.slaves.contains(&token) {
            let slaves = {
                let mut server_info = self.info.borrow_mut();
                server_info.connected_slaves += 1;
                server_info.connected_slaves
            };
            self.slaves.insert(token);
            self.span.record("slaves", slaves as u64);
            info!(slaves, "replica attached");
        }

        let mut server_info = self.info.borrow_mut();

        let mut cmd_buffer: Vec<u8> = vec![];
        for cmd in &to_propagate {
            let mut delivered = 0u64;

            cmd_buffer.clear();
            cmd.encode(&mut cmd_buffer);

            let offset = cmd_buffer.len();
            server_info.incr_repl_offset(offset);

            for token in &mut self.slaves.iter() {
                if let Some(slave) = self.clients.get_mut(token) {
                    slave.write_out_buffer(&cmd_buffer);
                    slave.flush();
                    delivered += 1;
                }
            }
            debug!(slaves = delivered, "propagated write");
        }
    }

    fn slave_psync(&mut self) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        debug!("handshake step: PSYNC");
        let resp_body = ["PSYNC", "?", "-1"].into_iter().collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();
        let out = master_client.read_fullresync()?;

        if !out.starts_with("+FULLRESYNC ") {
            error!(?out, "handshake PSYNC: expected +FULLRESYNC");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }
        master_client.consume(&mut self.db);
        if matches!(master_client.flush(), Disposition::Drop) {
            error!(?out, "handshake PSYNC: couldn't flush to master");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }

    fn slave_replconf(&mut self, port: u16) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        debug!("handshake step: REPLCONF listening-port");
        let resp_body = ["REPLCONF", "listening-port", &port.to_string()]
            .into_iter()
            .collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();

        let out = master_client.read_line()?;
        if out != "+OK\r\n" {
            error!(?out, "handshake REPLCONF 1/2: expected +OK");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        debug!("handshake step: REPLCONF capa psync2");
        let resp_body = ["REPLCONF", "capa", "psync2"]
            .into_iter()
            .collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();
        let out = master_client.read_line()?;
        if out != "+OK\r\n" {
            error!(?out, "handshake REPLCONF 2/2: expected +OK");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }
    fn slave_ping(&mut self) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        debug!("handshake step: PING");
        master_client.write_out(&iter::once("PING").collect::<RespBody>());
        master_client.flush();
        let out = master_client.read_line()?;
        if out != "+PONG\r\n" {
            error!(?out, "handshake PING: expected +PONG");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }
}

fn server_start(port: u16) -> Result<TcpListener, anyhow::Error> {
    let addr_str = format!("{ADDR}:{port}");
    let addr = addr_str
        .parse()
        .map_err(|_| NetworkingError::NotValidAddr(addr_str))?;

    let listener = mio::net::TcpListener::bind(addr)?;

    println!(
        "listening on {} (fd {})",
        listener.local_addr()?,
        listener.as_raw_fd()
    );
    Ok(listener)
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
